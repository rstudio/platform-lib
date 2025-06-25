package store

// Copyright (C) 2025 by Posit Software, PBC

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/rstudio/platform-lib/v2/examples/cmd/markdownRenderer/notifytypes"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listeners/local"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listenerutils"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/impls/database/dbqueuetypes"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/permit"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
	queuetypes "github.com/rstudio/platform-lib/v2/pkg/rsqueue/types"
)

type Store interface {
	// QueueStore Use composition to include the database queue store interface
	dbqueuetypes.QueueStore
	dbqueuetypes.QueueGroupStore

	Notify(channelName string, n interface{}) error

	// QueueNewGroup creates a new queue group in the database
	QueueNewGroup(name string) (dbqueuetypes.QueueGroupRecord, error)
}

// QueueGroup represents a queue group
type QueueGroup struct {
	gorm.Model
	Name      string `gorm:"column:name;unique"`
	Cancelled bool   `gorm:"column:cancelled"`
	Started   bool   `gorm:"column:started"`
}

func (QueueGroup) TableName() string {
	return "queue_group"
}

func (r *QueueGroup) GroupId() int64 {
	return int64(r.ID)
}

type Queue struct {
	gorm.Model
	Priority uint64 `gorm:"column:priority"`
	Permit   uint64 `gorm:"column:permit"`
	Item     []byte `gorm:"column:item"`
	Name     string `gorm:"column:name"`
	GroupID  *int64 `gorm:"column:group_id"`
	Group    *QueueGroup
	Type     uint64         `gorm:"column:type"`
	Address  sql.NullString `gorm:"column:address;unique"`
	Carrier  []byte         `gorm:"column:carrier"`
}

func (Queue) TableName() string {
	return "queue"
}

type QueueFailure struct {
	Address string `gorm:"column:address"`
	Error   string `gorm:"column:error"`
}

type QueuePermit struct {
	gorm.Model
}

func (p *QueuePermit) PermitId() permit.Permit {
	return permit.Permit(p.ID)
}

func (p *QueuePermit) PermitCreated() time.Time {
	return p.CreatedAt
}

type DbQueueNotification struct {
	listener.GenericNotification
}

func NewDbQueueNotify() *DbQueueNotification {
	return &DbQueueNotification{
		GenericNotification: listener.GenericNotification{
			NotifyGuid: uuid.New().String(),
			NotifyType: notifytypes.NotifyTypeQueue,
		},
	}
}

type queuedNotification struct {
	channel string
	n       interface{}
}

type store struct {
	db            *gorm.DB
	inTransaction bool

	// For local notification queuing in a transaction
	mutex         sync.Mutex
	notifications []queuedNotification
	llFactory     *local.ListenerProvider
}

func Open(path string, llf *local.ListenerProvider) Store {
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	// Migrate the schema
	db.AutoMigrate(&QueueGroup{})
	db.AutoMigrate(&Queue{})
	db.AutoMigrate(&QueueFailure{})
	db.AutoMigrate(&QueuePermit{})

	return &store{
		db:        db,
		llFactory: llf,
	}
}

func (conn *store) BeginTransaction(description string) (Store, error) {
	return &store{
		db:            conn.db.Begin(),
		inTransaction: true,
		notifications: make([]queuedNotification, 0),
		llFactory:     conn.llFactory,
	}, nil
}

func (conn *store) BeginTransactionQueue(description string) (dbqueuetypes.QueueStore, error) {
	return conn.BeginTransaction(description)
}

func (conn *store) CompleteTransaction(err *error) {
	if *err != nil {
		conn.db.Rollback()
	} else {
		finErr := conn.db.Commit().Error

		// If the transaction was committed successfully, send the notifications that
		// where queued during the transaction.
		if finErr == nil && conn.llFactory != nil {
			for _, n := range conn.notifications {
				slog.Debug(fmt.Sprintf("Notifying %s of available work: %#v", n.channel, n.n))
				conn.llFactory.Notify(n.channel, n.n)
			}
		}

	}
}

func (conn *store) NotifyExtend(permit uint64) error {
	return conn.Notify(notifytypes.ChannelLeader, dbqueuetypes.NewQueuePermitExtendNotification(permit, notifytypes.NotifyTypePermitExtend))
}

func (conn *store) QueueNewGroup(name string) (dbqueuetypes.QueueGroupRecord, error) {
	newGroup := &QueueGroup{
		Name:      name,
		Cancelled: false,
	}
	err := conn.db.Create(newGroup).Error
	if err != nil {
		return nil, err
	}

	return newGroup, nil
}

func QueueUpdateGroup(conn *gorm.DB, group *QueueGroup) (dbqueuetypes.QueueGroupRecord, error) {
	err := conn.Model(&QueueGroup{}).Where("name = ?", group.Name).Update("started", group.Started).Update("cancelled", group.Cancelled).Error
	if err != nil {
		return nil, err
	}

	err = conn.Where("name = ?", group.Name).First(group).Error
	if err != nil {
		return nil, err
	}

	return group, nil
}

func (conn *store) QueueGroupComplete(id int64) (done bool, cancelled bool, err error) {
	var count int64
	err = conn.db.Transaction(func(tx *gorm.DB) error {
		err = tx.Model(&Queue{}).Where("group_id = ?", id).Count(&count).Error
		if err != nil {
			return err
		}

		if count == 0 {
			// Determine if group was cancelled
			grp := &QueueGroup{}
			err = tx.First(grp, id).Error
			if err != nil {
				return err
			}
			cancelled = grp.Cancelled

			// Delete it
			err = tx.Delete(grp).Error
		}
		return err
	})
	if err != nil {
		return false, false, err
	}

	return count == 0, cancelled, err
}

func (conn *store) IsQueueAddressComplete(address string) (done bool, err error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return false, errors.New("no address provided for IsQueueAddressComplete")
	}

	// Include the address in the transaction description to avoid races when
	// testing with a special locking connection.
	var count int64
	var workErr error
	err = conn.db.Transaction(func(tx *gorm.DB) error {
		err = tx.Model(&Queue{}).Where("address = ?", address).Count(&count).Error
		if err != nil {
			return err
		}

		c2 := &store{
			db: tx,
		}
		workErr = c2.QueueAddressedCheck(address)
		return nil
	})
	if err != nil {
		return false, err
	}

	if workErr != nil {
		err = workErr
	}
	return count == 0, err
}

func (conn *store) IsQueueAddressInProgress(address string) (bool, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return false, errors.New("no address provided for IsQueueAddressInProgress")
	}

	// Get the count of work remaining in the queue for this group
	var count int64
	err := conn.db.Model(&Queue{}).Where("address = ?", address).Count(&count).Error
	if err != nil {
		return false, err
	}

	// Return `true` if work is in the queue
	return count > 0, nil
}

func (conn *store) QueueGroupStart(id int64) error {
	var tx Store
	var err error
	tx, err = conn.BeginTransaction("QueueGroupStart")
	if err != nil {
		return err
	}
	defer tx.CompleteTransaction(&err)

	err = tx.(*store).db.First(&QueueGroup{}, id).Update("started", true).Error
	if err != nil {
		return err
	}

	err = tx.Notify(notifytypes.ChannelMessages, NewDbQueueNotify())
	return nil
}

func (conn *store) QueueGroupClear(id int64) error {
	return conn.db.Delete(&Queue{}, "group_id = ?", id).Error
}

func (conn *store) QueueGroupCancel(id int64) error {
	return conn.db.First(&QueueGroup{}, id).Update("cancelled", true).Error
}

func (conn *store) QueuePushAddressed(name string, groupId sql.NullInt64, priority, workType uint64, address string, work interface{}, carrier []byte) (err error) {
	item, err := json.Marshal(work)
	if err != nil {
		return err
	}

	var gid *int64
	if groupId.Valid {
		tg := groupId.Int64
		gid = &tg
	}
	workRecord := Queue{
		Name:     name,
		Priority: priority,
		Item:     item,
		GroupID:  gid,
		Type:     workType,
		Address:  sql.NullString{String: address, Valid: address != ""},
		Carrier:  carrier,
	}

	var tx Store
	tx, err = conn.BeginTransaction("QueuePushAddressed")
	if err != nil {
		return err
	}
	defer tx.CompleteTransaction(&err)

	err = tx.(*store).db.Create(&workRecord).Error
	if isUniqueIndexViolation(err) {
		err = queue.ErrDuplicateAddressedPush
	}
	if err != nil {
		return err
	}

	err = tx.Notify(notifytypes.ChannelMessages, NewDbQueueNotify())
	return err
}

func isUniqueIndexViolation(err error) bool {
	if err != nil {
		if sqliteErr, ok := err.(sqlite3.Error); ok {
			return sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique ||
				sqliteErr.ExtendedCode == sqlite3.ErrConstraintPrimaryKey
		}
	}
	return false
}

func (conn *store) QueuePush(name string, groupId sql.NullInt64, priority, workType uint64, work interface{}, carrier []byte) (err error) {
	item, err := json.Marshal(work)
	if err != nil {
		return err
	}

	var gid *int64
	if groupId.Valid {
		tg := groupId.Int64
		gid = &tg
	}

	workRecord := Queue{
		Name:     name,
		Priority: priority,
		Item:     item,
		GroupID:  gid,
		Type:     workType,
		Carrier:  carrier,
	}

	var tx Store
	tx, err = conn.BeginTransaction("QueuePush")
	if err != nil {
		return err
	}
	defer tx.CompleteTransaction(&err)

	err = tx.(*store).db.Create(&workRecord).Error
	if err != nil {
		return err
	}

	err = tx.Notify(notifytypes.ChannelMessages, NewDbQueueNotify())
	return err
}

func getPermit(tx *gorm.DB) (QueuePermit, error) {
	p := QueuePermit{}
	err := tx.Create(&p).Error
	return p, err
}

func hasWork(tx *gorm.DB, maxPriority uint64, name string, types []uint64) (bool, error) {

	query := `
        SELECT count(q.id)
		FROM queue q
			LEFT JOIN queue_group g
				ON q.group_id = g.id
		WHERE
			q.priority <= ?
			AND q.permit = 0
			AND q.name = ?
			AND q.type IN (?)
			AND (
				q.group_id IS NULL
				OR g.started = ?
			)`
	var count int
	err := tx.Raw(query, maxPriority, name, types, true).Scan(&count).Error
	if err != nil {
		return false, err
	}

	// Return true if work is available
	return count > 0, nil
}

func claimWork(tx *gorm.DB, permitId uint, maxPriority uint64, name string, types []uint64) error {

	// Note the `FOR UPDATE` clause for Postgres.
	// See https://www.postgresql.org/docs/9.0/static/sql-select.html#SQL-FOR-UPDATE-SHARE
	// This is intended to force Postgres to immediately lock the selected queue rows to
	// prevent other actors from updating them.
	//
	// Excerpt: When FOR UPDATE or FOR SHARE appears at the top level of a SELECT query,
	// the rows that are locked are exactly those that are returned by the query; ...
	// In addition, rows that satisfied the query conditions as of the query snapshot will
	// be locked, although they will not be returned if they were updated after the snapshot
	// and no longer satisfy the query conditions. If a LIMIT is used, locking stops once
	// enough rows have been returned to satisfy the limit (but note that rows skipped
	// over by OFFSET will get locked).
	//
	// When a second actor (node) attempts to run this query simultaneously, it should block
	// until the first UPDATE completes, at which point, the `permit = 0` in the WHERE clause
	// will no longer be satisfied, and should prevent two actors from claiming the same
	// work.

	var err error
	var postgres bool
	if postgres {
		query := `
		WITH cte AS (
			SELECT q.id
			FROM queue q
				LEFT JOIN queue_group g
					ON q.group_id = g.id
			WHERE
				q.priority <= ?
				AND q.permit = 0
				AND q.name = ?
				AND q.type IN (?)
				AND (
					q.group_id IS NULL
					OR g.started = ?
				)
			ORDER BY q.priority ASC, q.id ASC
			LIMIT 1
			FOR UPDATE OF q
		)
		UPDATE queue
		SET permit = ?
		FROM cte
		WHERE queue.id = cte.id`
		err = tx.Exec(query, maxPriority, name, types, true, permitId).Error
	} else {
		query := `
		UPDATE queue
		SET permit = ?
		WHERE id IN (
			SELECT q.id
			FROM queue q
				LEFT JOIN queue_group g
					ON q.group_id = g.id
			WHERE
				q.priority <= ?
				AND q.permit = 0
				AND q.name = ?
				AND q.type IN (?)
				AND (
					q.group_id IS NULL
					OR g.started = ?
				)
			ORDER BY q.priority ASC, q.id ASC
			LIMIT 1
		)`
		res := tx.Exec(query, permitId, maxPriority, name, types, true)
		err = res.Error
	}

	return err
}

func getWork(tx *gorm.DB, permitId uint) ([]byte, string, uint64, []byte, error) {
	// Get work item
	workRecord := Queue{}
	err := tx.First(&workRecord, "permit = ?", permitId).Error
	if err != nil {
		return nil, "", 0, nil, err
	}

	// Unmarshal work
	return workRecord.Item, workRecord.Address.String, workRecord.Type, workRecord.Carrier, nil
}

func (conn *store) QueuePop(name string, maxPriority uint64, types []uint64) (*queue.QueueWork, error) {

	// Avoid empty slice errors or IN clauses
	if len(types) == 0 {
		types = append(types, queuetypes.TYPE_NONE)
	}

	// Important: we do this SELECT outside the transaction to help avoid "database is locked" errors.
	// Since SQLite WAL allows us to read during writes, we can do quick read-only check to see if any
	// work is available in the queue before even starting a transaction or requesting a queue permit.
	//
	// If we see that we potentially have work available (this check is not a guarantee), then we proceed
	// with getting a write lock by starting a transaction and requesting a queue permit. It's still
	// possible that we'll end up finding no work, but this will avoid unnecessary locking
	has, err := hasWork(conn.db, maxPriority, name, types)
	if err != nil {
		return nil, err
	} else if !has {
		return nil, sql.ErrNoRows
	}

	var pt QueuePermit
	result := &queue.QueueWork{}
	err = conn.db.Transaction(func(tx *gorm.DB) error {
		// Get a queue permit
		pt, err = getPermit(tx)
		if err != nil {
			return fmt.Errorf("Error getting queue permit: %s", err)
		}

		// Claim a queue item with the permit
		err = claimWork(tx, pt.ID, maxPriority, name, types)
		if err != nil {
			return fmt.Errorf("Error claiming queue work: %s", err)
		}

		// Get work item
		result.Work, result.Address, result.WorkType, result.Carrier, err = getWork(tx, pt.ID)
		result.Permit = pt.PermitId()
		if errors.Is(gorm.ErrRecordNotFound, err) {
			return err
		} else if err != nil {
			return fmt.Errorf("Error getting claimed queue work: %s", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (conn *store) QueueDelete(permitId permit.Permit) (err error) {
	return conn.db.Transaction(func(tx *gorm.DB) error {
		err = tx.Delete(&Queue{}, "permit = ?", permitId).Error
		if err != nil {
			return err
		}

		err = tx.Delete(&QueuePermit{}, permitId).Error
		return err
	})
}

func (conn *store) QueueAddressedComplete(address string, failure error) (err error) {

	var bytes []byte
	if failure != nil {
		queueError, isQueueError := failure.(*queue.QueueError)
		if isQueueError {
			// Record type of queue.QueueError
			bytes, err = json.Marshal(queueError)
		} else {
			// Record generic error
			bytes, err = json.Marshal(&queue.QueueError{
				Message: failure.Error(),
			})
		}
	}
	// Return if there were any marshaling errors
	if err != nil {
		return err
	}

	var postgres bool

	err = conn.db.Transaction(func(tx *gorm.DB) error {
		if postgres {
			// Lock to prevent race. Don't allow reading until this operation is complete.
			err = tx.Exec("LOCK queue_failure IN ACCESS EXCLUSIVE MODE").Error
			if err != nil {
				return err
			}
		}

		// First, delete any references to this address
		err = tx.Delete(&QueueFailure{}, "address = ?", address).Error
		if err != nil {
			return err
		}

		// Next, if there is an error to report, insert it
		if failure != nil {
			failure := QueueFailure{
				Address: address,
				Error:   string(bytes),
			}
			err = tx.Create(&failure).Error
			if err != nil {
				return err
			}
		}
		return nil
	})
	return
}

type QueueAddressFailure struct {
	Message string `json:"error"`
}

func (err *QueueAddressFailure) Error() string {
	return err.Message
}

func (conn *store) QueueAddressedCheck(address string) error {
	var failure QueueFailure
	err := conn.db.First(&failure, "address = ?", address).Error
	if err == gorm.ErrRecordNotFound {
		return nil
	} else if err != nil {
		return err
	} else {
		if failure.Error != "" {
			var queueError queue.QueueError
			if err := json.Unmarshal([]byte(failure.Error), &queueError); err != nil {
				return fmt.Errorf("error unmarshalling queue.QueueError: %s", err)
			} else {
				return &queueError
			}
		}
	}
	return nil
}

func (conn *store) QueuePermits(name string) ([]dbqueuetypes.QueuePermit, error) {
	permits := make([]QueuePermit, 0)

	// Get a list of permits that have expired
	err := conn.db.Raw(`
		SELECT p.*
		FROM queue_permits p
			INNER JOIN queue q
			ON q.permit = p.id
		WHERE q.name = ?
	`, name).Scan(&permits).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	result := make([]dbqueuetypes.QueuePermit, len(permits))
	for i := range permits {
		result[i] = &permits[i]
	}

	return result, nil
}

func (conn *store) QueuePermitDelete(permitId permit.Permit) error {
	// Delete the expired permit
	err := conn.db.Delete(&QueuePermit{}, permitId).Error
	if err != nil {
		return err
	}

	// Make the queued work associated with the permit available again
	err = conn.db.Model(&Queue{}).Where("permit = ?", permitId).Update("permit", 0).Error
	if err != nil {
		return err
	}

	// Notify that work may be available.
	err = conn.Notify(notifytypes.ChannelMessages, NewDbQueueNotify())
	if err != nil {
		return err
	}

	return nil
}

func (conn *store) QueuePeek(types ...uint64) (results []queue.QueueWork, err error) {

	// Load a list of any queue group work in the queue
	workRecords := make([]Queue, 0)
	err = conn.db.Where("type IN ?", types).Find(&workRecords).Error
	if err != nil {
		return
	}

	results = make([]queue.QueueWork, 0)
	for _, w := range workRecords {
		results = append(results, queue.QueueWork{
			Address:  w.Address.String,
			WorkType: w.Type,
			Work:     w.Item,
		})
	}
	return
}

func (conn *store) QueueGroupGet(name string) (record *QueueGroup, err error) {
	record = &QueueGroup{}
	err = conn.db.Where("name = ?", name).First(record).Error
	return
}

func (conn *store) Notify(channelName string, n interface{}) error {
	msgbytes, err := json.Marshal(n)
	if err != nil {
		return err
	}
	msg := string(msgbytes)

	// Ensure that the channel name is safe for PostgreSQL
	channelName = listenerutils.SafeChannelName(channelName)

	// For Postgres, notify using `pg_notify`
	var postgres bool
	if postgres {
		query := fmt.Sprintf("select pg_notify('%s', $1)", channelName)
		return conn.db.Exec(query, msg).Error
	}

	// For SQLite (single nodes), notify with our own notifier
	if conn.inTransaction {
		// When in a transaction, notifications go into a queue
		conn.mutex.Lock()
		defer conn.mutex.Unlock()
		conn.notifications = append(conn.notifications, queuedNotification{
			channel: channelName,
			n:       n,
		})
	} else {
		// Notify directly if not in a transaction
		conn.llFactory.Notify(channelName, n)
	}

	return nil
}
