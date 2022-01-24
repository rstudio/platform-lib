package storage

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"rspm/storage/types"
)

type PgStorageServer struct {
	pool        *pgxpool.Pool
	class       string
	chunker     chunkUtils
	debugLogger DebugLogger
}

func NewPgServer(class string, chunkSize uint64, waiter ChunkWaiter, notifier ChunkNotifier, cstore PersistentStorageStore, debugLogger DebugLogger) PersistentStorageServer {
	pgs := &PgStorageServer{
		class:       class,
		pool:        cstore.ConnPool(),
		debugLogger: debugLogger,
	}
	return &PgStorageServer{
		class:       class,
		pool:        cstore.ConnPool(),
		debugLogger: debugLogger,
		chunker: &defaultChunkUtils{
			chunkSize:   chunkSize,
			server:      pgs,
			waiter:      waiter,
			notifier:    notifier,
			pollTimeout: chunkPollTimeout,
			maxAttempts: maxChunkAttempts,
		},
	}
}

func pgxCommit(tx pgx.Tx, debugLogger DebugLogger, desc string, err *error) {
	var finErr error
	if *err == nil {
		debugLogger.Debugf("Committing large object on success for operation %s", desc)
		finErr = tx.Commit(context.Background())
	} else {
		debugLogger.Debugf("Rolling back large object on error for operation %s: %s", desc, *err)
		finErr = tx.Rollback(context.Background())
	}
	if finErr != nil {
		debugLogger.Debugf("Error committing large object: %s", finErr)
		if *err == nil {
			*err = finErr
		}
	}
}

// Extends pgx.LargeObject to commit the transaction and release the pool
// connection when the reader is closed
type LargeObjectCloser struct {
	*pgx.LargeObject
	pool     *pgxpool.Pool
	conn     *pgxpool.Conn
	tx       pgx.Tx
	op       string
	location string

	debugLogger DebugLogger
}

func (f *LargeObjectCloser) Close() error {
	// Create a nil error and commit the transaction
	var err error
	pgxCommit(f.tx, f.debugLogger, fmt.Sprintf("%s %s", f.op, f.location), &err)

	f.LargeObject.Close()

	// Release the connection
	f.conn.Release()

	return nil
}

func newLargeObjectCloser(lo *pgx.LargeObject, pool *pgxpool.Pool, conn *pgxpool.Conn, tx pgx.Tx, debuglogger DebugLogger, op, location string) *LargeObjectCloser {
	return &LargeObjectCloser{
		LargeObject: lo,
		pool:        pool,
		conn:        conn,
		tx:          tx,
		op:          op,
		location:    location,
		debugLogger: debuglogger,
	}
}

func (s *PgStorageServer) Check(dir, address string) (found bool, chunked *ChunksInfo, sz int64, ts time.Time, err error) {
	// Look up the large object (see if it exists) in our mapping table
	var dbOid uint32
	location := path.Join(s.class, dir, address)
	query := `SELECT oid FROM large_objects WHERE address = $1`
	var rows pgx.Rows
	if rows, err = s.pool.Query(context.Background(), query, location); err != nil {
		return
	}

	defer rows.Close()
	if !rows.Next() {
		// If the item was not found, check to see if it was chunked. If so, the original address
		// will be a directory containing an `info.json` file.
		infoLocation := path.Join(location, "info.json")
		if rows, err = s.pool.Query(context.Background(), query, infoLocation); err != nil {
			if err == sql.ErrNoRows {
				err = nil
			}
			return
		}
		defer rows.Close()
		if !rows.Next() {
			return
		}
		chunked = &ChunksInfo{}
	}

	// For regular (not chunked) assets, this loads the OID for the asset. For
	// chunked assets, this loads the OID for the chunked asset's `info.json`.
	err = rows.Scan(&dbOid)
	if err != nil {
		return
	}

	native, err := s.pool.Acquire(context.Background())
	if err != nil {
		return
	}

	defer native.Release()

	var tx pgx.Tx
	if tx, err = native.Begin(context.Background()); err != nil {
		return
	}
	defer pgxCommit(tx, s.debugLogger, fmt.Sprintf("Check %s", location), &err)

	// Get a LargeObjects instance. This lets us interact with the Postgres
	// large object store
	los := tx.LargeObjects()

	// Open the large object
	s.debugLogger.Debugf("Opening (for check) large object %s with oid %d.", location, dbOid)
	var lo *pgx.LargeObject
	if lo, err = los.Open(context.Background(), dbOid, pgx.LargeObjectModeRead); err != nil {
		return
	}
	defer func(err *error) {
		var finErr error
		s.debugLogger.Debugf("Closing (after check) large object %s with oid %d.", location, dbOid)
		finErr = lo.Close()
		if finErr != nil {
			if *err == nil {
				*err = finErr
			}
		}
	}(&err)

	if chunked != nil {
		// Read the info.json file
		dec := json.NewDecoder(lo)
		err = dec.Decode(chunked)
		if err != nil {
			return
		}
		sz = int64(chunked.FileSize)
		ts = chunked.ModTime
	} else {
		// Seek to the end to get the file size.
		// TODO: This may be inefficient. Research other ways of getting the correct size
		// TODO: also return the `ts` (modification time)
		if sz, err = lo.Seek(0, io.SeekEnd); err != nil {
			s.debugLogger.Debugf("failed during seek: %s", err)
			return
		}
	}

	found = true
	return
}

func (s *PgStorageServer) Get(dir, address string) (f io.ReadCloser, chunks *ChunksInfo, sz int64, lastMod time.Time, found bool, err error) {
	var chunked bool
	// Look up the large object (see if it exists) in our mapping table
	location := path.Join(s.class, dir, address)
	var dbOid uint32
	var rows pgx.Rows
	query := `SELECT oid FROM large_objects WHERE address = $1`
	if rows, err = s.pool.Query(context.Background(), query, location); err != nil {
		return
	} else {
		defer rows.Close()
		if rows.Next() {
			err = rows.Scan(&dbOid)
			if err != nil {
				return
			}
		} else {
			// If the item was not found, check to see if it was chunked. If so, the original address
			// will be a directory containing an `info.json` file.
			infoLocation := path.Join(location, "info.json")
			if rows, err = s.pool.Query(context.Background(), query, infoLocation); err != nil {
				return
			} else {
				defer rows.Close()
				if rows.Next() {
					err = rows.Scan(&dbOid)
					if err != nil {
						return
					}
					chunked = true
				} else {
					return
				}
			}
		}
	}

	if chunked {
		// Read the info.json file
		f, chunks, sz, lastMod, err = s.chunker.ReadChunked(dir, address)
		if err != nil {
			return
		}
	} else {
		var native *pgxpool.Conn
		native, err = s.pool.Acquire(context.Background())
		if err != nil {
			return
		}

		var tx pgx.Tx
		if tx, err = native.Begin(context.Background()); err != nil {
			return
		}

		// Get a LargeObjects instance. This lets us interact with the Postgres
		// large object store
		los := tx.LargeObjects()

		// Open the large object
		s.debugLogger.Debugf("Opening (for read) large object %s with oid %d.", location, dbOid)
		var lo *pgx.LargeObject
		if lo, err = los.Open(context.Background(), dbOid, pgx.LargeObjectModeRead); err != nil {
			return
		}

		// Get size by seeking to the end
		sz, err = lo.Seek(0, io.SeekEnd)
		if err != nil {
			return
		}

		// Go back to the beginning
		_, err = lo.Seek(0, io.SeekStart)
		if err != nil {
			return
		}

		// Get a closer that knows how to clean up the connection after we're done
		// reading from the large object we pass back
		f = newLargeObjectCloser(lo, s.pool, native, tx, s.debugLogger, "Get", location)
	}

	found = true
	return
}

func (s *PgStorageServer) Flush(dir, address string) {
	// No-op
}

func (s *PgStorageServer) PutChunked(resolve Resolver, dir, address string, sz uint64) (string, string, error) {
	if address == "" {
		return "", "", fmt.Errorf("cache only supports pre-addressed chunked put commands")
	}
	if sz == 0 {
		return "", "", fmt.Errorf("cache only supports pre-sized chunked put commands")
	}
	err := s.chunker.WriteChunked(dir, address, sz, resolve)
	if err != nil {
		return "", "", err
	}

	return dir, address, nil
}

func (s *PgStorageServer) Dir() string {
	return "pg:" + s.class
}

func (s *PgStorageServer) Type() StorageType {
	return StorageTypePostgres
}

func (s *PgStorageServer) CalculateUsage() (types.Usage, error) {
	// Currently unused.
	return types.Usage{}, fmt.Errorf("server PgStorageServer does not implement CalculateUsage")
}

func (s *PgStorageServer) Put(resolve Resolver, dir, address string) (dirOut, addrOut string, err error) {

	var permanentLocation string

	native, err := s.pool.Acquire(context.Background())
	if err != nil {
		return
	}
	defer native.Release()

	var tx pgx.Tx
	if tx, err = native.Begin(context.Background()); err != nil {
		return
	}
	defer pgxCommit(tx, s.debugLogger, fmt.Sprintf("Cache %s", permanentLocation), &err)

	// Get a LargeObjects instance. This lets us interact with the Postgres
	// large object store
	los := tx.LargeObjects()

	// Create a new large object
	var oid uint32
	if oid, err = los.Create(context.Background(), 0); err != nil {
		s.debugLogger.Debugf("Error creating large object: %s", err)
		return
	}

	// Open the new large object
	var lo *pgx.LargeObject
	if lo, err = los.Open(context.Background(), oid, pgx.LargeObjectModeWrite); err != nil {
		s.debugLogger.Debugf("Error opening large object: %s", err)
		return
	}

	// Create a temporary location for the item
	tempLocation := uuid.New().String()

	// Insert the large object's OID in the mapping table
	insert := `INSERT INTO large_objects (oid, address) VALUES ($1, $2)`
	if _, err = tx.Exec(context.Background(), insert, oid, tempLocation); err != nil {
		s.debugLogger.Debugf("Error inserting large object into mapping table: %s", err)
		return
	}

	// Copy the staging file to the large object
	s.debugLogger.Debugf("Copying data to large object")
	wdir, waddress, err := resolve(lo)
	if err != nil {
		s.debugLogger.Debugf("Error copying/resolving large object to Postgres LO storage: %s", err)
		return
	}

	// If no dir and address were provided, use the ones optionally returned
	// from the resolver function
	if dir == "" && address == "" {
		dir = wdir
		address = waddress
	}

	// Calculate the permanent address
	permanentLocation = path.Join(s.class, dir, address)

	// Remove any conflicting mappings
	delete := `DELETE FROM large_objects WHERE address = $1`
	if _, err = tx.Exec(context.Background(), delete, permanentLocation); err != nil {
		s.debugLogger.Debugf("Error deleting existing large object records from mapping table: %s", err)
		return
	}

	// Rename the location
	rename := `UPDATE large_objects SET address = $1 WHERE address = $2`
	if _, err = tx.Exec(context.Background(), rename, permanentLocation, tempLocation); err != nil {
		s.debugLogger.Debugf("Error setting large object record permanent address in mapping table: %s", err)
		return
	}

	if err = lo.Close(); err != nil {
		s.debugLogger.Debugf("Error closing large object: %s", err)
		return
	}

	dirOut = dir
	addrOut = address
	return
}

func (s *PgStorageServer) Remove(dir, address string) (err error) {

	ok, chunked, _, _, err := s.Check(dir, address)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	if chunked != nil {
		// Delete chunks
		for i := uint64(1); i <= chunked.NumChunks; i++ {
			chunk := fmt.Sprintf("%08d", i)
			addr := NotEmptyJoin([]string{s.class, dir, address, chunk}, "/")
			err = s.rem(addr)
			if err != nil {
				return err
			}
		}
		// Delete "info.json"
		addr := NotEmptyJoin([]string{s.class, dir, address, "info.json"}, "/")
		err = s.rem(addr)
	} else {
		location := path.Join(s.class, dir, address)
		return s.rem(location)
	}

	return
}

func (s *PgStorageServer) rem(location string) (err error) {

	native, err := s.pool.Acquire(context.Background())
	if err != nil {
		return
	}
	defer native.Release()

	var tx pgx.Tx
	if tx, err = native.Begin(context.Background()); err != nil {
		return
	}
	defer pgxCommit(tx, s.debugLogger, "remove", &err)

	// Get a LargeObjects instance. This lets us interact with the Postgres
	// large object store
	los := tx.LargeObjects()

	// Look up the large object (see if it exists) in our mapping table
	var oid uint32
	query := `SELECT oid FROM large_objects WHERE address = $1`
	row := tx.QueryRow(context.Background(), query, location)
	if err = row.Scan(&oid); err == pgx.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		return
	}

	// Remove the mapping
	delete := `DELETE FROM large_objects WHERE address = $1`
	if _, err = tx.Exec(context.Background(), delete, location); err != nil {
		s.debugLogger.Debugf("Error deleting large object record from mapping table: %s", err)
		return
	}

	// Remove the large object
	err = los.Unlink(context.Background(), oid)
	return
}

func (s *PgStorageServer) Enumerate() (items []PersistentStorageItem, err error) {
	query := `SELECT address FROM large_objects ORDER BY address`
	items = make([]PersistentStorageItem, 0)
	var rows pgx.Rows
	if rows, err = s.pool.Query(context.Background(), query); err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		return
	} else {
		defer rows.Close()
		for rows.Next() {
			var address string
			err = rows.Scan(&address)
			if err != nil {
				return
			}
			// If item is not for the correct class, skip it
			if !strings.HasPrefix(address, s.class+"/") {
				continue
			}
			var relPath string
			relPath, err = filepath.Rel(s.class, address)
			if err != nil {
				return
			}
			dir := filepath.Dir(relPath)
			if dir == "." {
				dir = ""
			}
			items = append(items, PersistentStorageItem{
				Dir:     dir,
				Address: filepath.Base(relPath),
			})
		}
	}

	items = filterChunks(items)
	return
}

func (s *PgStorageServer) move(dir, address string, server PersistentStorageServer) (err error) {
	parts, err := s.parts(dir, address)
	if err != nil {
		return
	}

	native, err := s.pool.Acquire(context.Background())
	if err != nil {
		return
	}
	defer native.Release()

	var tx pgx.Tx
	if tx, err = native.Begin(context.Background()); err != nil {
		return
	}
	defer pgxCommit(tx, s.debugLogger, "move", &err)

	// Update the mappings
	for _, part := range parts {
		source := path.Join(s.class, part.Dir, part.Address)
		destination := server.Locate(part.Dir, part.Address)
		delete := `UPDATE large_objects SET address = $1 WHERE address = $2`
		if _, err = tx.Exec(context.Background(), delete, destination, source); err != nil {
			s.debugLogger.Debugf("Error updating (move) large object record in mapping table: %s", err)
			return
		}
	}

	return
}

func (s *PgStorageServer) parts(dir, address string) ([]CopyPart, error) {
	ok, chunked, _, _, err := s.Check(dir, address)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("the PostgreSQL object with dir=%s and address=%s to copy does not exist", dir, address)
	}
	parts := make([]CopyPart, 0)
	if chunked != nil {
		if !chunked.Complete {
			return nil, fmt.Errorf("the PostgreSQL chunked object with dir=%s and address=%s to copy is incomplete", dir, address)
		}
		chunkDir := filepath.Join(dir, address)
		parts = append(parts, newCopyPart(chunkDir, "info.json"))
		for i := 1; i <= int(chunked.NumChunks); i++ {
			chunkName := fmt.Sprintf("%08d", i)
			parts = append(parts, newCopyPart(chunkDir, chunkName))
		}
		return parts, nil
	} else {
		return []CopyPart{newCopyPart(dir, address)}, nil
	}
}

func (s *PgStorageServer) Move(dir, address string, server PersistentStorageServer) error {
	copy := true
	switch server.(type) {
	case *PgStorageServer:
		// Attempt move
		err := s.move(dir, address, server)
		if err == nil {
			copy = false
		}
	default:
		// Don't do anything. Just copy
	}

	// Copy the file
	if copy {
		// Copy the file
		err := s.Copy(dir, address, server)
		if err != nil {
			return err
		}

		// Then, remove the file
		err = s.Remove(dir, address)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *PgStorageServer) Copy(dir, address string, server PersistentStorageServer) error {
	f, chunked, sz, _, ok, err := s.Get(dir, address)
	if err == nil && !ok {
		return fmt.Errorf("the PostgreSQL large object with dir=%s and address=%s to copy does not exist", dir, address)
	} else if err != nil {
		return err
	}

	install := func(file io.ReadCloser) Resolver {
		return func(writer io.Writer) (string, string, error) {
			_, err := io.Copy(writer, file)
			return "", "", err
		}
	}

	// Use the server Base() in case the server is wrapped, e.g., `MetadataPersistentStorageServer`
	if chunked != nil {
		_, _, err = server.Base().PutChunked(install(f), dir, address, uint64(sz))
	} else {
		_, _, err = server.Base().Put(install(f), dir, address)
	}
	return err
}

func (s *PgStorageServer) Locate(dir, address string) string {
	location := path.Join(s.class, dir, address)
	return location
}

func (s *PgStorageServer) Base() PersistentStorageServer {
	return s
}
