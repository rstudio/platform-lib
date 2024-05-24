package notifier

// Copyright (C) 2022 by RStudio, PBC.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/google/uuid"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listenerutils"
)

// ErrMessageToLarge Breaking the message into chunks will result in
// more chunks than the `maxChunks` permitted.
var ErrMessageToLarge = errors.New("notification payload too large")

// ErrChunkTooSmall Received a chunk that is <= the header size, which
// means the chunk contains no data.
var ErrChunkTooSmall = errors.New("chunk too small")

// Matches a chunked message that starts with the format:
// 01/03:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\n
var chunkHeaderRegex = regexp.MustCompile("" +
	`^\d{2}/\d{2}:` +
	`[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}` +
	`\n`)

// Provider is implemented by the end user and passes messages or
// chunks from the Notifier to the messaging system, e.g., Postgres
// NOTIFY.
type Provider interface {
	Notify(channelName string, msg []byte) error
}

const (
	// If not otherwise specified, use this as the maximum chunk size.
	// Defaults to 8000 since Postgres supports up to 8000 bytes for
	// NOTIFY.
	defaultMaxChunkSize = 8000

	// Must be set to exactly the chunk header size. Currently, this is
	// -  5 bytes for the chunk number / count +
	// -  1 byte for the colon separator +
	// - 36 bytes for the UUID +
	// -  1 byte for the trailing newline.
	headerSize = 43
)

type Notifier struct {
	provider     Provider
	maxChunks    int
	maxChunkSize int
	chunking     bool

	// UUID generator. This is very useful for testing since you can
	// replace the function and get back a constant UUID value.
	newGuid func() string
}

func newGuid() string {
	return uuid.NewString()
}

type Args struct {
	// Provider is used to send notifications to the system of your choice.
	Provider Provider

	// Chunking enables/disables chunking. Some messaging Providers may not
	// require chunking.
	Chunking bool

	// MaxChunks is the maximum number of chunks allowed for a message. Keep
	// this small to prevent notifications abuse.
	MaxChunks int

	// MaxChunkSize defaults to `defaultMaxChunkSize. Maximum size of a message
	// without chunking into smaller messages.
	MaxChunkSize int
}

// NewNotifier creates a new Notifier that is used to send notifications.
func NewNotifier(args Args) *Notifier {
	if args.MaxChunkSize == 0 {
		args.MaxChunkSize = defaultMaxChunkSize
	}
	return &Notifier{
		provider:     args.Provider,
		chunking:     args.Chunking,
		maxChunks:    args.MaxChunks,
		maxChunkSize: args.MaxChunkSize,
		newGuid:      newGuid,
	}
}

// Notify sends a regular or chunked message via the Provider. The Notifier
// configuration (see Args) determines when and how messages are chunked.
func (n *Notifier) Notify(channelName string, notification interface{}) error {
	msg, err := json.Marshal(notification)
	if err != nil {
		return err
	}

	// Ensure that the channel name is safe for Postgres
	channelName = listenerutils.SafeChannelName(channelName)

	// When chunking is disabled, or when the message length is <= maxChunkSize,
	// `makeChunks` will return a single chunk that is identical to the original
	// message.
	chunks, err := n.makeChunks(msg)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		err = n.provider.Notify(channelName, chunk)
		if err != nil {
			return err
		}
	}

	return nil
}

func chunkHeader(chunk, total int, guid string) string {
	return fmt.Sprintf("%02d/%02d:%s\n", chunk, total, guid)
}

func (n *Notifier) makeChunks(msg []byte) ([][]byte, error) {
	sz := len(msg)

	// Return original message if within size limit, or if chunking
	// is disabled.
	if !n.chunking || sz <= n.maxChunkSize {
		return [][]byte{msg}, nil
	}

	// Err if message is too large
	payloadPerChunk := n.maxChunkSize - headerSize
	if sz > n.maxChunks*payloadPerChunk {
		return nil, ErrMessageToLarge
	}

	// A chunked message has the format:
	// 01/03:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\n
	// Where:
	// * 01 - The chunk number
	// * 03 - Total number of chunks
	// * xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx - A UUID associated with this message
	results := make([][]byte, 0)
	buf := bytes.NewBuffer(msg)
	guid := n.newGuid()
	for {
		next := make([]byte, payloadPerChunk)
		szRead, err := buf.Read(next)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		// Prevent \x00 nil characters trailing relevant data.
		if szRead < payloadPerChunk {
			next = next[0:szRead]
		}
		results = append(results, next)
	}

	count := len(results)
	for i := range results {
		header := chunkHeader(i+1, count, guid)
		results[i] = append([]byte(header), results[i]...)
	}

	return results, nil
}

// IsChunk returns `true` if a message matches the chunk header regex.
func IsChunk(msg []byte) bool {
	return chunkHeaderRegex.Match(msg)
}

// ChunkInfo is used by ParseChunk to return parsed chunk data.
type ChunkInfo struct {
	// Chunk is the current chunk number.
	Chunk int

	// Count is the total count of chunks for this message.
	Count int

	// Guid associates chunks for a single message.
	Guid string

	// Message contains the data for this chunk.
	Message []byte
}

// ParseChunk parses a message chunk and returns a ChunkInfo
// struct populated with the chunk data.
func ParseChunk(msg []byte) (result ChunkInfo, err error) {
	// No chunk should be <= the header size
	if len(msg) <= headerSize {
		err = ErrChunkTooSmall
		return
	}

	// Get the result
	result.Chunk, err = strconv.Atoi(string(msg[0:2]))
	if err != nil {
		return
	}
	result.Count, err = strconv.Atoi(string(msg[3:5]))
	if err != nil {
		return
	}
	result.Guid = string(msg[6:42])
	result.Message = msg[headerSize:]
	return
}
