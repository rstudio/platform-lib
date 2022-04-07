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
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerutils"
)

var ErrMessageToLarge = errors.New("notification payload too large")
var ErrChunkTooSmall = errors.New("chunk too small")

var chunkHeaderRegex = regexp.MustCompile("" +
	`^\d{2}/\d{2}:` +
	`[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}` +
	`\n`)

type Provider interface {
	Notify(channelName string, msg []byte) error
}

const (
	defaultMaxChunkSize = 8000
	headerSize          = 43
)

type Notifier struct {
	provider     Provider
	maxChunks    int
	maxChunkSize int
	chunking     bool
	newGuid      func() string
}

func newGuid() string {
	return uuid.NewString()
}

type Args struct {
	Provider     Provider
	Chunking     bool
	MaxChunks    int
	MaxChunkSize int
}

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

func (n *Notifier) Notify(channelName string, notification interface{}) error {
	msg, err := json.Marshal(notification)
	if err != nil {
		return err
	}

	// Ensure that the channel name is safe for Postgres
	channelName = listenerutils.SafeChannelName(channelName)

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
	_ = guid
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

func IsChunk(msg []byte) bool {
	return chunkHeaderRegex.Match(msg)
}

type ChunkInfo struct {
	Chunk   int
	Count   int
	Guid    string
	Message []byte
}

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
