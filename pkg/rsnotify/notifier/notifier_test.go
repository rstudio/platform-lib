package notifier

// Copyright (C) 2022 by RStudio, PBC.

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"gopkg.in/check.v1"
)

type NotifierSuite struct{}

func TestPackage(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&NotifierSuite{})

type fakeProvider struct {
	notified []string
}

type fakeNotification struct {
	Name    string
	Visited []string
}

func (f *fakeProvider) Notify(ctx context.Context, channelName string, msg []byte) error {
	if f.notified == nil {
		f.notified = make([]string, 0)
	}
	f.notified = append(f.notified, string(msg))
	return nil
}

func (s *NotifierSuite) TestNew(c *check.C) {
	p := &fakeProvider{}
	n := NewNotifier(Args{Provider: p, MaxChunks: 20, MaxChunkSize: 500, Chunking: true})
	n.newGuid = nil
	c.Assert(n, check.DeepEquals, &Notifier{
		provider:     p,
		maxChunks:    20,
		maxChunkSize: 500,
		chunking:     true,
	})

	n = NewNotifier(Args{Provider: p, MaxChunks: 20})
	n.newGuid = nil
	c.Assert(n, check.DeepEquals, &Notifier{
		provider:     p,
		maxChunks:    20,
		maxChunkSize: defaultMaxChunkSize,
		chunking:     false,
	})
}

func (s *NotifierSuite) TestNotify(c *check.C) {
	p := &fakeProvider{}
	n := NewNotifier(Args{Provider: p, MaxChunks: 10, Chunking: true})
	err := n.Notify(context.Background(), "test", &fakeNotification{
		Name: "John Doe",
		Visited: []string{
			"USA",
			"Canada",
		},
	})
	c.Assert(err, check.IsNil)
	c.Assert(p.notified, check.DeepEquals, []string{
		`{"Name":"John Doe","Visited":["USA","Canada"]}`,
	})
}

func (s *NotifierSuite) TestNotifyChunks(c *check.C) {
	newGuid := func() string {
		return "254d1bd9-aa29-4116-97e8-e9c302b7dd84"
	}
	p := &fakeProvider{}
	n := NewNotifier(Args{Provider: p, MaxChunks: 10, MaxChunkSize: 77, Chunking: true})
	n.newGuid = newGuid
	notification := &fakeNotification{
		Name: "John Doe",
		Visited: []string{
			"New York",
			"Tokyo",
			"Chicago",
			"Paris",
			"London",
			"Munich",
		},
	}
	err := n.Notify(context.Background(), "test", notification)
	c.Assert(err, check.IsNil)
	c.Assert(p.notified, check.DeepEquals, []string{
		"01/03:254d1bd9-aa29-4116-97e8-e9c302b7dd84\n{\"Name\":\"John Doe\",\"Visited\":[\"New",
		"02/03:254d1bd9-aa29-4116-97e8-e9c302b7dd84\n York\",\"Tokyo\",\"Chicago\",\"Paris\",\"",
		"03/03:254d1bd9-aa29-4116-97e8-e9c302b7dd84\nLondon\",\"Munich\"]}",
	})

	// Turn off chunking
	n.chunking = false
	p.notified = make([]string, 0)
	err = n.Notify(context.Background(), "test", notification)
	c.Assert(err, check.IsNil)
	c.Assert(p.notified, check.DeepEquals, []string{
		"{\"Name\":\"John Doe\",\"Visited\":[\"New York\",\"Tokyo\",\"Chicago\",\"Paris\",\"London\",\"Munich\"]}",
	})
}

func (s *NotifierSuite) TestNotifyTooLarge(c *check.C) {
	p := &fakeProvider{}
	n := NewNotifier(Args{Provider: p, MaxChunks: 1, MaxChunkSize: 20, Chunking: true})
	err := n.Notify(context.Background(), "test", &fakeNotification{
		Name: "John Doe",
		Visited: []string{
			"New York",
			"Tokyo",
		},
	})
	c.Assert(err, check.DeepEquals, ErrMessageToLarge)
}

func (s *NotifierSuite) TestHeader(c *check.C) {
	guid := uuid.New().String()
	h := chunkHeader(4, 11, guid)
	c.Assert(h, check.DeepEquals, fmt.Sprintf("04/11:%s\n", guid))
	c.Assert(len(h), check.Equals, headerSize)
}

func (s *NotifierSuite) TestIsChunk(c *check.C) {
	cases := []struct {
		val    string
		desc   string
		result bool
	}{
		{
			val:    "03/11:254d1bd9-aa29-4116-97e8-e9c302b7dd84\n{something: then}",
			desc:   "ok",
			result: true,
		},
		{
			val:    "3/11:254d1bd9-aa29-4116-97e8-e9c302b7dd84\n{something: then}",
			desc:   "bad chunk number",
			result: false,
		},
		{
			val:    "03/11:254d1bd9-aa29-4116-97e8-e9c302b7dd84\r{something: then}",
			desc:   "no newline separator",
			result: false,
		},
		{
			val:    "03/11:254d1bd9-aa294116-97e8-e9c302b7dd84\n{something: then}",
			desc:   "bad UUID",
			result: false,
		},
	}
	for _, test := range cases {
		c.Assert(IsChunk([]byte(test.val)), check.Equals, test.result, check.Commentf(test.desc))
	}
}

func (s *NotifierSuite) TestParseChunk(c *check.C) {
	cases := []struct {
		val    string
		desc   string
		result ChunkInfo
		err    string
	}{
		{
			val:  "03/11:254d1bd9-aa29-4116-97e8-e9c302b7dd84\n{\"something\":\"then\"}",
			desc: "ok",
			result: ChunkInfo{
				Chunk:   3,
				Count:   11,
				Guid:    "254d1bd9-aa29-4116-97e8-e9c302b7dd84",
				Message: []byte(`{"something":"then"}`),
			},
		},
		{
			val:  "12/37:254d1bd9-aa29-4116-97e8-e9c302b7dd84\n{\"something\":\"then\",\"age\":3}",
			desc: "ok 2",
			result: ChunkInfo{
				Chunk:   12,
				Count:   37,
				Guid:    "254d1bd9-aa29-4116-97e8-e9c302b7dd84",
				Message: []byte(`{"something":"then","age":3}`),
			},
		},
		{
			val:  "03/11:254d1bd9-aa29-4116-97e8-e9c302b7dd84\n",
			desc: "not enough data",
			err:  ErrChunkTooSmall.Error(),
		},
		{
			val:  "0a/11:254d1bd9-aa29-4116-97e8-e9c302b7dd84\n{\"something\":\"then\"}",
			desc: "bad chunk number",
			err:  "strconv.Atoi: parsing \"0a\": invalid syntax",
		},
		{
			val:  "03/b1:254d1bd9-aa29-4116-97e8-e9c302b7dd84\n{\"something\":\"then\"}",
			desc: "bad total number",
			err:  "strconv.Atoi: parsing \"b1\": invalid syntax",
		},
	}
	for _, test := range cases {
		r, err := ParseChunk([]byte(test.val))
		if test.err != "" {
			c.Assert(err, check.ErrorMatches, test.err, check.Commentf(test.desc))
		} else {
			c.Assert(err, check.IsNil, check.Commentf(test.desc))
			c.Assert(r, check.DeepEquals, test.result, check.Commentf(test.desc))
		}
	}
}
