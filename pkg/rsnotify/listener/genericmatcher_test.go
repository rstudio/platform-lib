package listener

// Copyright (C) 2022 by RStudio, PBC.

import (
	"fmt"

	"gopkg.in/check.v1"
)

type GenericMatcherSuite struct{}

var _ = check.Suite(&GenericMatcherSuite{})

func (s *GenericMatcherSuite) TestNewMatcher(c *check.C) {
	types := make(map[uint8]interface{})
	m := NewMatcher("test")

	c.Assert(m, check.DeepEquals, &GenericMatcher{
		field: "test",
		types: types,
	})
}

func (s *GenericMatcherSuite) TestRegister(c *check.C) {
	const testNotificationType = 1
	type TestNotification struct{}
	m := &GenericMatcher{
		field: "test",
		types: make(map[uint8]interface{}),
	}
	m.Register(testNotificationType, TestNotification{})
	c.Assert(m.types[testNotificationType], check.NotNil)
	c.Assert(len(m.types), check.Equals, 1)
}

func (s *GenericMatcherSuite) TestField(c *check.C) {
	const testNotificationType = 1
	const testField = "test"
	type TestNotification struct{}
	m := &GenericMatcher{
		field: testField,
		types: make(map[uint8]interface{}),
	}
	f := m.Field()
	c.Assert(f, check.Equals, testField)
}

func (s *GenericMatcherSuite) TestType(c *check.C) {
	const testNotificationType = 1
	type TestNotification struct{}
	m := &GenericMatcher{
		field: "test",
		types: make(map[uint8]interface{}),
	}
	m.Register(testNotificationType, TestNotification{})
	c.Assert(m.types[testNotificationType], check.NotNil)

	t, err := m.Type(testNotificationType)
	c.Assert(err, check.IsNil)
	c.Assert(t, check.NotNil)
}

func (s *GenericMatcherSuite) TestUnknownType(c *check.C) {
	const missingNotificationType = 0
	const testNotificationType = 1
	expectedErr := fmt.Errorf("no matcher type found for %d: %w", missingNotificationType, MissingTypeError)
	type TestNotification struct{}
	m := &GenericMatcher{
		field: "test",
		types: make(map[uint8]interface{}),
	}
	m.Register(testNotificationType, TestNotification{})
	c.Assert(m.types[testNotificationType], check.NotNil)

	t, err := m.Type(missingNotificationType)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.DeepEquals, expectedErr)
	c.Assert(t, check.IsNil)
}
