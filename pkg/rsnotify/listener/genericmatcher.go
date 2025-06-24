package listener

// Copyright (C) 2025 By Posit Software, PBC.

import (
	"errors"
	"fmt"
)

// Copyright (C) 2025 By Posit Software, PBC.

var MissingTypeError = errors.New("MissingType error")

type GenericMatcher struct {
	field string
	types map[uint8]interface{}
}

func (m *GenericMatcher) Field() string {
	return m.field
}

func (m *GenericMatcher) Type(notifyType uint8) (interface{}, error) {
	if t, ok := m.types[notifyType]; ok {
		return t, nil
	}
	return nil, fmt.Errorf("no matcher type found for %d: %w", notifyType, MissingTypeError)
}

func (m *GenericMatcher) Register(notifyType uint8, dataType interface{}) {
	m.types[notifyType] = dataType
}

func NewMatcher(field string) *GenericMatcher {
	return &GenericMatcher{
		field: field,
		types: make(map[uint8]interface{}),
	}
}
