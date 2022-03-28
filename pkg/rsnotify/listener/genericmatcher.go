package listener

// Copyright (C) 2022 by RStudio, PBC.

type GenericMatcher struct {
	field string
	types map[uint8]interface{}
}

func (m *GenericMatcher) Field() string {
	return m.field
}

func (m *GenericMatcher) Type(notifyType uint8) (interface{}, error) {
	return m.types[notifyType], nil
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
