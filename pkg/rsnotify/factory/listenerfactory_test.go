package listenerfactory

/* listenerfactory_test.go
 *
 * Copyright (C) 2021 by RStudio, PBC
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property of
 * RStudio, PBC and its suppliers, if any. The intellectual and technical
 * concepts contained herein are proprietary to RStudio, PBC and its suppliers
 * and may be covered by U.S. and Foreign Patents, patents in process, and
 * are protected by trade secret or copyright law. Dissemination of this
 * information or reproduction of this material is strictly forbidden unless
 * prior written permission is obtained.
 */

import (
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lib/pq"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/locallistener"
)

type ListenerFactorySuite struct{}

type testFactory struct{}

func (f *testFactory) NewListener() (*pq.Listener, error) {
	return nil, nil
}

func (f *testFactory) IP() (string, error) {
	return "", nil
}

var _ = check.Suite(&ListenerFactorySuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }

type fakeStore struct {
	llf *locallistener.LocalListenerFactory
}

func (f *fakeStore) GetLocalListenerFactory() *locallistener.LocalListenerFactory {
	return f.llf
}

func (s *ListenerFactorySuite) TestNewListener(c *check.C) {
	llf := locallistener.NewLocalListenerFactory()
	cstore := &fakeStore{
		llf: llf,
	}
	pool := &pgxpool.Pool{}
	l := NewLocalListenerFactory(cstore.GetLocalListenerFactory())
	c.Check(l, check.DeepEquals, &LocalListenerFactory{
		llf: llf,
		commonListenerFactory: commonListenerFactory{
			unmarshallers: make(map[uint8]listener.Unmarshaller),
		},
	})
	lgr := &listener.TestLogger{}
	l2 := NewPgxListenerFactory(pool, lgr)
	c.Check(l2, check.DeepEquals, &PgxListenerFactory{
		pool:        pool,
		debugLogger: lgr,
		commonListenerFactory: commonListenerFactory{
			unmarshallers: make(map[uint8]listener.Unmarshaller),
		},
	})
	fakeFactory := &testFactory{}
	l3 := NewPqListenerFactory(fakeFactory, lgr)
	c.Check(l3, check.DeepEquals, &PqListenerFactory{
		factory:     fakeFactory,
		debugLogger: lgr,
		commonListenerFactory: commonListenerFactory{
			unmarshallers: make(map[uint8]listener.Unmarshaller),
		},
	})
}
