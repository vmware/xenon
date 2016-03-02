package client

import "xenon/operation"

type maxInFlightClient struct {
	c Client
	s chan struct{}
}

// MaxInFlight wraps a client to make sure no more than n operations are
// executed concurrently.
func MaxInFlight(c Client, n int) Client {
	m := maxInFlightClient{
		c: c,
		s: make(chan struct{}, n),
	}

	for i := 0; i < n; i++ {
		m.up()
	}

	return &m
}

func (m *maxInFlightClient) up() {
	m.s <- struct{}{}
}

func (m *maxInFlightClient) down() {
	<-m.s
}

func (m *maxInFlightClient) Send(o *operation.Operation) *operation.Operation {
	m.down()
	m.c.Send(o)
	m.up()
	return o
}
