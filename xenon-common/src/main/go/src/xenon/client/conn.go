package client

import (
	"net"
	"xenon"

	"github.com/golang/glog"
)

var MaximumConnections = 64

type Dialer struct {
	sem     semaphore
	dialer  *net.Dialer
	ConnMax int
}

func NewDialer(dialer *net.Dialer, connMax int) *Dialer {
	if connMax == 0 {
		connMax = MaximumConnections
	}

	return &Dialer{
		sem:     make(semaphore, connMax),
		dialer:  dialer,
		ConnMax: connMax,
	}
}

func (d *Dialer) Dial(network, address string) (net.Conn, error) {
	if glog.V(xenon.Debug) && (len(d.sem) == d.ConnMax) {
		glog.Warningf("dialer is at %d max connections (dialing %s)", len(d.sem), address)
	}

	d.sem.P(1)
	c, err := d.dialer.Dial(network, address)
	if err != nil {
		d.sem.V(1)
		return nil, err
	}
	return Conn{c, d.sem.V}, nil
}

type Conn struct {
	net.Conn
	v func(n int)
}

func (c Conn) Close() error {
	e := c.Conn.Close()
	c.v(1)
	return e
}
