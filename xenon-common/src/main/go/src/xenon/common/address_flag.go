package common

import (
	"fmt"
	"net"
)

type AddressFlag struct {
	host string
	port string
}

func (f *AddressFlag) Host() string {
	host := f.host
	if host == "" {
		host = "127.0.0.1"
	}

	return host
}

func (f *AddressFlag) Port() string {
	port := f.port
	if port == "" {
		port = "8000"
	}

	return port
}

func (f *AddressFlag) String() string {
	return fmt.Sprintf("%s:%s", f.Host(), f.Port())
}

func (f *AddressFlag) Set(v string) error {
	host, port, err := net.SplitHostPort(v)
	if err != nil {
		return err
	}

	f.host = host
	f.port = port
	return nil
}
