package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"xenon/dns"

	"github.com/golang/glog"
)

// Command line args
type CommandArgs struct {
	bindAddr   string
	domainName string
	recursors  string
}

var (
	cArgs CommandArgs
)

func init() {
	flag.StringVar(&cArgs.bindAddr, "bindAddr", "127.0.0.1:8600", "Interface to bind on")
	flag.StringVar(&cArgs.domainName, "domain", "xenon", "domain name for the dns server")
	flag.StringVar(&cArgs.recursors, "recursors", "", "Comma separated list of recursors")
}

// start the dns server with the specified args
func main() {

	flag.Parse()
	recursors := strings.Split(cArgs.recursors, ",")
	server, err := dns.NewServer(
		cArgs.domainName, cArgs.bindAddr, recursors)
	if err != nil {
		glog.Fatalf(fmt.Sprintf("Error starting dns server: %v", err))
		os.Exit(1)
	}
	waitForTerm()
	glog.Info("Shutting down dns server")
	server.Shutdown()
	os.Exit(0)
}

// wait for server to be shutdown
func waitForTerm() {
	// create a channel and wait for sigint or sigterm
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM)
	signal.Notify(sigchan, syscall.SIGINT)
	<-sigchan
}
