package dns

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/miekg/dns"
	"github.com/mitchellh/mapstructure"

	"xenon/client"
	"xenon/common"
	"xenon/operation"
	"xenon/uri"

	"golang.org/x/net/context"
)

type QueryResult struct {
	common.ServiceDocument
	ServiceLink    string   `json:"serviceLink,omitempty"`
	ServiceName    string   `json:"serviceName,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	HostName       string   `json:"hostName,omitempty"`
	NodeReferences []string `json:"nodeReferences,omitempty"`
}

// Server is used to expose service discovery endpoints using a DNS interface.
type Server struct {
	dnsHandler   *dns.ServeMux
	dnsServer    *dns.Server
	dnsServerTCP *dns.Server
	domain       string
	recursors    []string
}

// Shutdown stops the DNS Servers
func (d *Server) Shutdown() {
	if err := d.dnsServer.Shutdown(); err != nil {
		glog.Errorf("Error stopping udp server: %v", err)
	}
	if err := d.dnsServerTCP.Shutdown(); err != nil {
		glog.Errorf("Error stopping tcp server: %v", err)
	}
}

// NewServer starts a new DNS server
func NewServer(domain string,
	bind string, recursors []string) (*Server, error) {
	// Make sure domain is FQDN
	domain = dns.Fqdn(domain)
	// Construct the DNS multiplexer
	mux := dns.NewServeMux()
	var wg sync.WaitGroup
	wg.Add(2)

	// Setup the godns servers
	server := &dns.Server{
		Addr:              bind,
		Net:               "udp",
		Handler:           mux,
		UDPSize:           65535,
		NotifyStartedFunc: wg.Done,
	}
	serverTCP := &dns.Server{
		Addr:              bind,
		Net:               "tcp",
		Handler:           mux,
		NotifyStartedFunc: wg.Done,
	}

	// Create the server
	srv := &Server{
		dnsHandler:   mux,
		dnsServer:    server,
		dnsServerTCP: serverTCP,
		domain:       domain,
		recursors:    recursors,
	}

	// Register mux handler for handling queries for the configured domain
	mux.HandleFunc(domain, srv.handleQuery)
	// Handle any recursors defined for all other queries
	if len(recursors) > 0 {
		// validate recursors and add to the recursor list
		validatedRecursors := make([]string, len(recursors))
		for idx, recursor := range recursors {
			recursor, err := recursorAddr(recursor)
			if err != nil {
				glog.Warningf("Invalid recursor : %v", err)
			}
			validatedRecursors[idx] = recursor
		}
		srv.recursors = validatedRecursors
		// register a handler for all non domain queries
		mux.HandleFunc(".", srv.handleRecurse)
	}

	// Async start the DNS Servers, handle errors
	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil {
			glog.Errorf("Error starting udp server: %v", err)
			errCh <- fmt.Errorf("dns udp setup failed: %v", err)
		}
	}()

	errChTCP := make(chan error, 1)
	go func() {
		if err := serverTCP.ListenAndServe(); err != nil {
			glog.Errorf("Error starting tcp server: %v", err)
			errChTCP <- fmt.Errorf("dns tcp setup failed: %v", err)
		}
	}()

	// Wait for NotifyStartedFunc callbacks indicating server has started
	startCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(startCh)
	}()

	// Wait for either the startup to complete or error
	select {
	case e := <-errCh:
		return srv, e
	case e := <-errChTCP:
		return srv, e
	case <-startCh:
		return srv, nil
	}
}

// recursorAddr is used to add a port to the recursor if omitted.
func recursorAddr(recursor string) (string, error) {
	// Add the port if none
	if strings.Contains(recursor, ":") == false {
		recursor = fmt.Sprintf("%s:%d", recursor, 53)
	}

	// Get the address
	addr, err := net.ResolveTCPAddr("tcp", recursor)
	if err != nil {
		return "", err
	}
	return addr.String(), nil
}

// handleQuery is used to handle DNS queries in the configured domain
func (d *Server) handleQuery(resp dns.ResponseWriter, req *dns.Msg) {
	// Setup the message response
	m := new(dns.Msg)
	m.SetReply(req)
	m.Authoritative = true
	m.RecursionAvailable = (len(d.recursors) > 0)
	// Dispatch the correct handler
	d.dispatch(req, m)

	// Write out the complete response
	if err := resp.WriteMsg(m); err != nil {
		glog.Errorf("failed to respond: %v", err)
	}
}

// dispatch is used to parse a request and invoke the correct handler
func (d *Server) dispatch(req, resp *dns.Msg) {
	// Get the QName without the domain suffix
	qName := strings.ToLower(dns.Fqdn(req.Question[0].Name))
	qName = strings.TrimSuffix(qName, d.domain)

	// Split into the label parts
	labels := dns.SplitDomainName(qName)
	n := len(labels)
	tag := []string{}
	if n == 0 {
		goto INVALID
	}
	if n > 1 {
		tag = labels[:n-1]
	}
	d.serviceLookup(labels[n-1], tag, req, resp)
	return
INVALID:
	glog.Errorf("QName invalid: %s", qName)
	resp.SetRcode(req, dns.RcodeNameError)
}

// serviceLookup is used to handle a service query
func (d *Server) serviceLookup(service string, tags []string, req, resp *dns.Msg) {
	// create resp
	// construct the query for the http dns query service
	dnsURI := uri.Extend(uri.Local(), "core/dns/query")

	odataFilter := "serviceName eq " + service

	if len(tags) > 0 {
		var buffer bytes.Buffer
		for i, tag := range tags {
			buffer.WriteString(tag)
			if i < (len(tags) - 1) {
				buffer.WriteString(",")
			}
		}
		if buffer.Len() > 0 {
			odataFilter = odataFilter + "and tags eq " + buffer.String()
		}

	}

	encodedQuery, encErr := uri.URLEncoded(("$filter=" + odataFilter))

	if encErr != nil {
		resp.SetRcode(req, dns.RcodeNameError)
		return
	}

	dnsURI.RawQuery = encodedQuery

	glog.Errorf("QName invalid: %s", dnsURI.RawQuery)
	ctx := context.Background()

	op := operation.NewGet(ctx, dnsURI)
	client.Send(op)
	if err := op.Wait(); err != nil {
		resp.SetRcode(req, dns.RcodeNameError)
		return
	}

	var state common.ServiceDocumentQueryResult
	err := op.DecodeBody(&state)
	if err != nil {
		resp.SetRcode(req, dns.RcodeNameError)
		return
	}
	// process SRV records if needed, else process all other record types
	if req.Question[0].Qtype == dns.TypeSRV {
		d.serviceSRVRecords(state.Documents, req, resp)
	} else {
		d.serviceARecords(state.Documents, req, resp)
	}
	if len(resp.Answer) == 0 {
		// no record found, return error
		resp.SetRcode(req, dns.RcodeNameError)
	}
}

func (d *Server) serviceARecords(nodes map[string]interface{}, req, resp *dns.Msg) {
	if nodes != nil {
		for _, v := range nodes {
			var result QueryResult
			err := mapstructure.Decode(v, &result)
			if err != nil {
				glog.Errorf("Error decoding DNS result: %v", err)
				return
			}
			// IP address can be empty as it is not a mandatory field
			if result.NodeReferences != nil {
				glog.Error(result.NodeReferences)
				for _, nodeReference := range result.NodeReferences {
					node, parseErr := uri.Parse(nodeReference)
					if parseErr == nil {
						glog.Error(node.Host)
						records := d.createIPRecord(strings.Split(node.Host, ":")[0], req.Question[0].Name, req.Question[0].Qtype)
						resp.Answer = append(resp.Answer, records...)
					}
				}
			}
		}
	}
}

func (d *Server) createIPRecord(addr, qName string, qType uint16) (records []dns.RR) {
	ip := net.ParseIP(addr)
	if ip == nil {
		return records
	}
	if ipv4 := ip.To4(); ipv4 != nil {
		if qType == dns.TypeANY || qType == dns.TypeA {
			return []dns.RR{&dns.A{
				Hdr: dns.RR_Header{
					Name:   qName,
					Rrtype: dns.TypeA,
					Class:  dns.ClassINET,
					//TODO: set ttl based on config
					Ttl: uint32(0),
				},
				A: ip,
			}}
		}
	} else {
		// if the address can be parsed and it not a IPv4 address,it must be a IPv6 address
		if qType == dns.TypeANY || qType == dns.TypeAAAA {
			return []dns.RR{&dns.AAAA{
				Hdr: dns.RR_Header{
					Name:   qName,
					Rrtype: dns.TypeAAAA,
					Class:  dns.ClassINET,
					Ttl:    uint32(0),
				},
				AAAA: ip,
			}}
		}
	}
	return records
}

func (d *Server) serviceSRVRecords(nodes map[string]interface{}, req, resp *dns.Msg) {
	if nodes != nil {
		for _, v := range nodes {
			var result QueryResult
			err := mapstructure.Decode(v, &result)

			if err != nil {
				glog.Errorf("Error decoding DNS result: %v", err)
				return
			}

			if result.NodeReferences != nil && result.HostName != "" {
				// Add the SRV record
				for _, nodeReference := range result.NodeReferences {
					node, parseErr := uri.Parse(nodeReference)
					if parseErr == nil {
						nodeHost := strings.Split(node.Host, ":")
						ipAddress := nodeHost[0]
						port, err := strconv.Atoi(nodeHost[1])
						if err != nil {
							port = 0
						}
						srvRec := &dns.SRV{
							Hdr: dns.RR_Header{
								Name:   req.Question[0].Name,
								Rrtype: dns.TypeSRV,
								Class:  dns.ClassINET,
								Ttl:    uint32(0),
							},
							Priority: 1,
							Weight:   1,
							Port:     uint16(port),
							Target:   fmt.Sprintf("%s.", result.HostName),
						}
						resp.Answer = append(resp.Answer, srvRec)
						// Add the extra record with the IP address
						records := d.createIPRecord(ipAddress, srvRec.Target, dns.TypeANY)
						resp.Extra = append(resp.Extra, records...)
					}
				}
			}
		}
	}
}

// handleRecurse is used to handle recursive DNS queries
func (d *Server) handleRecurse(resp dns.ResponseWriter, req *dns.Msg) {
	q := req.Question[0]
	network := "udp"

	// Switch to TCP if the client is
	if _, ok := resp.RemoteAddr().(*net.TCPAddr); ok {
		network = "tcp"
	}

	// Recursively resolve
	c := &dns.Client{Net: network}
	var r *dns.Msg
	var err error
	for _, recursor := range d.recursors {
		r, _, err = c.Exchange(req, recursor)
		if err == nil {
			// Forward the response
			if err := resp.WriteMsg(r); err != nil {
				glog.Errorf("writeMsg failed: %v", err)
			}
			return
		}
		glog.Warningf("recurse failed: %v", err)
	}

	// If all resolvers fail, return a SERVFAIL message
	glog.Errorf("All resolvers failed for %v", q)
	m := &dns.Msg{}
	m.SetReply(req)
	m.RecursionAvailable = true
	m.SetRcode(req, dns.RcodeServerFailure)
	resp.WriteMsg(m)
}
