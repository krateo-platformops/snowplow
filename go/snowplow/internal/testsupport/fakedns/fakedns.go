// Package fakedns answers this process's DNS lookups from a function, for a
// test: no query leaves the process.
//
// It replaces net.DefaultResolver, because that is the one seam that reaches a
// dialer the code under test does not own (plumbing's). A test that uses it must
// not run in parallel.
package fakedns

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
)

// Answer returns the addresses of name (no trailing dot). No address means the
// name does not exist.
type Answer func(name string) []net.IP

const (
	typeA    = 1
	typeAAAA = 28

	headerLen = 12
)

// Install makes answer the process's resolver until the test ends.
func Install(t testing.TB, answer Answer) {
	t.Helper()
	prev := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			go serve(server, answer)
			return client, nil
		},
	}
	t.Cleanup(func() { net.DefaultResolver = prev })
}

// serve answers the queries of one connection. net.Pipe is no PacketConn, so the
// resolver speaks DNS over TCP on it: each message has a 2-byte length prefix.
func serve(c net.Conn, answer Answer) {
	defer c.Close()
	for {
		var size [2]byte
		if _, err := io.ReadFull(c, size[:]); err != nil {
			return
		}
		q := make([]byte, binary.BigEndian.Uint16(size[:]))
		if _, err := io.ReadFull(c, q); err != nil {
			return
		}
		out, err := respond(q, answer)
		if err != nil {
			return
		}
		binary.BigEndian.PutUint16(size[:], uint16(len(out)))
		if _, err := c.Write(append(size[:], out...)); err != nil {
			return
		}
	}
}

// respond builds the answer to a one-question query.
func respond(q []byte, answer Answer) ([]byte, error) {
	if len(q) < headerLen {
		return nil, errors.New("short query")
	}
	var labels []string
	end := headerLen
	for {
		if end >= len(q) {
			return nil, errors.New("unterminated name")
		}
		n := int(q[end])
		end++
		if n == 0 {
			break
		}
		if end+n > len(q) {
			return nil, errors.New("truncated label")
		}
		labels = append(labels, string(q[end:end+n]))
		end += n
	}
	if end+4 > len(q) {
		return nil, errors.New("no question type")
	}
	qtype := binary.BigEndian.Uint16(q[end : end+2])
	question := q[headerLen : end+4]

	ips := answer(strings.Join(labels, "."))
	var records [][]byte
	for _, ip := range ips {
		switch {
		case qtype == typeA && ip.To4() != nil:
			records = append(records, record(typeA, ip.To4()))
		case qtype == typeAAAA && ip.To4() == nil:
			records = append(records, record(typeAAAA, ip.To16()))
		}
	}

	flags := uint16(0x8180) // a response, recursion desired and available
	if len(ips) == 0 {
		flags |= 3 // NXDOMAIN
	}
	out := make([]byte, headerLen, headerLen+len(question)+len(records)*32)
	copy(out, q[:2]) // the query's id
	binary.BigEndian.PutUint16(out[2:], flags)
	binary.BigEndian.PutUint16(out[4:], 1)
	binary.BigEndian.PutUint16(out[6:], uint16(len(records)))
	out = append(out, question...)
	for _, r := range records {
		out = append(out, r...)
	}
	return out, nil
}

// record is one answer for the question's name (a compression pointer to it).
func record(qtype uint16, rdata []byte) []byte {
	r := []byte{0xc0, headerLen}
	r = binary.BigEndian.AppendUint16(r, qtype)
	r = binary.BigEndian.AppendUint16(r, 1) // class IN
	r = binary.BigEndian.AppendUint32(r, 0) // TTL
	r = binary.BigEndian.AppendUint16(r, uint16(len(rdata)))
	return append(r, rdata...)
}
