// Package PP2PLink provides FIFO reliable delivery over TCP.
// Each destination has a serial sender. ACKs and sequence numbers allow
// reconnection without duplicate delivery. Peers must eventually be available.
package PP2PLink

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type ReqMessage struct {
	To      int
	Message string
}
type IndMessage struct {
	From    int
	Message string
}
type packet struct {
	From    int
	Seq     uint64
	Message string
}
type ack struct{ Seq uint64 }

type PP2PLink struct {
	Req       chan ReqMessage
	Ind       chan IndMessage
	id        int
	addresses []string
	queues    []chan string
	mu        sync.Mutex
	last      []uint64
	listener  net.Listener
}

func NewPP2PLink(addresses []string, id int) (*PP2PLink, error) {
	if id < 0 || id >= len(addresses) || len(addresses) == 0 {
		return nil, fmt.Errorf("invalid process ID %d", id)
	}
	l, err := net.Listen("tcp", addresses[id])
	if err != nil {
		return nil, err
	}
	p := &PP2PLink{Req: make(chan ReqMessage, 4096), Ind: make(chan IndMessage), id: id, addresses: addresses, queues: make([]chan string, len(addresses)), last: make([]uint64, len(addresses)), listener: l}
	for i := range addresses {
		if i != id {
			p.queues[i] = make(chan string, 4096)
			go p.sender(i)
		}
	}
	go p.dispatch()
	go p.accept()
	return p, nil
}

func (p *PP2PLink) dispatch() {
	for msg := range p.Req {
		if msg.To != p.id && msg.To >= 0 && msg.To < len(p.queues) {
			p.queues[msg.To] <- msg.Message
		}
	}
}

func (p *PP2PLink) sender(dest int) {
	var conn net.Conn
	var seq uint64
	for msg := range p.queues[dest] {
		pkt := packet{From: p.id, Seq: seq + 1, Message: msg}
		for {
			if conn == nil {
				var err error
				conn, err = net.DialTimeout("tcp", p.addresses[dest], time.Second)
				if err != nil {
					time.Sleep(30 * time.Millisecond)
					continue
				}
			}
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			var a ack
			if err := writeFrame(conn, pkt); err == nil {
				err = readFrame(conn, &a)
				if err == nil && a.Seq == pkt.Seq {
					seq++
					break
				}
			}
			_ = conn.Close()
			conn = nil
			time.Sleep(30 * time.Millisecond)
		}
	}
}

func (p *PP2PLink) accept() {
	for {
		c, err := p.listener.Accept()
		if err != nil {
			log.Printf("PL accept: %v", err)
			return
		}
		go p.receive(c)
	}
}

func (p *PP2PLink) receive(c net.Conn) {
	defer c.Close()
	for {
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		var m packet
		if err := readFrame(c, &m); err != nil {
			return
		}
		if m.From < 0 || m.From >= len(p.addresses) || m.From == p.id {
			return
		}
		p.mu.Lock()
		if m.Seq == p.last[m.From]+1 {
			p.Ind <- IndMessage{From: m.From, Message: m.Message}
			p.last[m.From] = m.Seq
		} else if m.Seq != p.last[m.From] {
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
		if err := writeFrame(c, ack{Seq: m.Seq}); err != nil {
			return
		}
	}
}

func writeFrame(w io.Writer, v interface{}) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if len(b) > 65536 {
		return errors.New("frame too large")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(b)))
	if e = writeAll(w, header[:]); e != nil {
		return e
	}
	return writeAll(w, b)
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, e := w.Write(b)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func readFrame(r io.Reader, v interface{}) error {
	var header [4]byte
	if _, e := io.ReadFull(r, header[:]); e != nil {
		return e
	}
	n := binary.BigEndian.Uint32(header[:])
	if n == 0 || n > 65536 {
		return errors.New("invalid frame size")
	}
	b := make([]byte, n)
	if _, e := io.ReadFull(r, b); e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}
