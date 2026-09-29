// Package DIMEX implements Ricart-Agrawala and Chandy-Lamport snapshots.
// A single event loop serializes local events and received messages.
package DIMEX

import (
	"SD/PP2PLink"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type State string

const (
	NoMX   State = "noMX"
	WantMX State = "wantMX"
	InMX   State = "inMX"
)

type dmxReq int

const (
	ENTER dmxReq = iota
	EXIT
)

type dmxResp struct{}
type Message struct {
	Type       string
	From       int
	ReqTS      int
	SnapshotID int
	Clock      int
}
type LocalState struct {
	State     State
	Clock     int
	RequestTS int
	Responses []bool
	Waiting   []int
}
type Snapshot struct {
	SnapshotID int
	ProcessID  int
	Local      LocalState
	Channels   map[int][]Message
	closed     map[int]bool
}
type DIMEX_Module struct {
	Req         chan dmxReq
	Ind         chan dmxResp
	SnapshotReq chan int
	Pp2plink    *PP2PLink.PP2PLink
	id, n       int
	st          State
	lcl, reqTs  int
	responses   []bool
	waiting     []int
	active      map[int]*Snapshot
	file        *os.File
	fault       string
}

func NewDIMEX(addresses []string, id int, snapshotDir, fault string) (*DIMEX_Module, error) {
	if fault != "" && fault != "unsafe" && fault != "block" {
		return nil, fmt.Errorf("unknown fault %q", fault)
	}
	if id < 0 || id >= len(addresses) {
		return nil, fmt.Errorf("invalid ID %d", id)
	}
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(snapshotDir, fmt.Sprintf("process_%d.jsonl", id))
	f, e := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if e != nil {
		return nil, e
	}
	pl, e := PP2PLink.NewPP2PLink(addresses, id)
	if e != nil {
		f.Close()
		return nil, e
	}
	m := &DIMEX_Module{
		Req:         make(chan dmxReq, 1),
		Ind:         make(chan dmxResp, 1),
		SnapshotReq: make(chan int, 1024),
		Pp2plink:    pl,
		id:          id,
		n:           len(addresses),
		st:          NoMX,
		responses:   make([]bool, len(addresses)),
		waiting:     make([]int, len(addresses)),
		active:      make(map[int]*Snapshot),
		file:        f,
		fault:       fault,
	}
	go m.run()
	return m, nil
}

func (m *DIMEX_Module) send(to int, msg Message) {
	msg.Clock = m.lcl
	b, _ := json.Marshal(msg)
	m.Pp2plink.Req <- PP2PLink.ReqMessage{To: to, Message: string(b)}
}
func before(id1, ts1, id2, ts2 int) bool { return ts1 < ts2 || (ts1 == ts2 && id1 < id2) }
func (m *DIMEX_Module) reply(to, ts int) {
	m.lcl++
	m.send(to, Message{Type: "RESP_OK", From: m.id, ReqTS: ts})
}

func (m *DIMEX_Module) run() {
	for {
		select {
		case req := <-m.Req:
			if req == ENTER {
				m.entry()
			} else {
				m.exit()
			}
		case id := <-m.SnapshotReq:
			if id > 0 {
				m.startSnapshot(id, -1)
			}
		case ind := <-m.Pp2plink.Ind:
			var msg Message
			if err := json.Unmarshal([]byte(ind.Message), &msg); err != nil || msg.From != ind.From {
				continue
			}
			if msg.Type == "MARKER" {
				m.marker(msg.SnapshotID, ind.From)
				continue
			}
			for _, s := range m.active {
				if !s.closed[ind.From] {
					s.Channels[ind.From] = append(s.Channels[ind.From], msg)
				}
			}
			if msg.Type == "REQ_ENTRY" {
				m.onRequest(msg)
			} else if msg.Type == "RESP_OK" {
				m.onReply(msg)
			}
		}
	}
}

func (m *DIMEX_Module) entry() {
	if m.st != NoMX {
		panic("ENTRY outside noMX")
	}
	m.lcl++
	m.reqTs = m.lcl
	m.st = WantMX
	for i := range m.responses {
		m.responses[i] = false
	}
	for i := 0; i < m.n; i++ {
		if i != m.id {
			m.send(i, Message{Type: "REQ_ENTRY", From: m.id, ReqTS: m.reqTs})
		}
	}
	if m.n == 1 {
		m.st = InMX
		m.Ind <- dmxResp{}
	}
}
func (m *DIMEX_Module) exit() {
	if m.st != InMX {
		panic("EXIT outside inMX")
	}
	m.st = NoMX
	for i, ts := range m.waiting {
		if ts != 0 {
			m.waiting[i] = 0
			m.reply(i, ts)
		}
	}
}
func (m *DIMEX_Module) onRequest(msg Message) {
	if msg.ReqTS <= 0 || msg.From < 0 || msg.From >= m.n || msg.From == m.id {
		return
	}
	if m.lcl < msg.ReqTS {
		m.lcl = msg.ReqTS
	}
	m.lcl++
	deferReply := m.st == InMX || (m.st == WantMX && before(m.id, m.reqTs, msg.From, msg.ReqTS))
	if m.fault == "unsafe" {
		deferReply = false
	}
	if m.fault == "block" {
		deferReply = true
	}
	if deferReply {
		m.waiting[msg.From] = msg.ReqTS
	} else {
		m.reply(msg.From, msg.ReqTS)
	}
}
func (m *DIMEX_Module) onReply(msg Message) {
	if m.lcl < msg.Clock {
		m.lcl = msg.Clock
	}
	m.lcl++
	if m.st != WantMX || msg.ReqTS != m.reqTs || msg.From == m.id || m.responses[msg.From] {
		return
	}
	m.responses[msg.From] = true
	for i := 0; i < m.n; i++ {
		if i != m.id && !m.responses[i] {
			return
		}
	}
	m.st = InMX
	m.Ind <- dmxResp{}
}
func (m *DIMEX_Module) startSnapshot(id, firstFrom int) {
	if _, exists := m.active[id]; exists {
		return
	}
	s := &Snapshot{
		SnapshotID: id,
		ProcessID:  m.id,
		Local: LocalState{
			State:     m.st,
			Clock:     m.lcl,
			RequestTS: m.reqTs,
			Responses: append([]bool(nil), m.responses...),
			Waiting:   append([]int(nil), m.waiting...),
		},
		Channels: make(map[int][]Message),
		closed:   make(map[int]bool),
	}
	m.active[id] = s
	for i := 0; i < m.n; i++ {
		if i != m.id {
			s.Channels[i] = []Message{}
			s.closed[i] = i == firstFrom
		}
	}
	for i := 0; i < m.n; i++ {
		if i != m.id {
			m.send(i, Message{Type: "MARKER", From: m.id, SnapshotID: id})
		}
	}
	m.finishIfComplete(id)
}
func (m *DIMEX_Module) marker(id, from int) {
	if id <= 0 || from == m.id {
		return
	}
	if _, ok := m.active[id]; !ok {
		m.startSnapshot(id, from)
		return
	}
	m.active[id].closed[from] = true
	m.finishIfComplete(id)
}
func (m *DIMEX_Module) finishIfComplete(id int) {
	s := m.active[id]
	for i := 0; i < m.n; i++ {
		if i != m.id && !s.closed[i] {
			return
		}
	}
	b, e := json.Marshal(s)
	if e == nil {
		b = append(b, '\n')
		_, e = m.file.Write(b)
	}
	if e != nil {
		panic(e)
	}
	delete(m.active, id)
}
