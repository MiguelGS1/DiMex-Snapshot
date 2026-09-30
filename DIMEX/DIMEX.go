// Package DIMEX implementa exclusão mútua por Ricart–Agrawala e snapshots de Chandy–Lamport.
// Um único laço processa pedidos locais, mensagens recebidas e marcadores.
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

type requestKind int

const (
	ENTER requestKind = iota
	EXIT
)

type accessGranted struct{}

// Os campos de Message, LocalState e Snapshot fazem parte do JSON dos snapshots.
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
	Req              chan requestKind
	Ind              chan accessGranted
	SnapshotReq      chan int
	Pp2plink         *PP2PLink.PP2PLink
	processID        int
	processCount     int
	state            State
	logicalClock     int
	requestTimestamp int
	repliesReceived  []bool
	deferredRequests []int
	activeSnapshots  map[int]*Snapshot
	snapshotFile     *os.File
	faultMode        string
}

func NewDIMEX(addresses []string, processID int, snapshotDir, faultMode string) (*DIMEX_Module, error) {
	if faultMode != "" && faultMode != "unsafe" && faultMode != "block" {
		return nil, fmt.Errorf("unknown fault %q", faultMode)
	}
	if processID < 0 || processID >= len(addresses) {
		return nil, fmt.Errorf("invalid ID %d", processID)
	}
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, err
	}
	snapshotPath := filepath.Join(snapshotDir, fmt.Sprintf("process_%d.jsonl", processID))
	snapshotFile, err := os.OpenFile(snapshotPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	link, err := PP2PLink.NewPP2PLink(addresses, processID)
	if err != nil {
		snapshotFile.Close()
		return nil, err
	}
	module := &DIMEX_Module{
		Req:              make(chan requestKind, 1),
		Ind:              make(chan accessGranted, 1),
		SnapshotReq:      make(chan int, 1024),
		Pp2plink:         link,
		processID:        processID,
		processCount:     len(addresses),
		state:            NoMX,
		repliesReceived:  make([]bool, len(addresses)),
		deferredRequests: make([]int, len(addresses)),
		activeSnapshots:  make(map[int]*Snapshot),
		snapshotFile:     snapshotFile,
		faultMode:        faultMode,
	}
	go module.run()
	return module, nil
}

func (module *DIMEX_Module) send(destinationID int, message Message) {
	message.Clock = module.logicalClock
	encoded, _ := json.Marshal(message)
	module.Pp2plink.Req <- PP2PLink.ReqMessage{To: destinationID, Message: string(encoded)}
}

// O menor par (timestamp lógico, ID) tem prioridade.
func before(firstID, firstTimestamp, secondID, secondTimestamp int) bool {
	return firstTimestamp < secondTimestamp || (firstTimestamp == secondTimestamp && firstID < secondID)
}

func (module *DIMEX_Module) reply(destinationID, requestTimestamp int) {
	module.logicalClock++
	module.send(destinationID, Message{Type: "RESP_OK", From: module.processID, ReqTS: requestTimestamp})
}

func (module *DIMEX_Module) run() {
	for {
		select {
		case request := <-module.Req:
			if request == ENTER {
				module.entry()
			} else {
				module.exit()
			}
		case snapshotID := <-module.SnapshotReq:
			if snapshotID > 0 {
				module.startSnapshot(snapshotID, -1)
			}
		case delivery := <-module.Pp2plink.Ind:
			var message Message
			if err := json.Unmarshal([]byte(delivery.Message), &message); err != nil || message.From != delivery.From {
				continue
			}
			if message.Type == "MARKER" {
				module.marker(message.SnapshotID, delivery.From)
				continue
			}
			// Até chegar o marcador desse remetente, a mensagem pertence ao canal do snapshot.
			for _, snapshot := range module.activeSnapshots {
				if !snapshot.closed[delivery.From] {
					snapshot.Channels[delivery.From] = append(snapshot.Channels[delivery.From], message)
				}
			}
			if message.Type == "REQ_ENTRY" {
				module.onRequest(message)
			} else if message.Type == "RESP_OK" {
				module.onReply(message)
			}
		}
	}
}

func (module *DIMEX_Module) entry() {
	if module.state != NoMX {
		panic("ENTRY outside noMX")
	}
	module.logicalClock++
	module.requestTimestamp = module.logicalClock
	module.state = WantMX
	for peerID := range module.repliesReceived {
		module.repliesReceived[peerID] = false
	}
	for peerID := 0; peerID < module.processCount; peerID++ {
		if peerID != module.processID {
			module.send(peerID, Message{Type: "REQ_ENTRY", From: module.processID, ReqTS: module.requestTimestamp})
		}
	}
	// Sem outros processos, não há respostas a aguardar.
	if module.processCount == 1 {
		module.state = InMX
		module.Ind <- accessGranted{}
	}
}

func (module *DIMEX_Module) exit() {
	if module.state != InMX {
		panic("EXIT outside inMX")
	}
	module.state = NoMX
	for peerID, timestamp := range module.deferredRequests {
		if timestamp != 0 {
			module.deferredRequests[peerID] = 0
			module.reply(peerID, timestamp)
		}
	}
}

func (module *DIMEX_Module) onRequest(message Message) {
	if message.ReqTS <= 0 || message.From < 0 || message.From >= module.processCount || message.From == module.processID {
		return
	}
	if module.logicalClock < message.ReqTS {
		module.logicalClock = message.ReqTS
	}
	module.logicalClock++
	deferReply := module.state == InMX ||
		(module.state == WantMX && before(module.processID, module.requestTimestamp, message.From, message.ReqTS))
	// As falhas mudam apenas a decisão de responder, para testar as invariantes.
	if module.faultMode == "unsafe" {
		deferReply = false
	}
	if module.faultMode == "block" {
		deferReply = true
	}
	if deferReply {
		module.deferredRequests[message.From] = message.ReqTS
	} else {
		module.reply(message.From, message.ReqTS)
	}
}

func (module *DIMEX_Module) onReply(message Message) {
	if module.logicalClock < message.Clock {
		module.logicalClock = message.Clock
	}
	module.logicalClock++
	if module.state != WantMX || message.ReqTS != module.requestTimestamp ||
		message.From == module.processID || module.repliesReceived[message.From] {
		return
	}
	module.repliesReceived[message.From] = true
	for peerID := 0; peerID < module.processCount; peerID++ {
		if peerID != module.processID && !module.repliesReceived[peerID] {
			return
		}
	}
	// Ind só libera a aplicação após a resposta de todos os outros processos.
	module.state = InMX
	module.Ind <- accessGranted{}
}

func (module *DIMEX_Module) startSnapshot(snapshotID, firstMarkerFrom int) {
	if _, exists := module.activeSnapshots[snapshotID]; exists {
		return
	}
	snapshot := &Snapshot{
		SnapshotID: snapshotID,
		ProcessID:  module.processID,
		Local: LocalState{
			State:     module.state,
			Clock:     module.logicalClock,
			RequestTS: module.requestTimestamp,
			Responses: append([]bool(nil), module.repliesReceived...),
			Waiting:   append([]int(nil), module.deferredRequests...),
		},
		Channels: make(map[int][]Message),
		closed:   make(map[int]bool),
	}
	module.activeSnapshots[snapshotID] = snapshot
	for peerID := 0; peerID < module.processCount; peerID++ {
		if peerID != module.processID {
			snapshot.Channels[peerID] = []Message{}
			snapshot.closed[peerID] = peerID == firstMarkerFrom
		}
	}
	// Os marcadores usam a mesma fila FIFO das mensagens normais para cada destino.
	for peerID := 0; peerID < module.processCount; peerID++ {
		if peerID != module.processID {
			module.send(peerID, Message{Type: "MARKER", From: module.processID, SnapshotID: snapshotID})
		}
	}
	module.finishIfComplete(snapshotID)
}

func (module *DIMEX_Module) marker(snapshotID, senderID int) {
	if snapshotID <= 0 || senderID == module.processID {
		return
	}
	if _, exists := module.activeSnapshots[snapshotID]; !exists {
		// O primeiro marcador registra o estado local e fecha o canal de origem.
		module.startSnapshot(snapshotID, senderID)
		return
	}
	module.activeSnapshots[snapshotID].closed[senderID] = true
	module.finishIfComplete(snapshotID)
}

func (module *DIMEX_Module) finishIfComplete(snapshotID int) {
	snapshot := module.activeSnapshots[snapshotID]
	for peerID := 0; peerID < module.processCount; peerID++ {
		if peerID != module.processID && !snapshot.closed[peerID] {
			return
		}
	}
	encoded, err := json.Marshal(snapshot)
	if err == nil {
		encoded = append(encoded, '\n')
		_, err = module.snapshotFile.Write(encoded)
	}
	if err != nil {
		panic(err)
	}
	delete(module.activeSnapshots, snapshotID)
}
