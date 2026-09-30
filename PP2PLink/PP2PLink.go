// Package PP2PLink entrega mensagens em ordem FIFO entre processos via TCP.
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

// Os campos do pacote também definem o formato JSON enviado pela conexão.
type packet struct {
	From    int
	Seq     uint64
	Message string
}
type ack struct{ Seq uint64 }

type PP2PLink struct {
	Req              chan ReqMessage
	Ind              chan IndMessage
	processID        int
	addresses        []string
	outboundQueues   []chan string
	receiveMu        sync.Mutex
	lastDeliveredSeq []uint64
	listener         net.Listener
}

func NewPP2PLink(addresses []string, processID int) (*PP2PLink, error) {
	if processID < 0 || processID >= len(addresses) || len(addresses) == 0 {
		return nil, fmt.Errorf("invalid process ID %d", processID)
	}
	listener, err := net.Listen("tcp", addresses[processID])
	if err != nil {
		return nil, err
	}
	link := &PP2PLink{
		Req:              make(chan ReqMessage, 4096),
		Ind:              make(chan IndMessage),
		processID:        processID,
		addresses:        addresses,
		outboundQueues:   make([]chan string, len(addresses)),
		lastDeliveredSeq: make([]uint64, len(addresses)),
		listener:         listener,
	}
	for peerID := range addresses {
		if peerID != processID {
			link.outboundQueues[peerID] = make(chan string, 4096)
			go link.sender(peerID)
		}
	}
	go link.dispatch()
	go link.accept()
	return link, nil
}

// Cada destino tem uma fila e uma goroutine de envio, mantendo a ordem das mensagens.
func (link *PP2PLink) dispatch() {
	for request := range link.Req {
		if request.To != link.processID && request.To >= 0 && request.To < len(link.outboundQueues) {
			link.outboundQueues[request.To] <- request.Message
		}
	}
}

func (link *PP2PLink) sender(destinationID int) {
	var connection net.Conn
	var lastConfirmedSeq uint64
	for payload := range link.outboundQueues[destinationID] {
		outgoing := packet{From: link.processID, Seq: lastConfirmedSeq + 1, Message: payload}
		for {
			if connection == nil {
				var err error
				connection, err = net.DialTimeout("tcp", link.addresses[destinationID], time.Second)
				if err != nil {
					time.Sleep(30 * time.Millisecond)
					continue
				}
			}
			_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
			var confirmation ack
			if err := writeFrame(connection, outgoing); err == nil {
				err = readFrame(connection, &confirmation)
				if err == nil && confirmation.Seq == outgoing.Seq {
					lastConfirmedSeq++
					break
				}
			}
			// A mesma sequência é reenviada se a conexão ou a confirmação falhar.
			_ = connection.Close()
			connection = nil
			time.Sleep(30 * time.Millisecond)
		}
	}
}

func (link *PP2PLink) accept() {
	for {
		connection, err := link.listener.Accept()
		if err != nil {
			log.Printf("PL accept: %v", err)
			return
		}
		go link.receive(connection)
	}
}

func (link *PP2PLink) receive(connection net.Conn) {
	defer connection.Close()
	for {
		_ = connection.SetDeadline(time.Now().Add(10 * time.Second))
		var incoming packet
		if err := readFrame(connection, &incoming); err != nil {
			return
		}
		if incoming.From < 0 || incoming.From >= len(link.addresses) || incoming.From == link.processID {
			return
		}
		link.receiveMu.Lock()
		lastSequence := link.lastDeliveredSeq[incoming.From]
		if incoming.Seq == lastSequence+1 {
			link.Ind <- IndMessage{From: incoming.From, Message: incoming.Message}
			link.lastDeliveredSeq[incoming.From] = incoming.Seq
		} else if incoming.Seq != lastSequence {
			link.receiveMu.Unlock()
			return
		}
		// Uma retransmissão já entregue recebe ACK, mas não chega duas vezes ao DiMEx.
		link.receiveMu.Unlock()
		if err := writeFrame(connection, ack{Seq: incoming.Seq}); err != nil {
			return
		}
	}
}

// O tamanho no cabeçalho permite ler exatamente um JSON por mensagem TCP.
func writeFrame(writer io.Writer, value interface{}) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(encoded) > 65536 {
		return errors.New("frame too large")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(encoded)))
	if err = writeAll(writer, header[:]); err != nil {
		return err
	}
	return writeAll(writer, encoded)
}

func writeAll(writer io.Writer, remaining []byte) error {
	for len(remaining) > 0 {
		written, err := writer.Write(remaining)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		remaining = remaining[written:]
	}
	return nil
}

func readFrame(reader io.Reader, destination interface{}) error {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	frameSize := binary.BigEndian.Uint32(header[:])
	if frameSize == 0 || frameSize > 65536 {
		return errors.New("invalid frame size")
	}
	encoded := make([]byte, frameSize)
	if _, err := io.ReadFull(reader, encoded); err != nil {
		return err
	}
	return json.Unmarshal(encoded, destination)
}
