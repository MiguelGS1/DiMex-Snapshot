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

// ReqMessage é o pedido de envio feito pelo módulo DiMEx.
// To é o ID do destinatário, não o endereço de rede.
type ReqMessage struct {
	To      int
	Message string
}

// IndMessage informa ao DiMEx que uma mensagem chegou.
// From identifica o processo que a enviou.
type IndMessage struct {
	From    int
	Message string
}

// packet é o conteúdo transmitido pela conexão TCP.
// Seq identifica a posição da mensagem na sequência enviada a um destino.
// Os nomes dos campos também definem as chaves do JSON transmitido.
type packet struct {
	From    int
	Seq     uint64
	Message string
}

// O destinatário confirma qual número de sequência recebeu.
type ack struct{ Seq uint64 }

type PP2PLink struct {
	Req       chan ReqMessage
	Ind       chan IndMessage
	processID int
	addresses []string

	// Uma fila por destino impede que duas goroutines enviem mensagens
	// simultaneamente ao mesmo processo e alterem sua ordem.
	outboundQueues []chan string

	// Protege a conferência da sequência quando há conexões simultâneas.
	receiveMu        sync.Mutex
	lastDeliveredSeq []uint64
	listener         net.Listener
}

func NewPP2PLink(addresses []string, processID int) (*PP2PLink, error) {
	if processID < 0 || processID >= len(addresses) || len(addresses) == 0 {
		return nil, fmt.Errorf("invalid process ID %d", processID)
	}

	// Cada processo escuta no endereço que ocupa a posição do seu ID.
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

	// Cada outro processo recebe sua própria fila e sua própria goroutine
	// de envio. Assim, as mensagens para aquele destino saem uma de cada vez.
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

// Retira os pedidos de Req e coloca cada mensagem na fila do destinatário.
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

	// Só avança para a próxima mensagem após confirmar a atual.
	for payload := range link.outboundQueues[destinationID] {
		outgoing := packet{
			From:    link.processID,
			Seq:     lastConfirmedSeq + 1,
			Message: payload,
		}

		for {
			// Tenta conectar novamente caso o destinatário ainda não esteja
			// disponível ou uma conexão anterior tenha falhado.
			if connection == nil {
				var err error
				connection, err = net.DialTimeout("tcp", link.addresses[destinationID], time.Second)
				if err != nil {
					time.Sleep(30 * time.Millisecond)
					continue
				}
			}

			// Limita o tempo gasto no envio e na espera pela confirmação.
			_ = connection.SetDeadline(time.Now().Add(3 * time.Second))

			var confirmation ack
			if err := writeFrame(connection, outgoing); err == nil {
				err = readFrame(connection, &confirmation)
				if err == nil && confirmation.Seq == outgoing.Seq {
					lastConfirmedSeq++
					break
				}
			}

			// Se a conexão ou o ACK falhar, a mensagem mantém o mesmo Seq.
			// O receptor reconhecerá uma possível duplicata pelo número.
			_ = connection.Close()
			connection = nil
			time.Sleep(30 * time.Millisecond)
		}
	}
}

// Aceita conexões TCP e cria uma goroutine para tratar cada uma delas.
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

		// A conferência e a entrega ficam protegidas pelo mesmo mutex.
		// Isso evita que duas conexões entreguem a mesma sequência ao DiMEx.
		link.receiveMu.Lock()
		lastSequence := link.lastDeliveredSeq[incoming.From]

		if incoming.Seq == lastSequence+1 {
			// Chegou a próxima mensagem esperada desse remetente.
			link.Ind <- IndMessage{From: incoming.From, Message: incoming.Message}
			link.lastDeliveredSeq[incoming.From] = incoming.Seq
		} else if incoming.Seq != lastSequence {
			// Um número à frente do esperado quebraria a ordem FIFO.
			link.receiveMu.Unlock()
			return
		}

		// Se Seq for igual ao último entregue, trata-se de retransmissão:
		// confirma novamente, mas não entrega a mensagem duas vezes.
		link.receiveMu.Unlock()
		if err := writeFrame(connection, ack{Seq: incoming.Seq}); err != nil {
			return
		}
	}
}

// TCP transporta bytes continuamente, sem separar mensagens.
// O cabeçalho de quatro bytes informa o tamanho do JSON que vem a seguir.
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

// Uma chamada a Write pode escrever só parte dos bytes.
// A função continua até enviar todo o cabeçalho ou conteúdo.
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

	// Lê o cabeçalho inteiro antes de descobrir o tamanho da mensagem.
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	frameSize := binary.BigEndian.Uint32(header[:])
	if frameSize == 0 || frameSize > 65536 {
		return errors.New("invalid frame size")
	}

	// Lê exatamente a quantidade de bytes anunciada no cabeçalho.
	encoded := make([]byte, frameSize)
	if _, err := io.ReadFull(reader, encoded); err != nil {
		return err
	}
	return json.Unmarshal(encoded, destination)
}
