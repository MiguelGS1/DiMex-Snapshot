// Package DIMEX implementa exclusão mútua por Ricart–Agrawala
// e snapshots distribuídos por Chandy–Lamport.
package DIMEX

import (
	"SD/PP2PLink"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Os três estados possíveis em relação à seção crítica.
type State string

const (
	NoMX   State = "noMX"   // Não está solicitando acesso.
	WantMX State = "wantMX" // Solicitou acesso e aguarda respostas.
	InMX   State = "inMX"   // Recebeu as respostas e está na seção crítica.
)

// A aplicação envia ENTER para pedir acesso e EXIT para liberá-lo.
type requestKind int

const (
	ENTER requestKind = iota
	EXIT
)

// Sinal enviado à aplicação quando ela pode entrar na seção crítica.
type accessGranted struct{}

// Message é usada para pedidos, respostas e marcadores de snapshot.
// ReqTS identifica o pedido; Clock carrega o relógio lógico do remetente.
type Message struct {
	Type       string
	From       int
	ReqTS      int
	SnapshotID int
	Clock      int
}

// Estado que o processo grava no momento do snapshot.
// Responses indica de quem já recebeu resposta; Waiting guarda o timestamp
// dos pedidos cuja resposta foi adiada (zero significa que não há pedido).
type LocalState struct {
	State     State
	Clock     int
	RequestTS int
	Responses []bool
	Waiting   []int
}

// Um registro de snapshot contém o estado local e as mensagens observadas
// nos canais de entrada. closed é usado durante a coleta e não vai para o JSON.
type Snapshot struct {
	SnapshotID int
	ProcessID  int
	Local      LocalState
	Channels   map[int][]Message
	closed     map[int]bool
}

type DIMEX_Module struct {
	Req         chan requestKind   // Pedidos ENTER e EXIT da aplicação.
	Ind         chan accessGranted // Autorização para a aplicação entrar.
	SnapshotReq chan int           // IDs de snapshots iniciados localmente.
	Pp2plink    *PP2PLink.PP2PLink

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

	// Cada processo grava seus estados em um arquivo JSONL separado.
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, err
	}
	snapshotPath := filepath.Join(snapshotDir, fmt.Sprintf("process_%d.jsonl", processID))
	snapshotFile, err := os.OpenFile(snapshotPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// O PL abre a porta deste processo e faz a comunicação com os demais.
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

	// Um único laço altera o estado do DiMEx e dos snapshots.
	go module.run()
	return module, nil
}

// Acrescenta o relógio lógico à mensagem e entrega o envio ao PP2PLink.
func (module *DIMEX_Module) send(destinationID int, message Message) {
	message.Clock = module.logicalClock
	encoded, _ := json.Marshal(message)
	module.Pp2plink.Req <- PP2PLink.ReqMessage{To: destinationID, Message: string(encoded)}
}

// O pedido com menor (timestamp lógico, ID) tem prioridade.
// O ID resolve o empate quando dois pedidos têm o mesmo timestamp.
func before(firstID, firstTimestamp, secondID, secondTimestamp int) bool {
	return firstTimestamp < secondTimestamp ||
		(firstTimestamp == secondTimestamp && firstID < secondID)
}

// Envia uma resposta para o pedido identificado por requestTimestamp.
func (module *DIMEX_Module) reply(destinationID, requestTimestamp int) {
	module.logicalClock++
	module.send(destinationID, Message{
		Type:  "RESP_OK",
		From:  module.processID,
		ReqTS: requestTimestamp,
	})
}

func (module *DIMEX_Module) run() {
	for {
		// Cada evento é tratado até o fim antes de começar o próximo.
		// Isso evita alterações simultâneas no estado interno do módulo.
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
			if err := json.Unmarshal([]byte(delivery.Message), &message); err != nil ||
				message.From != delivery.From {
				continue
			}

			// Marcadores controlam o snapshot; não são pedidos de acesso.
			if message.Type == "MARKER" {
				module.marker(message.SnapshotID, delivery.From)
				continue
			}

			// Um canal registra mensagens recebidas depois de salvar o estado
			// local e antes de receber o marcador daquele remetente.
			for _, snapshot := range module.activeSnapshots {
				if !snapshot.closed[delivery.From] {
					snapshot.Channels[delivery.From] = append(
						snapshot.Channels[delivery.From], message,
					)
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

// Trata o pedido da aplicação para entrar na seção crítica.
func (module *DIMEX_Module) entry() {
	if module.state != NoMX {
		panic("ENTRY outside noMX")
	}

	// Este timestamp identifica o pedido até a entrada ser autorizada.
	module.logicalClock++
	module.requestTimestamp = module.logicalClock
	module.state = WantMX

	// As respostas de um acesso anterior não valem para o pedido atual.
	for peerID := range module.repliesReceived {
		module.repliesReceived[peerID] = false
	}

	// Solicita permissão a todos os outros processos.
	for peerID := 0; peerID < module.processCount; peerID++ {
		if peerID != module.processID {
			module.send(peerID, Message{
				Type:  "REQ_ENTRY",
				From:  module.processID,
				ReqTS: module.requestTimestamp,
			})
		}
	}

	// Com apenas um processo, não há respostas a aguardar.
	if module.processCount == 1 {
		module.state = InMX
		module.Ind <- accessGranted{}
	}
}

// Libera a seção crítica e responde aos pedidos que ficaram adiados.
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

// Decide se responde agora ao pedido recebido ou se guarda a resposta.
func (module *DIMEX_Module) onRequest(message Message) {
	if message.ReqTS <= 0 || message.From < 0 ||
		message.From >= module.processCount || message.From == module.processID {
		return
	}

	// Ao receber o pedido, o relógio local avança a partir do timestamp
	// recebido, caso ele seja maior que o valor atual.
	if module.logicalClock < message.ReqTS {
		module.logicalClock = message.ReqTS
	}
	module.logicalClock++

	// Quem está em InMX adia. Quem também quer entrar só adia quando
	// o próprio pedido tem prioridade sobre o pedido recebido.
	deferReply := module.state == InMX ||
		(module.state == WantMX &&
			before(module.processID, module.requestTimestamp, message.From, message.ReqTS))

	// As falhas alteram somente a decisão de responder:
	// unsafe responde mesmo quando deveria adiar;
	// block adia mesmo quando deveria responder.
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

// Registra a resposta de outro processo ao pedido atual.
func (module *DIMEX_Module) onReply(message Message) {
	if module.logicalClock < message.Clock {
		module.logicalClock = message.Clock
	}
	module.logicalClock++

	// Resposta de outro pedido ou já recebida não conta novamente.
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

	// Somente após todas as respostas o DiMEx libera a aplicação.
	module.state = InMX
	module.Ind <- accessGranted{}
}

// Salva o estado local e inicia a coleta dos canais de entrada.
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

			// Copia os vetores para que eventos posteriores não alterem
			// o estado local já registrado neste snapshot.
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

			// Se o snapshot começou ao receber um marcador, o canal
			// daquele remetente já está fechado para esta coleta.
			snapshot.closed[peerID] = peerID == firstMarkerFrom
		}
	}

	// Marcadores percorrem as mesmas filas FIFO das mensagens normais.
	// Assim, cada destinatário observa as mensagens anteriores ao
	// marcador antes de fechar o canal correspondente.
	for peerID := 0; peerID < module.processCount; peerID++ {
		if peerID != module.processID {
			module.send(peerID, Message{
				Type:       "MARKER",
				From:       module.processID,
				SnapshotID: snapshotID,
			})
		}
	}

	module.finishIfComplete(snapshotID)
}

// Trata o marcador recebido de um canal de entrada.
func (module *DIMEX_Module) marker(snapshotID, senderID int) {
	if snapshotID <= 0 || senderID == module.processID {
		return
	}

	if _, exists := module.activeSnapshots[snapshotID]; !exists {
		// O primeiro marcador salva o estado local e inicia o snapshot.
		module.startSnapshot(snapshotID, senderID)
		return
	}

	// Um marcador posterior encerra a gravação daquele canal.
	module.activeSnapshots[snapshotID].closed[senderID] = true
	module.finishIfComplete(snapshotID)
}

// Grava o estado deste processo quando todos os canais de entrada fecharam.
func (module *DIMEX_Module) finishIfComplete(snapshotID int) {
	snapshot := module.activeSnapshots[snapshotID]
	for peerID := 0; peerID < module.processCount; peerID++ {
		if peerID != module.processID && !snapshot.closed[peerID] {
			return
		}
	}

	// JSONL usa uma linha por processo e por ID de snapshot.
	// O verificador reúne as linhas dos processos pelo SnapshotID.
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
