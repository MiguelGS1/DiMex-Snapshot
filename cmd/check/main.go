package main

import (
	"SD/DIMEX"
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Um snapshot global reúne os estados dos processos com o mesmo SnapshotID.
// A chave do mapa é o ID do processo.
type globalSnapshot map[int]DIMEX.Snapshot

// Guarda uma falha encontrada, o snapshot em que ocorreu e sua explicação.
type violation struct {
	SnapshotID int
	Name       string
	Detail     string
}

// Cada função de análise devolve as razões de uma possível violação.
type invariantReasons func(globalSnapshot) []string

func main() {
	// Os caminhos e a quantidade esperada de processos vêm dos argumentos.
	snapshotDir := flag.String("snapshots", "snapshots", "per-process JSONL directory")
	outputPath := flag.String("file", "mxOUT.txt", "shared output file")
	processCount := flag.Int("n", 3, "number of processes")
	minSnapshots := flag.Int("min", 1, "minimum complete snapshots required")
	flag.Parse()

	if *processCount < 1 || *minSnapshots < 0 {
		fail("invalid n or min")
	}

	// Agrupa os registros dos arquivos process_0.jsonl, process_1.jsonl...
	// Os registros com o mesmo SnapshotID formarão um snapshot global.
	snapshotsByID := make(map[int]globalSnapshot)
	for processID := 0; processID < *processCount; processID++ {
		snapshotPath := filepath.Join(*snapshotDir, fmt.Sprintf("process_%d.jsonl", processID))
		snapshotFile, err := os.Open(snapshotPath)
		if err != nil {
			fail(err.Error())
		}

		// Cada linha do arquivo contém um estado local em JSON.
		// O limite maior permite ler registros com mensagens nos canais.
		scanner := bufio.NewScanner(snapshotFile)
		scanner.Buffer(make([]byte, 4096), 10*1024*1024)

		for scanner.Scan() {
			var snapshot DIMEX.Snapshot
			if err := json.Unmarshal(scanner.Bytes(), &snapshot); err != nil {
				fail(fmt.Sprintf("%s: %v", snapshotPath, err))
			}

			// O ID registrado deve corresponder ao arquivo lido.
			if snapshot.ProcessID != processID || snapshot.SnapshotID <= 0 {
				fail(fmt.Sprintf("%s: invalid snapshot ID or process ID", snapshotPath))
			}

			// Há uma posição por processo nos vetores e um canal de entrada
			// para cada outro processo.
			if len(snapshot.Local.Responses) != *processCount ||
				len(snapshot.Local.Waiting) != *processCount ||
				len(snapshot.Channels) != *processCount-1 {
				fail(fmt.Sprintf("%s: malformed arrays or channels in snapshot %d",
					snapshotPath, snapshot.SnapshotID))
			}

			if snapshotsByID[snapshot.SnapshotID] == nil {
				snapshotsByID[snapshot.SnapshotID] = make(globalSnapshot)
			}
			if _, duplicate := snapshotsByID[snapshot.SnapshotID][processID]; duplicate {
				fail(fmt.Sprintf("%s: repeated snapshot %d", snapshotPath, snapshot.SnapshotID))
			}
			snapshotsByID[snapshot.SnapshotID][processID] = snapshot
		}

		if err := scanner.Err(); err != nil {
			fail(err.Error())
		}
		snapshotFile.Close()
	}

	// Cada entrada associa o nome mostrado no relatório à condição booleana
	// e à função que descreve a falha, caso ela exista.
	checks := []struct {
		name    string
		valid   func(globalSnapshot) bool
		reasons invariantReasons
	}{
		{"INV1 at most one inMX", Inv1, inv1},
		{"INV2 all noMX implies no pending protocol", Inv2, inv2},
		{"INV3 deferred request belongs to an active requester", Inv3, inv3},
		{"INV4 request accounting per peer", Inv4, inv4},
		{"INV5 only a process with priority may defer", Inv5, inv5},
		{"INV6 inMX received all replies", Inv6, inv6},
	}

	completeCount, incompleteCount := 0, 0
	var violations []violation

	// Ordena os IDs para que o relatório apareça na ordem dos snapshots.
	snapshotIDs := make([]int, 0, len(snapshotsByID))
	for snapshotID := range snapshotsByID {
		snapshotIDs = append(snapshotIDs, snapshotID)
	}
	sort.Ints(snapshotIDs)

	for _, snapshotID := range snapshotIDs {
		globalState := snapshotsByID[snapshotID]

		// A análise das invariantes exige o estado de todos os processos.
		if len(globalState) != *processCount {
			incompleteCount++
			continue
		}

		completeCount++
		for _, check := range checks {
			if !check.valid(globalState) {
				for _, reason := range check.reasons(globalState) {
					violations = append(violations,
						violation{snapshotID, check.name, reason})
				}
			}
		}
	}

	// Mostra os primeiros detalhes sem preencher o terminal com centenas
	// de linhas quando a mesma falha aparece em muitos snapshots.
	for index, item := range violations {
		if index < 20 {
			fmt.Printf("snapshot %d: %s: %s\n",
				item.SnapshotID, item.Name, item.Detail)
		}
	}
	if len(violations) > 20 {
		fmt.Printf("... %d more violations\n", len(violations)-20)
	}
	fmt.Printf("complete snapshots=%d, incomplete=%d, invariant violations=%d\n",
		completeCount, incompleteCount, len(violations))

	// O arquivo compartilhado deve conter repetições de "|.".
	// "|" marca a entrada e "." marca a saída da seção crítica.
	output, err := os.ReadFile(*outputPath)
	if err != nil {
		fail(err.Error())
	}

	invalidPositions := 0
	for index, character := range output {
		if (index%2 == 0 && character != '|') ||
			(index%2 == 1 && character != '.') {
			invalidPositions++
		}
	}

	// "||" e ".." ajudam a enxergar escritas intercaladas.
	doubleBars := strings.Count(string(output), "||")
	doubleDots := strings.Count(string(output), "..")

	// Ctrl+C dentro da seção crítica pode deixar o último "|" sem ".".
	// Esse acesso ficou inacabado, mas o caractere isolado não mostra
	// que dois processos entraram ao mesmo tempo.
	unfinishedAccess := len(output)%2 != 0 && output[len(output)-1] == '|'

	fmt.Printf("shared file: %d bytes, %d two-byte positions, ||= %d, ..= %d, invalid positions=%d\n",
		len(output), len(output)/2, doubleBars, doubleDots, invalidPositions)

	if unfinishedAccess {
		fmt.Println("note: the file ends with an unfinished access (a process was interrupted inside the critical section)")
	}

	// Código 1: os dados foram lidos, mas a execução não passou na análise.
	// Nos cenários unsafe e block, o script de demonstração espera esse retorno.
	if len(violations) > 0 || completeCount < *minSnapshots ||
		incompleteCount > 0 || invalidPositions > 0 ||
		(len(output)%2 != 0 && !unfinishedAccess) {
		os.Exit(1)
	}
}

// Código 2 indica um problema de entrada, como argumento ou arquivo inválido.
func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}

// As versões Inv1...Inv6 fornecem o resultado booleano.
// As versões inv1...inv6 produzem o motivo quando há violação.
func Inv1(snapshot globalSnapshot) bool { return len(inv1(snapshot)) == 0 }
func Inv2(snapshot globalSnapshot) bool { return len(inv2(snapshot)) == 0 }
func Inv3(snapshot globalSnapshot) bool { return len(inv3(snapshot)) == 0 }
func Inv4(snapshot globalSnapshot) bool { return len(inv4(snapshot)) == 0 }
func Inv5(snapshot globalSnapshot) bool { return len(inv5(snapshot)) == 0 }
func Inv6(snapshot globalSnapshot) bool { return len(inv6(snapshot)) == 0 }

// INV1: no máximo um processo pode estar na seção crítica.
func inv1(snapshot globalSnapshot) []string {
	inCriticalSection := 0
	for _, process := range snapshot {
		if process.Local.State == DIMEX.InMX {
			inCriticalSection++
		}
	}
	if inCriticalSection > 1 {
		return []string{fmt.Sprintf("%d processes inMX", inCriticalSection)}
	}
	return nil
}

// INV2: se todos estão em NoMX, não deve haver respostas adiadas
// nem mensagens de protocolo registradas em trânsito.
func inv2(snapshot globalSnapshot) []string {
	for _, process := range snapshot {
		if process.Local.State != DIMEX.NoMX {
			return nil
		}
	}
	for processID, process := range snapshot {
		for requesterID, timestamp := range process.Local.Waiting {
			if timestamp != 0 {
				return []string{fmt.Sprintf("p%d waiting for p%d",
					processID, requesterID)}
			}
		}
		for senderID, messages := range process.Channels {
			if len(messages) > 0 {
				return []string{fmt.Sprintf("%d messages on %d -> %d",
					len(messages), senderID, processID)}
			}
		}
	}
	return nil
}

// INV3: uma resposta adiada deve corresponder a um pedido ainda ativo,
// identificado pelo mesmo timestamp.
func inv3(snapshot globalSnapshot) []string {
	var reasons []string
	for receiverID, receiver := range snapshot {
		for requesterID, timestamp := range receiver.Local.Waiting {
			requester := snapshot[requesterID]
			if timestamp != 0 &&
				(requester.Local.State == DIMEX.NoMX ||
					requester.Local.RequestTS != timestamp) {
				reasons = append(reasons, fmt.Sprintf(
					"p%d waiting[%d]=%d, requester state=%s ts=%d",
					receiverID, requesterID, timestamp,
					requester.Local.State, requester.Local.RequestTS,
				))
			}
		}
	}
	return reasons
}

// INV4: para cada processo que quer entrar, o pedido dirigido a cada colega
// deve estar em exatamente uma destas posições:
// resposta já recebida, pedido adiado, resposta em trânsito ou pedido em trânsito.
func inv4(snapshot globalSnapshot) []string {
	var reasons []string
	for requesterID, requester := range snapshot {
		if requester.Local.State != DIMEX.WantMX {
			continue
		}
		for peerID, peer := range snapshot {
			if peerID == requesterID {
				continue
			}

			positions := 0
			if requester.Local.Responses[peerID] {
				positions++
			}
			if peer.Local.Waiting[requesterID] == requester.Local.RequestTS {
				positions++
			}
			for _, message := range requester.Channels[peerID] {
				if message.Type == "RESP_OK" &&
					message.ReqTS == requester.Local.RequestTS {
					positions++
				}
			}
			for _, message := range peer.Channels[requesterID] {
				if message.Type == "REQ_ENTRY" &&
					message.ReqTS == requester.Local.RequestTS {
					positions++
				}
			}

			if positions != 1 {
				reasons = append(reasons, fmt.Sprintf(
					"request p%d from p%d accounted %d times (expected 1)",
					requesterID, peerID, positions,
				))
			}
		}
	}
	return reasons
}

// INV5: um processo só pode adiar a resposta enquanto está em InMX
// ou quando seu próprio pedido tem prioridade sobre o pedido recebido.
func inv5(snapshot globalSnapshot) []string {
	var reasons []string
	for processID, process := range snapshot {
		for requesterID, timestamp := range process.Local.Waiting {
			if timestamp == 0 || process.Local.State == DIMEX.InMX {
				continue
			}
			if process.Local.State == DIMEX.WantMX &&
				before(processID, process.Local.RequestTS, requesterID, timestamp) {
				continue
			}
			reasons = append(reasons, fmt.Sprintf(
				"p%d (%s, ts=%d) deferred p%d (ts=%d) without priority",
				processID, process.Local.State,
				process.Local.RequestTS, requesterID, timestamp,
			))
		}
	}
	return reasons
}

// Reproduz a ordem (timestamp, ID) usada pelo módulo DiMEx.
func before(firstID, firstTimestamp, secondID, secondTimestamp int) bool {
	return firstTimestamp < secondTimestamp ||
		(firstTimestamp == secondTimestamp && firstID < secondID)
}

// INV6: quem já está em InMX precisa ter recebido resposta
// de todos os outros processos.
func inv6(snapshot globalSnapshot) []string {
	var reasons []string
	for processID, process := range snapshot {
		if process.Local.State == DIMEX.InMX {
			for peerID := range snapshot {
				if processID != peerID && !process.Local.Responses[peerID] {
					reasons = append(reasons, fmt.Sprintf(
						"p%d inMX without p%d reply", processID, peerID,
					))
				}
			}
		}
	}
	return reasons
}
