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

// Cada ID de processo aponta para seu estado no mesmo corte global.
type globalSnapshot map[int]DIMEX.Snapshot

type violation struct {
	SnapshotID int
	Name       string
	Detail     string
}

type invariantReasons func(globalSnapshot) []string

func main() {
	snapshotDir := flag.String("snapshots", "snapshots", "per-process JSONL directory")
	outputPath := flag.String("file", "mxOUT.txt", "shared output file")
	processCount := flag.Int("n", 3, "number of processes")
	minSnapshots := flag.Int("min", 1, "minimum complete snapshots required")
	flag.Parse()
	if *processCount < 1 || *minSnapshots < 0 {
		fail("invalid n or min")
	}

	snapshotsByID := make(map[int]globalSnapshot)
	for processID := 0; processID < *processCount; processID++ {
		snapshotPath := filepath.Join(*snapshotDir, fmt.Sprintf("process_%d.jsonl", processID))
		snapshotFile, err := os.Open(snapshotPath)
		if err != nil {
			fail(err.Error())
		}
		scanner := bufio.NewScanner(snapshotFile)
		scanner.Buffer(make([]byte, 4096), 10*1024*1024)
		for scanner.Scan() {
			var snapshot DIMEX.Snapshot
			if err := json.Unmarshal(scanner.Bytes(), &snapshot); err != nil {
				fail(fmt.Sprintf("%s: %v", snapshotPath, err))
			}
			if snapshot.ProcessID != processID || snapshot.SnapshotID <= 0 {
				fail(fmt.Sprintf("%s: invalid snapshot ID or process ID", snapshotPath))
			}
			if len(snapshot.Local.Responses) != *processCount ||
				len(snapshot.Local.Waiting) != *processCount ||
				len(snapshot.Channels) != *processCount-1 {
				fail(fmt.Sprintf("%s: malformed arrays or channels in snapshot %d", snapshotPath, snapshot.SnapshotID))
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
	snapshotIDs := make([]int, 0, len(snapshotsByID))
	for snapshotID := range snapshotsByID {
		snapshotIDs = append(snapshotIDs, snapshotID)
	}
	sort.Ints(snapshotIDs)
	for _, snapshotID := range snapshotIDs {
		globalState := snapshotsByID[snapshotID]
		if len(globalState) != *processCount {
			incompleteCount++
			continue
		}
		completeCount++
		for _, check := range checks {
			if !check.valid(globalState) {
				for _, reason := range check.reasons(globalState) {
					violations = append(violations, violation{snapshotID, check.name, reason})
				}
			}
		}
	}
	for index, item := range violations {
		if index < 20 {
			fmt.Printf("snapshot %d: %s: %s\n", item.SnapshotID, item.Name, item.Detail)
		}
	}
	if len(violations) > 20 {
		fmt.Printf("... %d more violations\n", len(violations)-20)
	}
	fmt.Printf("complete snapshots=%d, incomplete=%d, invariant violations=%d\n",
		completeCount, incompleteCount, len(violations))

	output, err := os.ReadFile(*outputPath)
	if err != nil {
		fail(err.Error())
	}
	invalidPositions := 0
	for index, character := range output {
		if (index%2 == 0 && character != '|') || (index%2 == 1 && character != '.') {
			invalidPositions++
		}
	}
	doubleBars := strings.Count(string(output), "||")
	doubleDots := strings.Count(string(output), "..")
	// Ctrl+C durante a seção crítica pode deixar apenas o último "|".
	unfinishedAccess := len(output)%2 != 0 && output[len(output)-1] == '|'
	fmt.Printf("shared file: %d bytes, %d two-byte positions, ||= %d, ..= %d, invalid positions=%d\n",
		len(output), len(output)/2, doubleBars, doubleDots, invalidPositions)
	if unfinishedAccess {
		fmt.Println("note: the file ends with an unfinished access (a process was interrupted inside the critical section)")
	}
	if len(violations) > 0 || completeCount < *minSnapshots || incompleteCount > 0 ||
		invalidPositions > 0 || (len(output)%2 != 0 && !unfinishedAccess) {
		os.Exit(1)
	}
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}

func Inv1(snapshot globalSnapshot) bool { return len(inv1(snapshot)) == 0 }
func Inv2(snapshot globalSnapshot) bool { return len(inv2(snapshot)) == 0 }
func Inv3(snapshot globalSnapshot) bool { return len(inv3(snapshot)) == 0 }
func Inv4(snapshot globalSnapshot) bool { return len(inv4(snapshot)) == 0 }
func Inv5(snapshot globalSnapshot) bool { return len(inv5(snapshot)) == 0 }
func Inv6(snapshot globalSnapshot) bool { return len(inv6(snapshot)) == 0 }

// INV1: só um processo pode estar na seção crítica.
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

// INV2: se todos estão livres, não deve haver trabalho de protocolo pendente.
func inv2(snapshot globalSnapshot) []string {
	for _, process := range snapshot {
		if process.Local.State != DIMEX.NoMX {
			return nil
		}
	}
	for processID, process := range snapshot {
		for requesterID, timestamp := range process.Local.Waiting {
			if timestamp != 0 {
				return []string{fmt.Sprintf("p%d waiting for p%d", processID, requesterID)}
			}
		}
		for senderID, messages := range process.Channels {
			if len(messages) > 0 {
				return []string{fmt.Sprintf("%d messages on %d -> %d", len(messages), senderID, processID)}
			}
		}
	}
	return nil
}

// INV3: uma resposta adiada precisa pertencer ao pedido atual de quem a aguarda.
func inv3(snapshot globalSnapshot) []string {
	var reasons []string
	for receiverID, receiver := range snapshot {
		for requesterID, timestamp := range receiver.Local.Waiting {
			requester := snapshot[requesterID]
			if timestamp != 0 && (requester.Local.State == DIMEX.NoMX || requester.Local.RequestTS != timestamp) {
				reasons = append(reasons, fmt.Sprintf("p%d waiting[%d]=%d, requester state=%s ts=%d",
					receiverID, requesterID, timestamp, requester.Local.State, requester.Local.RequestTS))
			}
		}
	}
	return reasons
}

// INV4: para cada par, o pedido deve estar em exatamente uma etapa do caminho.
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
				if message.Type == "RESP_OK" && message.ReqTS == requester.Local.RequestTS {
					positions++
				}
			}
			for _, message := range peer.Channels[requesterID] {
				if message.Type == "REQ_ENTRY" && message.ReqTS == requester.Local.RequestTS {
					positions++
				}
			}
			if positions != 1 {
				reasons = append(reasons, fmt.Sprintf("request p%d from p%d accounted %d times (expected 1)",
					requesterID, peerID, positions))
			}
		}
	}
	return reasons
}

// INV5: só adia a resposta quem já entrou ou possui um pedido com prioridade.
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
			reasons = append(reasons, fmt.Sprintf("p%d (%s, ts=%d) deferred p%d (ts=%d) without priority",
				processID, process.Local.State, process.Local.RequestTS, requesterID, timestamp))
		}
	}
	return reasons
}

// Mesma ordem total usada pelo módulo DiMEx.
func before(firstID, firstTimestamp, secondID, secondTimestamp int) bool {
	return firstTimestamp < secondTimestamp || (firstTimestamp == secondTimestamp && firstID < secondID)
}

// INV6: quem já entrou recebeu a autorização de todos os outros processos.
func inv6(snapshot globalSnapshot) []string {
	var reasons []string
	for processID, process := range snapshot {
		if process.Local.State == DIMEX.InMX {
			for peerID := range snapshot {
				if processID != peerID && !process.Local.Responses[peerID] {
					reasons = append(reasons, fmt.Sprintf("p%d inMX without p%d reply", processID, peerID))
				}
			}
		}
	}
	return reasons
}
