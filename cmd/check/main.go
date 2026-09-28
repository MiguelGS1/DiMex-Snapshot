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

type global map[int]DIMEX.Snapshot
type violation struct {
	Snapshot     int
	Name, Detail string
}
type invariant func(global) []string

func main() {
	dir := flag.String("snapshots", "snapshots", "per-process JSONL directory")
	file := flag.String("file", "mxOUT.txt", "shared output file")
	n := flag.Int("n", 3, "number of processes")
	min := flag.Int("min", 1, "minimum complete snapshots required")
	flag.Parse()
	if *n < 1 || *min < 0 {
		fail("invalid n or min")
	}
	all := make(map[int]global)
	for p := 0; p < *n; p++ {
		path := filepath.Join(*dir, fmt.Sprintf("process_%d.jsonl", p))
		f, e := os.Open(path)
		if e != nil {
			fail(e.Error())
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 4096), 10*1024*1024)
		for scanner.Scan() {
			var s DIMEX.Snapshot
			if e := json.Unmarshal(scanner.Bytes(), &s); e != nil {
				fail(fmt.Sprintf("%s: %v", path, e))
			}
			if s.ProcessID != p || s.SnapshotID <= 0 {
				fail(fmt.Sprintf("%s: invalid snapshot ID or process ID", path))
			}
			if len(s.Local.Responses) != *n || len(s.Local.Waiting) != *n || len(s.Channels) != *n-1 {
				fail(fmt.Sprintf("%s: malformed arrays or channels in snapshot %d", path, s.SnapshotID))
			}
			if all[s.SnapshotID] == nil {
				all[s.SnapshotID] = make(global)
			}
			if _, duplicate := all[s.SnapshotID][p]; duplicate {
				fail(fmt.Sprintf("%s: repeated snapshot %d", path, s.SnapshotID))
			}
			all[s.SnapshotID][p] = s
		}
		if e := scanner.Err(); e != nil {
			fail(e.Error())
		}
		f.Close()
	}
	checks := []struct {
		name    string
		valid   func(global) bool
		reasons invariant
	}{
		{"INV1 at most one inMX", Inv1, inv1},
		{"INV2 all noMX implies no pending protocol", Inv2, inv2},
		{"INV3 deferred request belongs to an active requester", Inv3, inv3},
		{"INV4 request accounting per peer", Inv4, inv4},
		{"INV5 noMX cannot defer a reply", Inv5, inv5},
		{"INV6 priority wait graph is acyclic", Inv6, inv6},
		{"INV7 inMX received all replies", Inv7, inv7},
	}
	good, missing := 0, 0
	var violations []violation
	ids := make([]int, 0, len(all))
	for id := range all {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		s := all[id]
		if len(s) != *n {
			missing++
			continue
		}
		good++
		for _, c := range checks {
			if !c.valid(s) {
				for _, reason := range c.reasons(s) {
					violations = append(violations, violation{id, c.name, reason})
				}
			}
		}
	}
	for i, v := range violations {
		if i < 20 {
			fmt.Printf("snapshot %d: %s: %s\n", v.Snapshot, v.Name, v.Detail)
		}
	}
	if len(violations) > 20 {
		fmt.Printf("... %d more violations\n", len(violations)-20)
	}
	fmt.Printf("complete snapshots=%d, incomplete=%d, invariant violations=%d\n", good, missing, len(violations))
	b, e := os.ReadFile(*file)
	if e != nil {
		fail(e.Error())
	}
	malformed := 0
	for i, c := range b {
		if (i%2 == 0 && c != '|') || (i%2 == 1 && c != '.') {
			malformed++
		}
	}
	doubleBars := strings.Count(string(b), "||")
	doubleDots := strings.Count(string(b), "..")
	fmt.Printf("shared file: %d bytes, %d two-byte positions, ||= %d, ..= %d, invalid positions=%d\n", len(b), len(b)/2, doubleBars, doubleDots, malformed)
	if len(violations) > 0 || good < *min || missing > 0 || malformed > 0 || len(b)%2 != 0 {
		os.Exit(1)
	}
}
func fail(s string) { fmt.Fprintln(os.Stderr, s); os.Exit(2) }

// Each InvX is a boolean predicate over a complete global snapshot.
// The lower-case companions provide a reason when a predicate fails.
func Inv1(s global) bool { return len(inv1(s)) == 0 }
func Inv2(s global) bool { return len(inv2(s)) == 0 }
func Inv3(s global) bool { return len(inv3(s)) == 0 }
func Inv4(s global) bool { return len(inv4(s)) == 0 }
func Inv5(s global) bool { return len(inv5(s)) == 0 }
func Inv6(s global) bool { return len(inv6(s)) == 0 }
func Inv7(s global) bool { return len(inv7(s)) == 0 }
func inv1(s global) []string {
	count := 0
	for _, p := range s {
		if p.Local.State == DIMEX.InMX {
			count++
		}
	}
	if count > 1 {
		return []string{fmt.Sprintf("%d processes inMX", count)}
	}
	return nil
}
func inv2(s global) []string {
	for _, p := range s {
		if p.Local.State != DIMEX.NoMX {
			return nil
		}
	}
	for id, p := range s {
		for q, ts := range p.Local.Waiting {
			if ts != 0 {
				return []string{fmt.Sprintf("p%d waiting for p%d", id, q)}
			}
		}
		for q, msgs := range p.Channels {
			if len(msgs) > 0 {
				return []string{fmt.Sprintf("%d messages on %d -> %d", len(msgs), q, id)}
			}
		}
	}
	return nil
}
func inv3(s global) []string {
	var out []string
	for q, receiver := range s {
		for p, ts := range receiver.Local.Waiting {
			if ts != 0 && (s[p].Local.State == DIMEX.NoMX || s[p].Local.RequestTS != ts) {
				out = append(out, fmt.Sprintf("p%d waiting[%d]=%d, requester state=%s ts=%d", q, p, ts, s[p].Local.State, s[p].Local.RequestTS))
			}
		}
	}
	return out
}
func inv4(s global) []string {
	var out []string
	for p, requester := range s {
		if requester.Local.State != DIMEX.WantMX {
			continue
		}
		for q, other := range s {
			if q == p {
				continue
			}
			total := 0
			if requester.Local.Responses[q] {
				total++
			}
			if other.Local.Waiting[p] == requester.Local.RequestTS {
				total++
			}
			for _, m := range requester.Channels[q] {
				if m.Type == "RESP_OK" && m.ReqTS == requester.Local.RequestTS {
					total++
				}
			}
			for _, m := range other.Channels[p] {
				if m.Type == "REQ_ENTRY" && m.ReqTS == requester.Local.RequestTS {
					total++
				}
			}
			if total != 1 {
				out = append(out, fmt.Sprintf("request p%d from p%d accounted %d times (expected 1)", p, q, total))
			}
		}
	}
	return out
}
func inv5(s global) []string {
	var out []string
	for q, p := range s {
		if p.Local.State == DIMEX.NoMX {
			for id, ts := range p.Local.Waiting {
				if ts != 0 {
					out = append(out, fmt.Sprintf("p%d noMX deferred p%d", q, id))
				}
			}
		}
	}
	return out
}
func inv6(s global) []string {
	// Edge p->q: requester p is waiting for q, which deferred its reply.
	color := make(map[int]int)
	var visit func(int) bool
	visit = func(p int) bool {
		color[p] = 1
		for q, other := range s {
			if q == p || s[p].Local.State != DIMEX.WantMX || other.Local.Waiting[p] != s[p].Local.RequestTS {
				continue
			}
			if color[q] == 1 {
				return true
			}
			if color[q] == 0 && visit(q) {
				return true
			}
		}
		color[p] = 2
		return false
	}
	for p := range s {
		if color[p] == 0 && visit(p) {
			return []string{"cycle in deferred-reply wait graph"}
		}
	}
	return nil
}
func inv7(s global) []string {
	var out []string
	for p, proc := range s {
		if proc.Local.State == DIMEX.InMX {
			for q := range s {
				if p != q && !proc.Local.Responses[q] {
					out = append(out, fmt.Sprintf("p%d inMX without p%d reply", p, q))
				}
			}
		}
	}
	return out
}
