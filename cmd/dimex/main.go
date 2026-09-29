package main

import (
	"SD/DIMEX"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

var workSink uint64

func main() {
	filePath := flag.String("file", "mxOUT.txt", "shared append-only output file")
	snapshots := flag.String("snapshots", "snapshots", "directory for per-process snapshots")
	count := flag.Int("snap-count", 300, "snapshots started by process zero")
	interval := flag.Duration("snap-interval", 12*time.Millisecond, "time between snapshot starts")
	duration := flag.Duration("duration", 7*time.Second, "time each process runs (0 runs until Ctrl+C)")
	startup := flag.Duration("startup", 700*time.Millisecond, "initial startup grace period")
	finishTimeout := flag.Duration("finish-timeout", 2*time.Second, "extra time for an already requested entry to finish after duration")
	drain := flag.Duration("drain", time.Second, "keep serving peer messages after the final exit")
	work := flag.Int("work", 10000, "CPU iterations between the two writes (no sleeping)")
	fault := flag.String("fault", "", "fault injection: unsafe or block")
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "usage: dimex [flags] ID HOST:PORT HOST:PORT HOST:PORT ...")
		os.Exit(2)
	}
	id, e := strconv.Atoi(flag.Arg(0))
	if e != nil || id < 0 || id >= flag.NArg()-1 {
		fatal(fmt.Errorf("invalid ID %q", flag.Arg(0)))
	}
	if *duration < 0 || *interval <= 0 || *work < 0 || *count < 0 || *finishTimeout <= 0 || *drain < 0 {
		fatal(fmt.Errorf("invalid flag value"))
	}
	addresses := flag.Args()[1:]
	d, e := DIMEX.NewDIMEX(addresses, id, *snapshots, *fault)
	if e != nil {
		fatal(e)
	}
	f, e := os.OpenFile(*filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if e != nil {
		fatal(e)
	}
	defer f.Close()
	fmt.Printf("process %d listening at %s, peers=%d\n", id, addresses[id], len(addresses))
	if *startup > 0 {
		time.Sleep(*startup)
	} // Startup only; no sleeps between critical-section operations.
	// With --duration=0 the process runs until Ctrl+C, like the original template.
	forever := *duration == 0
	deadline := time.Now().Add(*duration)
	if id == 0 {
		go func() {
			ticker := time.NewTicker(*interval)
			defer ticker.Stop()
			for i := 1; i <= *count; i++ {
				<-ticker.C
				d.SnapshotReq <- i
			}
			fmt.Printf("initiated %d snapshots\n", *count)
		}()
	}
	accesses := 0
	for forever || time.Now().Before(deadline) {
		d.Req <- DIMEX.ENTER
		if forever {
			<-d.Ind // no time limit: wait until DIMEX grants access
		} else {
			select {
			case <-d.Ind:
			case <-time.After(time.Until(deadline) + *finishTimeout):
				if *fault != "block" {
					fatal(fmt.Errorf("process %d could not finish its pending request, accesses=%d", id, accesses))
				}
				fmt.Printf("process %d blocked waiting for replies, accesses=%d\n", id, accesses)
				return
			}
		}
		if _, e = f.WriteString("|"); e != nil {
			fatal(e)
		}
		for j := 0; j < *work; j++ {
			workSink += uint64(j)
		}
		if _, e = f.WriteString("."); e != nil {
			fatal(e)
		}
		d.Req <- DIMEX.EXIT
		accesses++
	}
	// Keep responding to peers whose final request is still pending.
	time.Sleep(*drain)
	fmt.Printf("process %d accesses=%d\n", id, accesses)
}
func fatal(e error) { fmt.Fprintln(os.Stderr, e); os.Exit(1) }
