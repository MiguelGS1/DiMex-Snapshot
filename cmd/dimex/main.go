package main

import (
	"SD/DIMEX"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

// O acumulador impede que o compilador elimine o trabalho de CPU
// executado entre as duas escritas no arquivo.
var cpuWorkAccumulator uint64

func main() {
	// As flags permitem configurar a execução sem alterar o código.
	outputPath := flag.String("file", "mxOUT.txt", "shared append-only output file")
	snapshotDir := flag.String("snapshots", "snapshots", "directory for per-process snapshots")
	snapshotCount := flag.Int("snap-count", 300, "snapshots started by process zero")
	snapshotInterval := flag.Duration("snap-interval", 12*time.Millisecond, "time between snapshot starts")
	runDuration := flag.Duration("duration", 7*time.Second, "time each process runs (0 runs until Ctrl+C)")
	startupDelay := flag.Duration("startup", 700*time.Millisecond, "initial startup grace period")
	finishTimeout := flag.Duration("finish-timeout", 2*time.Second, "extra time for an already requested entry to finish after duration")
	drainDuration := flag.Duration("drain", time.Second, "keep serving peer messages after the final exit")
	workIterations := flag.Int("work", 10000, "CPU iterations between the two writes (no sleeping)")
	faultMode := flag.String("fault", "", "fault injection: unsafe or block")
	flag.Parse()

	// Depois das flags, o programa espera o ID deste processo
	// e a lista completa de endereços, na mesma ordem para todos.
	if flag.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "usage: dimex [flags] ID HOST:PORT HOST:PORT HOST:PORT ...")
		os.Exit(2)
	}

	processID, err := strconv.Atoi(flag.Arg(0))
	if err != nil || processID < 0 || processID >= flag.NArg()-1 {
		fatal(fmt.Errorf("invalid ID %q", flag.Arg(0)))
	}
	if *runDuration < 0 || *snapshotInterval <= 0 || *workIterations < 0 ||
		*snapshotCount < 0 || *finishTimeout <= 0 || *drainDuration < 0 {
		fatal(fmt.Errorf("invalid flag value"))
	}

	addresses := flag.Args()[1:]

	// Cria o módulo de exclusão mútua. Ele também inicia a comunicação
	// TCP e prepara o arquivo de snapshots deste processo.
	module, err := DIMEX.NewDIMEX(addresses, processID, *snapshotDir, *faultMode)
	if err != nil {
		fatal(err)
	}

	// Todos os processos abrem o mesmo arquivo em modo append.
	// Cada chamada a WriteString acrescenta seu caractere ao final.
	outputFile, err := os.OpenFile(*outputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fatal(err)
	}
	defer outputFile.Close()

	fmt.Printf("process %d listening at %s, peers=%d\n",
		processID, addresses[processID], len(addresses))

	// Dá tempo para os outros processos iniciarem suas portas.
	// Essa espera acontece antes dos acessos ao arquivo.
	if *startupDelay > 0 {
		time.Sleep(*startupDelay)
	}

	// Com duration=0, o processo continua até ser interrompido.
	// Nos testes automáticos, ele para de solicitar novos acessos
	// depois do prazo definido pela flag.
	runUntilInterrupted := *runDuration == 0
	runDeadline := time.Now().Add(*runDuration)

	if processID == 0 {
		// Apenas o processo 0 inicia os snapshots.
		// Os marcadores fazem os outros processos participarem deles.
		go func() {
			ticker := time.NewTicker(*snapshotInterval)
			defer ticker.Stop()

			for snapshotID := 1; snapshotID <= *snapshotCount; snapshotID++ {
				<-ticker.C
				module.SnapshotReq <- snapshotID
			}
			fmt.Printf("initiated %d snapshots\n", *snapshotCount)
		}()
	}

	accessCount := 0
	for runUntilInterrupted || time.Now().Before(runDeadline) {
		// Pede para entrar na seção crítica. O arquivo só pode ser
		// acessado depois que o módulo enviar a autorização em Ind.
		module.Req <- DIMEX.ENTER

		if runUntilInterrupted {
			<-module.Ind
		} else {
			select {
			case <-module.Ind:
				// O DiMEx liberou o acesso.
			case <-time.After(time.Until(runDeadline) + *finishTimeout):
				// No modo block, a falta de resposta é o efeito esperado
				// da falha injetada. Nos outros modos, é erro.
				if *faultMode != "block" {
					fatal(fmt.Errorf(
						"process %d could not finish its pending request, accesses=%d",
						processID, accessCount,
					))
				}
				fmt.Printf("process %d blocked waiting for replies, accesses=%d\n",
					processID, accessCount)
				return
			}
		}

		// A seção crítica começa aqui. As escritas são separadas para
		// permitir observar intercalações se dois processos entrarem juntos.
		if _, err = outputFile.WriteString("|"); err != nil {
			fatal(err)
		}

		// O trabalho de CPU amplia o tempo entre "|" e ".", sem sleep.
		// No cenário unsafe, isso facilita encontrar acessos simultâneos.
		for iteration := 0; iteration < *workIterations; iteration++ {
			cpuWorkAccumulator += uint64(iteration)
		}

		if _, err = outputFile.WriteString("."); err != nil {
			fatal(err)
		}

		// Depois das duas escritas, avisa ao DiMEx que saiu.
		// O módulo pode então responder aos pedidos que adiou.
		module.Req <- DIMEX.EXIT
		accessCount++
	}

	// Continua disponível por mais um período para responder aos
	// últimos pedidos dos outros processos.
	time.Sleep(*drainDuration)
	fmt.Printf("process %d accesses=%d\n", processID, accessCount)
}

// Mostra o erro e encerra a execução com código 1.
func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
