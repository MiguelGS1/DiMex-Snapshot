# Trabalho 1 — DiMEx e snapshot de Chandy–Lamport

## Componentes do grupo

- Miguel Gheno dos Santos
- Pedro Riva
- Max Breuel

## Como executar o trabalho

É necessário ter Go 1.18 ou superior e Bash instalados. No terminal, clone o repositório e entre na pasta do projeto:

```bash
git clone https://github.com/MiguelGS1/DiMex-Snapshot.git
cd DiMex-Snapshot
```

Se o projeto já estiver aberto no VS Code, basta abrir o terminal na pasta que contém o arquivo `go.mod`. Execute os comandos abaixo **um de cada vez**:

```bash
bash scripts/demo.sh normal
bash scripts/demo.sh unsafe 5200
bash scripts/demo.sh block 5300
```

Cada comando compila o programa, inicia três processos e coleta 250 snapshots:

- `normal`: execução sem falhas; espera-se 250 snapshots completos e nenhuma violação de invariantes.
- `unsafe`: simula uma falha de exclusão mútua; o verificador deve identificar a violação `INV1`.
- `block`: simula bloqueio dos processos; o verificador deve identificar a violação `INV6`.

Os resultados são gravados em `runs/normal/`, `runs/unsafe/` e `runs/block/`. Para conferir a quantidade de snapshots da execução normal:

```bash
wc -l runs/normal/snapshots/process_*.jsonl
```

O comando deve mostrar 250 linhas em cada um dos três arquivos.

## Rodar à mão, em terminais separados

O script acima sobe os três processos de uma vez e os encerra sozinho depois de alguns
segundos. Para rodar como no template da professora — um processo por terminal, até o
usuário interromper — use `--duration=0`, que faz cada processo rodar até o `Ctrl+C`:

```bash
go build -o bin/dimex ./cmd/dimex
bin/dimex --duration=0 --snapshots=snapshots --file=mxOUT.txt 0 127.0.0.1:5000 127.0.0.1:5001 127.0.0.1:5002
bin/dimex --duration=0 --snapshots=snapshots --file=mxOUT.txt 1 127.0.0.1:5000 127.0.0.1:5001 127.0.0.1:5002
bin/dimex --duration=0 --snapshots=snapshots --file=mxOUT.txt 2 127.0.0.1:5000 127.0.0.1:5001 127.0.0.1:5002
```

Cada linha vai num terminal diferente, e a ordem não importa: quem sobe primeiro espera os
outros. Depois do `Ctrl+C` em todos, confira o resultado com:

```bash
go build -o bin/check ./cmd/check
bin/check --snapshots=snapshots --file=mxOUT.txt --n=3
```