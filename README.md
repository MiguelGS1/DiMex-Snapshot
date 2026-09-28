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