#!/usr/bin/env bash

# Encerra o script se um comando falhar, se uma variável não existir
# ou se algum comando de um pipeline retornar erro.
set -euo pipefail

# Entra na raiz do projeto, independentemente de onde o script foi chamado.
cd "$(dirname "$0")/.."

# O primeiro argumento escolhe o cenário; o segundo define a primeira porta.
scenario="${1:-normal}"
base_port="${2:-5100}"

# Aceita somente os três cenários implementados pelo programa.
case "$scenario" in
 normal|unsafe|block) ;;
 *) echo "usage: $0 [normal|unsafe|block] [base-port]" >&2; exit 2 ;;
esac

# Prepara os arquivos da execução atual. Os snapshots antigos desse cenário
# são removidos para não serem confundidos com os que serão gerados agora.
mkdir -p bin "runs/$scenario/snapshots"
find "runs/$scenario/snapshots" -maxdepth 1 -type f -name 'process_*.jsonl' -delete
: > "runs/$scenario/mxOUT.txt"

# Compila a aplicação e o verificador. O script sempre executa o código atual.
go build -buildvcs=false -o bin/dimex ./cmd/dimex
go build -buildvcs=false -o bin/check ./cmd/check

# O trabalho de CPU ocorre entre as escritas de "|" e ".".
# Em unsafe, a seção crítica fica mais longa para aumentar a chance
# de um snapshot encontrar processos acessando o arquivo ao mesmo tempo.
work_iterations=15000
if [[ "$scenario" == unsafe ]]; then work_iterations=50000; fi

# Cada processo roda por 5 segundos. O processo 0 solicita 250 snapshots,
# com intervalo de 12 ms entre os inícios. Os outros tempos ajudam na
# inicialização e na conclusão dos pedidos que já estavam em andamento.
process_args=(--duration=5s --startup=700ms --finish-timeout=2s --drain=1s --snap-count=250 --snap-interval=12ms --work="$work_iterations" --snapshots="runs/$scenario/snapshots" --file="runs/$scenario/mxOUT.txt")

# O modo normal não recebe falha; os outros dois ativam a falha escolhida.
if [[ "$scenario" != normal ]]; then process_args+=(--fault="$scenario"); fi

# Os três processos rodam na mesma máquina, cada um escutando sua porta.
peer_addresses=("127.0.0.1:$base_port" "127.0.0.1:$((base_port+1))" "127.0.0.1:$((base_port+2))")

# Todos recebem a mesma lista de endereços; o ID determina qual porta escutar.
# O "&" inicia cada processo em segundo plano para que os três rodem juntos.
process_ids=()
for process_id in 0 1 2; do
 bin/dimex "${process_args[@]}" "$process_id" "${peer_addresses[@]}" > "runs/$scenario/process_$process_id.log" 2>&1 &
 process_ids+=("$!")
done

# Se o script for interrompido ou falhar, encerra os processos que iniciou.
# Após a conclusão normal de todos eles, essa limpeza deixa de ser necessária.
trap 'for process_pid in "${process_ids[@]}"; do kill "$process_pid" 2>/dev/null || true; done' EXIT
for process_pid in "${process_ids[@]}"; do wait "$process_pid"; done
trap - EXIT

echo "=== $scenario ==="

# O verificador junta os estados dos três processos pelo ID do snapshot,
# exige pelo menos 250 snapshots completos e examina o arquivo compartilhado.
# No cenário normal ele deve retornar 0; com falha detectada retorna diferente de 0.
if check_result=$(bin/check --snapshots="runs/$scenario/snapshots" --file="runs/$scenario/mxOUT.txt" --n=3 --min=250 2>&1); then
 printf '%s\n' "$check_result"

 # Uma execução com falha que passou na verificação não demonstrou a falha.
 if [[ "$scenario" != normal ]]; then
  echo "The injected fault was not detected." >&2
  exit 1
 fi
else
 printf '%s\n' "$check_result"

 # Qualquer violação no cenário normal é um problema.
 if [[ "$scenario" == normal ]]; then
  echo "Normal run failed validation." >&2
  exit 1
 fi

 # A falha precisa aparecer em pelo menos uma invariante de snapshot.
 # Um erro apenas na leitura do arquivo não basta para validar a demonstração.
 if ! grep -Eq 'invariant violations=[1-9][0-9]*' <<<"$check_result"; then
  echo "The file checker failed, but no snapshot invariant detected the injected fault." >&2
  exit 1
 fi

 # Em unsafe, INV1 aponta mais de um processo na seção crítica.
 if [[ "$scenario" == unsafe ]] && ! grep -q 'INV1' <<<"$check_result"; then
  echo "The unsafe fault did not appear in a snapshot (INV1)." >&2
  exit 1
 fi

 # Em block, INV5 aponta respostas adiadas sem a prioridade exigida.
 if [[ "$scenario" == block ]] && ! grep -q 'INV5' <<<"$check_result"; then
  echo "The blocked fault did not appear in a snapshot (INV5)." >&2
  exit 1
 fi

 echo "The injected fault was detected in snapshots (nonzero checker exit is expected)."
fi