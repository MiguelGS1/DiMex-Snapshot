#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

scenario="${1:-normal}"
base_port="${2:-5100}"
case "$scenario" in
 normal|unsafe|block) ;;
 *) echo "usage: $0 [normal|unsafe|block] [base-port]" >&2; exit 2 ;;
esac

mkdir -p bin "runs/$scenario/snapshots"
find "runs/$scenario/snapshots" -maxdepth 1 -type f -name 'process_*.jsonl' -delete
: > "runs/$scenario/mxOUT.txt"
go build -buildvcs=false -o bin/dimex ./cmd/dimex
go build -buildvcs=false -o bin/check ./cmd/check

work_iterations=15000
if [[ "$scenario" == unsafe ]]; then work_iterations=50000; fi
process_args=(--duration=5s --startup=700ms --finish-timeout=2s --drain=1s --snap-count=250 --snap-interval=12ms --work="$work_iterations" --snapshots="runs/$scenario/snapshots" --file="runs/$scenario/mxOUT.txt")
if [[ "$scenario" != normal ]]; then process_args+=(--fault="$scenario"); fi
peer_addresses=("127.0.0.1:$base_port" "127.0.0.1:$((base_port+1))" "127.0.0.1:$((base_port+2))")

# Todos recebem a mesma lista de endereços; o ID determina qual porta escutar.
process_ids=()
for process_id in 0 1 2; do
 bin/dimex "${process_args[@]}" "$process_id" "${peer_addresses[@]}" > "runs/$scenario/process_$process_id.log" 2>&1 &
 process_ids+=("$!")
done
trap 'for process_pid in "${process_ids[@]}"; do kill "$process_pid" 2>/dev/null || true; done' EXIT
for process_pid in "${process_ids[@]}"; do wait "$process_pid"; done
trap - EXIT

echo "=== $scenario ==="
if check_result=$(bin/check --snapshots="runs/$scenario/snapshots" --file="runs/$scenario/mxOUT.txt" --n=3 --min=250 2>&1); then
 printf '%s\n' "$check_result"
 if [[ "$scenario" != normal ]]; then echo "The injected fault was not detected." >&2; exit 1; fi
else
 printf '%s\n' "$check_result"
 if [[ "$scenario" == normal ]]; then echo "Normal run failed validation." >&2; exit 1; fi
 if ! grep -Eq 'invariant violations=[1-9][0-9]*' <<<"$check_result"; then
  echo "The file checker failed, but no snapshot invariant detected the injected fault." >&2
  exit 1
 fi
 if [[ "$scenario" == unsafe ]] && ! grep -q 'INV1' <<<"$check_result"; then
  echo "The unsafe fault did not appear in a snapshot (INV1)." >&2
  exit 1
 fi
 if [[ "$scenario" == block ]] && ! grep -q 'INV5' <<<"$check_result"; then
  echo "The blocked fault did not appear in a snapshot (INV5)." >&2
  exit 1
 fi
 echo "The injected fault was detected in snapshots (nonzero checker exit is expected)."
fi
