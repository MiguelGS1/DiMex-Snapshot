#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mode="${1:-normal}"
base="${2:-5100}"
case "$mode" in normal|unsafe|block) ;; *) echo "usage: $0 [normal|unsafe|block] [base-port]" >&2; exit 2;; esac
mkdir -p bin "runs/$mode/snapshots"
find "runs/$mode/snapshots" -maxdepth 1 -type f -name 'process_*.jsonl' -delete
: > "runs/$mode/mxOUT.txt"
go build -buildvcs=false -o bin/dimex ./cmd/dimex
go build -buildvcs=false -o bin/check ./cmd/check
work=15000
if [[ "$mode" == unsafe ]]; then work=50000; fi
args=(--duration=5s --startup=700ms --finish-timeout=2s --drain=1s --snap-count=250 --snap-interval=12ms --work="$work" --snapshots="runs/$mode/snapshots" --file="runs/$mode/mxOUT.txt")
if [[ "$mode" != normal ]]; then args+=(--fault="$mode"); fi
addresses=("127.0.0.1:$base" "127.0.0.1:$((base+1))" "127.0.0.1:$((base+2))")
pids=()
for id in 0 1 2; do
 bin/dimex "${args[@]}" "$id" "${addresses[@]}" > "runs/$mode/process_$id.log" 2>&1 &
 pids+=("$!")
done
trap 'for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done' EXIT
for p in "${pids[@]}"; do wait "$p"; done
trap - EXIT
echo "=== $mode ==="
if result=$(bin/check --snapshots="runs/$mode/snapshots" --file="runs/$mode/mxOUT.txt" --n=3 --min=250 2>&1); then
 printf '%s\n' "$result"
 if [[ "$mode" != normal ]]; then echo "The injected fault was not detected." >&2; exit 1; fi
else
 printf '%s\n' "$result"
 if [[ "$mode" == normal ]]; then echo "Normal run failed validation." >&2; exit 1; fi
 if ! grep -Eq 'invariant violations=[1-9][0-9]*' <<<"$result"; then
  echo "The file checker failed, but no snapshot invariant detected the injected fault." >&2
  exit 1
 fi
 if [[ "$mode" == unsafe ]] && ! grep -q 'INV1' <<<"$result"; then
  echo "The unsafe fault did not appear in a snapshot (INV1)." >&2
  exit 1
 fi
 if [[ "$mode" == block ]] && ! grep -q 'INV6' <<<"$result"; then
  echo "The blocked fault did not appear in a snapshot (INV6)." >&2
  exit 1
 fi
 echo "The injected fault was detected in snapshots (nonzero checker exit is expected)."
fi
