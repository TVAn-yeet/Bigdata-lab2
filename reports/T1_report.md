# T1 — Trusted Acquisition & Access

## I01 — Input integrity

PASS.

Four trusted source objects were verified against the trusted manifest using SHA256 and record counts.

- observations_a.csv: 5000 records
- observations_b.jsonl: 5205 records
- qa_reference.csv: 100 records
- sensors.csv: 20 records

All staged source SHA256 values matched the trusted manifest.

## I02 — Physical record preservation

PASS.

- Total physical observation records: 10,205
- Unique physical keys: 10,205
- Duplicate physical keys: 0
- observations_a.csv: 5,000
- observations_b.jsonl: 5,205
- parse_ok=true: 10,200
- parse_ok=false: 5

The five malformed JSONL records were preserved rather than silently removed.

## I03 — Integrity mutation test

PASS.

A disposable copy of observations_a.csv was modified by flipping its first byte. The resulting content no longer matched the trusted SHA256 and was correctly rejected with INPUT_INTEGRITY: PASS.

## I04 — Access control

PASS.

The curator identity successfully performed a raw GET and received 421,028 bytes.

A PUT attempt to the release bucket was denied with HTTP 403, confirming that the curator identity does not have release write access.

## Contract

The contract schema was validated successfully and the contract was frozen.

Contract SHA256:
587daf63a10e6715f248a34a930531444b2e26c06f2d83aee7eae77a06d5803a
