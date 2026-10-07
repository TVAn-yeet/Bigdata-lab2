"""Supplied plumbing: verify input bytes and preserve every physical row.

Run from /work: python stage_inputs.py s3 trusted_manifest.json
Local fixture check: python stage_inputs.py local fixture/source_manifest.json
"""

import csv
import hashlib
import json
import os
import sys
from pathlib import Path

mode, manifest_path = sys.argv[1:3]

assert mode in {"local", "s3"}

manifest = json.loads(Path(manifest_path).read_text())

root = Path("snapshot")
root.mkdir(exist_ok=True)

keys = {
    "observations_a.csv",
    "observations_b.jsonl",
    "sensors.csv",
    "qa_reference.csv",
}

assert {x["key"] for x in manifest["objects"]} == keys

prefix = "lab2/inputs/batch-01/"
store = os.environ.get("STORE_ID", "bd-gXX-objects")

if mode == "s3":
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )

for obj in manifest["objects"]:
    key = obj["key"]

    if mode == "s3":
        response = client.get_object(
            Bucket="research-raw",
            Key=prefix + key,
        )
        body = response["Body"]

        try:
            data = body.read()
        finally:
            body.close()
    else:
        data = (Path(manifest_path).parent / key).read_bytes()

    digest = hashlib.sha256(data).hexdigest()

    if len(data) != obj["bytes"] or digest != obj["sha256"]:
        raise RuntimeError("INPUT_INTEGRITY: " + key)

    (root / key).write_bytes(data)


fields = [
    "record_id",
    "sensor_id",
    "event_time",
    "ingest_time",
    "reading",
    "unit",
    "operator_email",
]

bkeys = [
    "id",
    "sensor",
    "timestamp",
    "arrived_at",
    "temperature",
    "temperature_unit",
    "operator",
]

extra = [
    "source_object",
    "source_row",
    "source_sha256",
    "parse_ok",
    "raw_payload",
]

inventory = []

with Path("envelopes.csv").open(
    "w",
    newline="",
    encoding="utf-8",
) as out:

    writer = csv.DictWriter(
        out,
        fieldnames=fields + extra,
    )

    writer.writeheader()

    for key in sorted(keys):

        if not key.startswith("observations_"):
            continue

        p = root / key
        digest = hashlib.sha256(p.read_bytes()).hexdigest()

        n = 0

        with p.open(
            encoding="utf-8",
            newline="",
        ) as src:

            records = (
                csv.DictReader(src)
                if key.endswith(".csv")
                else src
            )

            for n, raw in enumerate(records, 1):

                payload = (
                    json.dumps(raw)
                    if isinstance(raw, dict)
                    else raw
                )

                ok = True

                try:
                    r = (
                        raw
                        if isinstance(raw, dict)
                        else json.loads(raw)
                    )

                    if not isinstance(r, dict):
                        raise ValueError("not object")

                    if key.endswith(".jsonl"):
                        r = {
                            a: r.get(b)
                            for a, b in zip(fields, bkeys)
                        }

                    r = {
                        k: "" if r.get(k) is None else str(r[k])
                        for k in fields
                    }

                except (ValueError, TypeError):
                    r = {k: "" for k in fields}
                    ok = False

                r.update(
                    source_object=(
                        "s3://research-raw/"
                        + prefix
                        + key
                    ),
                    source_row=n,
                    source_sha256=digest,
                    parse_ok=str(ok).lower(),
                    raw_payload=payload,
                )

                writer.writerow(r)

        expected = next(
            x["records"]
            for x in manifest["objects"]
            if x["key"] == key
        )

        if n != expected:
            raise RuntimeError("ROW_COUNT: " + key)

        inventory.append(
            dict(
                store_id=store,
                key=prefix + key,
                records=n,
                sha256=digest,
            )
        )

Path("input_inventory.json").write_text(
    json.dumps(inventory, indent=2)
)

print(
    json.dumps(
        {
            "status": "VERIFIED",
            "observation_rows": sum(
                x["records"] for x in inventory
            ),
            "store_id": store,
        }
    )
)
