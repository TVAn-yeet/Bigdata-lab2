import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

root = Path("input/snapshot")
manifest_path = Path("input/trusted_manifest.json")
output = Path("input/input_inventory.json")

manifest = json.loads(manifest_path.read_text())
store_id = os.environ.get("STORE_ID")
bucket = "research-raw"
prefix = "lab2/inputs/batch-01/"

if not store_id or store_id == "bd-gXX-objects":
    raise SystemExit(
        "STOP: Set STORE_ID to the actual configured store identifier first."
    )

inventory = []
retrieved_at = datetime.now(timezone.utc).isoformat()

for obj in manifest["objects"]:
    name = obj["key"]
    p = root / name
    data = p.read_bytes()
    digest = hashlib.sha256(data).hexdigest()

    if len(data) != obj["bytes"] or digest != obj["sha256"]:
        raise SystemExit(f"INPUT_INTEGRITY: {name}")

    if name.endswith(".jsonl"):
        with p.open(encoding="utf-8") as f:
            records = sum(1 for _ in f)
        fmt = "JSONL"
    else:
        with p.open(encoding="utf-8", newline="") as f:
            records = sum(1 for _ in csv.DictReader(f))
        fmt = "CSV"

    if records != obj["records"]:
        raise SystemExit(f"ROW_COUNT: {name}")

    inventory.append({
        "store_id": store_id,
        "bucket": bucket,
        "key": prefix + name,
        "format": fmt,
        "records": records,
        "bytes": len(data),
        "sha256": digest,
        "retrieved_at_utc": retrieved_at
    })

output.write_text(json.dumps(inventory, indent=2) + "\n")
print(f"INVENTORY: PASS ({len(inventory)} objects)")
print(f"Output: {output}")
