import os
import json
import hashlib
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

def now():
    return datetime.now(timezone.utc).isoformat()

client = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)

result = {
    "test_id": "I04",
    "timestamp_utc": now(),
    "store_id": os.environ.get("STORE_ID"),
    "raw_get": {},
    "release_put": {},
}

# Test 1: Raw object must be readable.
bucket = "research-raw"
key = "lab2/inputs/batch-01/observations_a.csv"

try:
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    result["raw_get"] = {
        "bucket": bucket,
        "key": key,
        "result": "PASS" if len(body) > 0 else "FAIL",
        "http_status": response.get("ResponseMetadata", {}).get("HTTPStatusCode"),
        "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }
except ClientError as e:
    result["raw_get"] = {
        "bucket": bucket,
        "key": key,
        "result": "FAIL",
        "error_code": e.response.get("Error", {}).get("Code"),
        "http_status": e.response.get("ResponseMetadata", {}).get("HTTPStatusCode"),
    }

# Test 2: Release write must be denied.
# Use a unique key so this test never overwrites an existing object.
release_bucket = "research-release"
release_key = (
    "lab2/task1-access-test/should-not-exist-"
    + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    + ".txt"
)

try:
    response = client.put_object(
        Bucket=release_bucket,
        Key=release_key,
        Body=b"I04 permission test; remove if unexpectedly created.",
        ContentType="text/plain",
    )
    result["release_put"] = {
        "bucket": release_bucket,
        "key": release_key,
        "result": "FAIL",
        "http_status": response.get("ResponseMetadata", {}).get("HTTPStatusCode"),
        "note": "Write unexpectedly succeeded; do not retry or delete with curator credentials.",
    }
except ClientError as e:
    code = e.response.get("Error", {}).get("Code", "")
    status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    denied = status == 403 or code in ("AccessDenied", "403")
    result["release_put"] = {
        "bucket": release_bucket,
        "key": release_key,
        "result": "PASS" if denied else "FAIL",
        "error_code": code,
        "http_status": status,
    }

result["status"] = (
    "PASS"
    if result["raw_get"].get("result") == "PASS"
    and result["release_put"].get("result") == "PASS"
    else "FAIL"
)

print(json.dumps(result, indent=2))
