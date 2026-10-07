FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install \
    "duckdb==1.4.1" \
    "boto3==1.40.38" \
    "jsonschema==4.25.1"

RUN useradd --create-home --uid 1000 --shell /usr/sbin/nologin curator \
    && mkdir -p /work \
    && chown -R curator:curator /work

WORKDIR /work
USER 1000:1000

CMD ["sleep", "infinity"]
