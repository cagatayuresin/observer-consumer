"""
observer-consumer  –  rules-folder edition (v1.2.0)

Görevler:
1. RabbitMQ'dan metrics mesajlarını alır, InfluxDB'ye yazar.
2. ./rules/ klasöründeki YAML|JSON kural dosyalarını tarar:
     • Startup'ta tüm dosyaları birleştirir, version hash'i üretir.
     • Her SCAN_INTERVAL saniyede bir yeniden tarar.
     • Değişiklik algılanırsa yeni kural setini
       RabbitMQ 'rules' (fanout) exchange'i üzerinden agent'lara publish eder.
3. /health endpoint'i mevcut rules_version'ı raporlar.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List

import yaml
from aio_pika import Message, connect_robust, IncomingMessage
from aio_pika.abc import AbstractIncomingMessage
from fastapi import FastAPI
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS

# ──────────────────────────────── Konfig  ───────────────────────────────────
# RabbitMQ
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
METRICS_QUEUE = os.getenv("METRICS_QUEUE", "metrics")

# InfluxDB
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "my-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "metrics")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

# Kural klasörü & tarama periyodu
RULES_DIR = (Path(__file__).resolve().parent.parent / "rules").expanduser()
SCAN_INTERVAL = int(os.getenv("RULES_SCAN_INTERVAL", "30"))  # saniye
# ────────────────────────────────────────────────────────────────────────────

app = FastAPI()

# bağlantılar
influx_client: InfluxDBClient | None = None
write_api = None
rabbit_conn = None
rules_exchange = None

# bellekte buffer & kural seti
metrics_buffer: List[Point] = []
RULES: Dict[str, Any] = {"version": 0, "rules": []}


# ───────────────────────────── Yardımcı Fonksiyonlar ───────────────────────
def _load_rule_file(filepath: Path) -> List[Dict[str, Any]]:
    """YAML/JSON dosyasını liste hâline parse et."""
    with filepath.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data if isinstance(data, list) else [data]


def build_rules_from_dir() -> Dict[str, Any]:
    """
    rules/ altındaki tüm .yaml/.yml/.json dosyalarını okuyup birleştirir.
    Dosya isimleri + mtime değerlerinden 32-bit hash üretip 'version' alanına yazar.
    """
    RULES_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(RULES_DIR.glob("*.y*ml")) + sorted(RULES_DIR.glob("*.json"))
    rule_list: List[Dict[str, Any]] = []
    md5 = hashlib.md5()

    for fp in files:
        rule_list.extend(_load_rule_file(fp))
        md5.update(fp.name.encode())
        md5.update(str(fp.stat().st_mtime_ns).encode())

    version = int(md5.hexdigest(), 16) & 0xFFFFFFFF  # 32-bit
    return {"version": version, "rules": rule_list}


async def publish_rules() -> None:
    """Bellekteki RULES sözlüğünü fan-out exchange üzerinden agent'lara gönder."""
    body = json.dumps(RULES).encode()
    await rules_exchange.publish(Message(body), routing_key="")
    print(f"📡  rules published  → version {RULES['version']}")


async def watch_rules_dir() -> None:
    """Arka planda rules/ klasörünü izleyip değişiklikleri publish eder."""
    global RULES
    while True:
        new_rules = build_rules_from_dir()
        if new_rules["version"] != RULES["version"]:
            RULES = new_rules
            await publish_rules()
        await asyncio.sleep(SCAN_INTERVAL)


# ────────────────────────── Metrics Handler (RabbitMQ) ─────────────────────
async def process_metrics(msg: AbstractIncomingMessage) -> None:
    """metrics_queue'dan gelen mesajı InfluxDB'ye yazar (batch)."""
    async with msg.process():
        payload = json.loads(msg.body.decode())

        ts_ns = int(payload["timestamp"] * 1e9)
        agent_id = payload.get("agent_id", "unknown")

        p = (
            Point("system_metrics")
            .tag("agent_id", agent_id)
            .field("cpu_percent", float(payload.get("cpu_percent", 0.0)))
            .field("memory_percent", float(payload.get("memory_percent", 0.0)))
            .time(ts_ns, WritePrecision.NS)
        )

        metrics_buffer.append(p)
        if len(metrics_buffer) >= BATCH_SIZE:
            flush = metrics_buffer.copy()
            metrics_buffer.clear()
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: write_api.write(
                    bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=flush
                ),
            )


# ─────────────────────────────── Startup ───────────────────────────────────
@app.on_event("startup")
async def startup() -> None:
    global influx_client, write_api, rabbit_conn, rules_exchange, RULES

    print("🚀 starting consumer …")

    # InfluxDB
    influx_client = InfluxDBClient(
        url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG
    )
    write_api = influx_client.write_api(write_options=ASYNCHRONOUS)

    # RabbitMQ
    rabbit_conn = await connect_robust(RABBITMQ_URL)
    chan = await rabbit_conn.channel()
    await chan.set_qos(prefetch_count=100)

    await chan.declare_queue(METRICS_QUEUE, durable=True)
    queue = await chan.get_queue(METRICS_QUEUE)
    await queue.consume(process_metrics)

    # rules exchange
    rules_exchange = await chan.declare_exchange("rules", "fanout")

    # ilk kural yükle & publish
    RULES = build_rules_from_dir()
    await publish_rules()

    # watcher
    asyncio.create_task(watch_rules_dir())

    print("✅ consumer ready")


# ─────────────────────────────── Health ────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "rules_version": RULES["version"],
        "buffer": len(metrics_buffer),
    }


# ────────────────────────────── Shutdown ───────────────────────────────────
@app.on_event("shutdown")
async def shutdown() -> None:
    if rabbit_conn:
        await rabbit_conn.close()
    if influx_client:
        influx_client.close()
