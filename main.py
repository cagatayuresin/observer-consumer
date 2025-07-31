'''observer‑consumer  –  rules‑folder edition (v1.3.1)

∆ Fix: InfluxDB'ye yazılan batch'lerin gerçekten diske flush edilmemesi
  bazı ortamlarda verinin UI'de görünmemesine yol açıyordu.

  •  write_api'ya error ve success callback'leri eklendi.  ❌ hatalar log'a düşer.
  •  write() çağrısından hemen sonra `write_api.flush()` eklenerek batch
    kuyrukta beklemeden diske yazılıyor (debug modu).  Production'da
    performans için flush satırı kaldırılabilir.
'''

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import traceback
from pathlib import Path
from typing import Any, Dict, List

import yaml
from aio_pika import Message, connect_robust
from aio_pika.abc import AbstractIncomingMessage
from fastapi import FastAPI
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS, WriteOptions
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
load_dotenv()

# ─────────────────────────────── Configuration ─────────────────────────────
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
METRICS_QUEUE = os.getenv("METRICS_QUEUE", "metrics")

INFLUXDB_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUX_ORG", "my-org")
INFLUXDB_BUCKET = os.getenv("INFLUX_BUCKET", "metrics")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
RULES_DIR = (Path(__file__).resolve().parent.parent / "rules").expanduser()
SCAN_INTERVAL = int(os.getenv("RULES_SCAN_INTERVAL", "30"))
# ---------------------------------------------------------------------------

app = FastAPI()

influx_client: InfluxDBClient | None = None
write_api = None
rabbit_conn = None
rules_exchange = None

metrics_buffer: List[Point] = []
RULES: Dict[str, Any] = {"version": 0, "rules": []}

# ──────────────────────────── Helper Functions ─────────────────────────────

def _load_rule_file(fp: Path) -> List[Dict[str, Any]]:
    with fp.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data if isinstance(data, list) else [data]


def build_rules_from_dir() -> Dict[str, Any]:
    RULES_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(RULES_DIR.glob("*.y*ml")) + sorted(RULES_DIR.glob("*.json"))
    rule_list: List[Dict[str, Any]] = []
    md5 = hashlib.md5()
    for fp in files:
        rule_list.extend(_load_rule_file(fp))
        md5.update(fp.name.encode())
        md5.update(str(fp.stat().st_mtime_ns).encode())
    version = int(md5.hexdigest(), 16) & 0xFFFFFFFF
    return {"version": version, "rules": rule_list}


async def publish_rules() -> None:
    await rules_exchange.publish(Message(json.dumps(RULES).encode()), routing_key="")
    print(f"📡  rules published  → version {RULES['version']}")


async def watch_rules_dir() -> None:
    global RULES
    while True:
        try:
            new = build_rules_from_dir()
            if new["version"] != RULES["version"]:
                RULES = new
                await publish_rules()
        except Exception as exc:
            print("⚠️  rules watcher:", exc)
            traceback.print_exc()
        await asyncio.sleep(SCAN_INTERVAL)

# ────────────────────────── Metrics Handler (RabbitMQ) ─────────────────────
async def process_metrics(msg: AbstractIncomingMessage) -> None:
    """
    RabbitMQ'den gelen metrik paketini işler.

    • payload içindeki tüm sayısal alanları `system_metrics` ölçümüne yazar
      (bool → int, nested dict flatten).
    • payload["pod_resources"] listesindeki her öğeyi ayrı `pod_metrics`
      ölçüm satırı olarak ekler.
    """
    print("📥 message arrived – size", len(msg.body))

    async with msg.process():
        try:
            payload = json.loads(msg.body.decode())
            ts_ns   = int(payload["timestamp"] * 1e9)           # nanosecond zaman damgası
            agent_id = payload.get("agent_id", "unknown")

            # ───────────────────────── system_metrics (tek satır) ─────────────────────────
            sys_point = Point("system_metrics").tag("agent_id", agent_id)

            def _add_field(k: str, v: Any):
                # bool → int, yalnızca int/float field yazar
                if isinstance(v, bool):
                    v = int(v)
                if isinstance(v, (int, float)):
                    sys_point.field(k, float(v))

            for k, v in payload.items():
                if k in {"timestamp", "agent_id", "pod_resources"}:
                    continue
                if isinstance(v, dict):
                    # nested dict flatten: pod_status_running = 3
                    for sub_k, sub_v in v.items():
                        _add_field(f"{k}_{sub_k}", sub_v)
                else:
                    _add_field(k, v)

            sys_point.time(ts_ns, WritePrecision.NS)
            metrics_buffer.append(sys_point)

            # ───────────────────────── pod_metrics (çok satır) ───────────────────────────
            for pr in payload.get("pod_resources", []):
                pod_point = (
                    Point("pod_metrics")
                    .tag("agent_id", agent_id)
                    .tag("ns", pr["ns"])
                    .tag("pod_name", pr["pod_name"])
                    .field("cpu_millicores",   float(pr["cpu_millicores"]))
                    .field("cpu_percent",      float(pr["cpu_percent"]))
                    .field("memory_bytes",     float(pr["memory_bytes"]))
                    .field("memory_percent",   float(pr["memory_percent"]))
                    .time(ts_ns, WritePrecision.NS)
                )
                metrics_buffer.append(pod_point)

            # ───────────────────────── batch flush ───────────────────────────────────────
            if len(metrics_buffer) >= BATCH_SIZE:
                to_flush = metrics_buffer.copy()
                metrics_buffer.clear()
                await asyncio.get_running_loop().run_in_executor(
                    None,
                    lambda: write_api.write(INFLUXDB_BUCKET, INFLUXDB_ORG, to_flush),
                )
                write_api.flush()
                print(f"✅ {len(to_flush)} points WRITTEN + FLUSHED")

        except Exception as exc:
            print("⚠️  metric error:", exc)
            traceback.print_exc()
            raise


# ─────────────────────────────── Startup ───────────────────────────────────
@app.on_event("startup")
async def startup() -> None:
    global influx_client, write_api, rabbit_conn, rules_exchange, RULES

    print("🔌 RABBITMQ_URL =", RABBITMQ_URL)
    print("🚀 starting consumer …")

    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

    def _on_success(conf, data):
        print(f"💾 flush OK (batch {len(data)} lines)")

    def _on_error(conf, data, exc):
        print("❌ Influx write error:", exc)
        print("↳ sample line:", (data or "")[:200])

    write_api = influx_client.write_api(
        write_options=WriteOptions(batch_size=BATCH_SIZE, flush_interval=1000),
        success_callback=_on_success,
        error_callback=_on_error,
    )

    rabbit_conn = await connect_robust(RABBITMQ_URL)
    chan = await rabbit_conn.channel()
    await chan.set_qos(prefetch_count=100)
    await chan.declare_queue(METRICS_QUEUE, durable=True)
    queue = await chan.get_queue(METRICS_QUEUE)
    await queue.consume(process_metrics)
    rules_exchange = await chan.declare_exchange("rules", "fanout")

    RULES = build_rules_from_dir()
    await publish_rules()
    asyncio.create_task(watch_rules_dir())
    print("✅ consumer ready – listening on", METRICS_QUEUE)

# ─────────────────────────────── Health ────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "rules_version": RULES["version"], "buffer": len(metrics_buffer)}

# ────────────────────────────── Shutdown ───────────────────────────────────
@app.on_event("shutdown")
async def shutdown() -> None:
    if rabbit_conn:
        await rabbit_conn.close()
    if influx_client:
        influx_client.close()
    print("👋 shutdown complete")
