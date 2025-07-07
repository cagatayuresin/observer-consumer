
from fastapi import FastAPI
from aio_pika import connect_robust, IncomingMessage, Message
import os
import json
import time
import asyncio
from influxdb_client import InfluxDBClient, Point, WritePrecision
from dotenv import load_dotenv  # ← EKLENDI

# .env dosyasını yükle
load_dotenv()  # ← EKLENDI

app = FastAPI()

# Environment variables
RABBITMQ_URL    = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
METRICS_QUEUE   = os.getenv("METRICS_QUEUE", "metrics_queue")
INFLUXDB_URL    = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN", "")  # ← BOŞ OLMASIN!
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG", "my-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "metrics")

print(f"🔑 InfluxDB Config:")
print(f"   URL: {INFLUXDB_URL}")
print(f"   ORG: {INFLUXDB_ORG}")
print(f"   BUCKET: {INFLUXDB_BUCKET}")
print(f"   TOKEN: {'✅ Set' if INFLUXDB_TOKEN else '❌ Missing'}")

# Global değişkenler
influx_client = None
write_api = None
connection = None

@app.on_event("startup")
async def startup_event():
    global influx_client, write_api, connection
    
    try:
        # 1. Connect to InfluxDB v2 with token auth
        influx_client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG
        )
        write_api = influx_client.write_api()  # ← DEĞIŞIKLIK: Sync write kullan
        
        print(f"✅ InfluxDB bağlantısı kuruldu: {INFLUXDB_URL}")
        
        # 2. Connect to RabbitMQ and declare queue
        connection = await connect_robust(RABBITMQ_URL)
        channel = await connection.channel()
        queue = await channel.declare_queue(METRICS_QUEUE, durable=True)
        
        print(f"✅ RabbitMQ bağlantısı kuruldu: {RABBITMQ_URL}")
        print(f"✅ Queue dinleniyor: {METRICS_QUEUE}")
        
        # 3. Start consuming
        await queue.consume(process_message)
        
        # 4. Store connections on app state for shutdown
        app.state.influx_client = influx_client
        app.state.write_api = write_api
        app.state.rabbitmq_connection = connection
        
    except Exception as e:
        print(f"❌ Startup hatası: {e}")
        raise

async def process_message(message: IncomingMessage):
    # ← DEĞIŞIKLIK: write_api parametresi kaldırıldı, global kullanılıyor
    global write_api
    
    async with message.process():
        try:
            payload = json.loads(message.body.decode("utf-8"))
            print(f"📨 Mesaj alındı: {payload}")  # ← DEĞIŞIKLIK: Debug log eklendi
            
            # ← DEĞIŞIKLIK: Timestamp kontrolü eklendi
            timestamp = payload.get("timestamp", time.time())
            if timestamp > time.time() + 3600:  # 1 saat ileriye izin verme
                timestamp = time.time()
            
            point = (
                Point("system_metrics")
                .tag("agent_id", payload.get("agent_id", "unknown"))
                .tag("agent_version", payload.get("agent_version", "unknown"))
                .field("cpu_percent", float(payload.get("cpu_percent", 0.0)))
                .field("memory_percent", float(payload.get("memory_percent", 0.0)))
                .field("disk_percent", float(payload.get("disk_percent", 0.0)))
                .field("uptime_seconds", float(payload.get("uptime_seconds", 0.0)))
                .field("kubernetes_available", bool(payload.get("kubernetes_available", False)))
                .field("pod_count", int(payload.get("pod_count", 0)))
                .time(int(timestamp * 1e9), WritePrecision.NS)  # ← DEĞIŞIKLIK: Timestamp değişkeni kullanıldı
            )
            
            # ← DEĞIŞIKLIK: Sync write kullan (await kaldırıldı)
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            print(f"✅ InfluxDB'ye yazıldı: {payload.get('agent_id')} - CPU: {payload.get('cpu_percent')}%")
            
        except Exception as e:
            print(f"❌ Mesaj işleme hatası: {e}")
            print(f"❌ Payload: {message.body.decode('utf-8')}")

@app.on_event("shutdown")
async def shutdown_event():
    try:
        if connection:
            await connection.close()
            print("✅ RabbitMQ bağlantısı kapatıldı")
        if influx_client:
            influx_client.close()
            print("✅ InfluxDB bağlantısı kapatıldı")
    except Exception as e:
        print(f"❌ Shutdown hatası: {e}")

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": time.time()}

# ← DEĞIŞIKLIK: Debug endpoint eklendi
@app.get("/debug")
async def debug():
    return {
        "rabbitmq_url": RABBITMQ_URL,
        "metrics_queue": METRICS_QUEUE,
        "influxdb_url": INFLUXDB_URL,
        "influxdb_bucket": INFLUXDB_BUCKET,
        "influxdb_org": INFLUXDB_ORG,
        "connections": {
            "rabbitmq": connection is not None,
            "influxdb": influx_client is not None
        }
    }

