
import asyncio, json, time
from aio_pika import connect_robust, Message

async def publish_metrics():
    try:
        # 1. Connect to RabbitMQ
        connection = await connect_robust("amqp://guest:guest@localhost:5672/")  # ← DEĞIŞIKLIK: Credentials düzeltildi
        channel = await connection.channel()
        await channel.declare_queue("metrics_queue", durable=True)  # ← DEĞIŞIKLIK: Queue ismi tutarlı
        
        # ← DEĞIŞIKLIK: Test için birkaç mesaj gönder
        for i in range(5):
            payload = {
                "agent_id": f"test-agent-{i}",
                "agent_version": "1.0",
                "timestamp": time.time(),
                "cpu_percent": 55.5 + i * 5,  # Farklı değerler
                "memory_percent": 40.0 + i * 3,
                "disk_percent": 70.2 + i * 2,
                "uptime_seconds": 12345 + i * 100,
                "kubernetes_available": i % 2 == 0,  # Bazıları true, bazıları false
                "pod_count": i * 2
            }
            
            message = Message(body=json.dumps(payload).encode(), delivery_mode=2)
            await channel.default_exchange.publish(message, routing_key="metrics_queue")
            print(f"✅ Test mesajı gönderildi: {payload['agent_id']} - CPU: {payload['cpu_percent']}%")
            
            await asyncio.sleep(1)  # 1 saniye bekle
            
        await connection.close()
        print("✅ Tüm test mesajları gönderildi")
        
    except Exception as e:
        print(f"❌ Producer hatası: {e}")

if __name__ == "__main__":
    asyncio.run(publish_metrics())