import json
import time
import random
from kafka import KafkaProducer

BROKER_URL = 'localhost:9092'
TOPIC_NAME = 'system-metrics'

def run_producer():
    print(f"Connecting producer to Redpanda/Kafka broker at {BROKER_URL}...")
    producer = None
    
    # Retry connection until broker is fully up
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BROKER_URL],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to broker.")
        except Exception:
            print("Broker not ready yet. Retrying in 2 seconds...")
            time.sleep(2)

    step = 0
    print("Streaming system telemetry metrics...")

    try:
        while True:
            step += 1
            # Normal baseline metrics
            cpu_load = random.uniform(30.0, 45.0)
            latency_ms = random.uniform(45.0, 75.0)

            # Injected anomaly spike every 20 steps
            if step % 20 == 0:
                print("\n[CHAOS INJECTION] >>> Triggering abnormal resource spike <<<")
                cpu_load = random.uniform(92.0, 99.8)
                latency_ms = random.uniform(480.0, 920.0)

            payload = {
                "timestamp": time.time(),
                "host_id": "prod-node-01",
                "cpu_usage": round(cpu_load, 2),
                "latency_ms": round(latency_ms, 2)
            }

            producer.send(TOPIC_NAME, payload)
            producer.flush()
            print(f"[METRIC SENT] Host: {payload['host_id']} | CPU: {payload['cpu_usage']}% | Latency: {payload['latency_ms']}ms")
            time.sleep(1.0)
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.close()

if __name__ == "__main__":
    run_producer()