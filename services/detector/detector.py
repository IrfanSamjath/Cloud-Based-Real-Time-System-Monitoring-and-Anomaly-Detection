import json
import time
from collections import deque
import numpy as np
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Configuration
BROKER_URL = "localhost:9092"
TOPIC_NAME = "system-metrics"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-super-secret-auth-token"
INFLUX_ORG = "telemetry-org"
INFLUX_BUCKET = "system-metrics"

# Initialize InfluxDB Client
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Sliding window parameters
WINDOW_SIZE = 30
Z_SCORE_THRESHOLD = 3.0  # Deviation threshold to trigger alert
window = deque(maxlen=WINDOW_SIZE)

def run_detector():
    print(f"Connecting consumer to {BROKER_URL} on topic '{TOPIC_NAME}'...")
    consumer = None

    while not consumer:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[BROKER_URL],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'
            )
            print("Consumer connected. Listening for telemetry...")
        except Exception:
            print("Kafka broker not reachable yet. Retrying in 2 seconds...")
            time.sleep(2)

    try:
        for message in consumer:
            data = message.value
            cpu = data["cpu_usage"]
            latency = data["latency_ms"]
            host = data["host_id"]

            is_anomaly = False
            z_score = 0.0

            # Compute rolling Z-score once enough baseline points are collected
            if len(window) >= 10:
                mean = np.mean(window)
                std = np.std(window)
                
                if std > 0.1:
                    z_score = abs(cpu - mean) / std
                    if z_score > Z_SCORE_THRESHOLD:
                        is_anomaly = True
                        print(f"🚨 [ANOMALY DETECTED] Host: {host} | CPU: {cpu}% | Baseline: {mean:.1f}% | Z-Score: {z_score:.2f}")

            window.append(cpu)

            # Persist record into InfluxDB
            point = (
                Point("server_telemetry")
                .tag("host_id", host)
                .field("cpu_usage", float(cpu))
                .field("latency_ms", float(latency))
                .field("z_score", float(z_score))
                .field("is_anomaly", 1 if is_anomaly else 0)
            )
            write_api.write(bucket=INFLUX_BUCKET, record=point)

    except KeyboardInterrupt:
        print("\nStopping detector...")
        consumer.close()
        client.close()

if __name__ == "__main__":
    run_detector()