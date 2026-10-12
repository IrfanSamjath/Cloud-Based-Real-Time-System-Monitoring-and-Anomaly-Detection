# Cloud-Based Real-Time System Monitoring and Anomaly Detection

An end-to-end, cloud-style telemetry pipeline that streams live server metrics,
detects anomalies in real time using a rolling Z-score algorithm, persists the
results to a time-series database, and visualizes everything on a live Grafana
dashboard.

![Stack](https://img.shields.io/badge/stack-Redpanda%20%7C%20InfluxDB%20%7C%20Grafana-blue)

## Architecture

```
┌──────────────┐     Kafka      ┌───────────────┐    write    ┌───────────┐
│  producer.py │ ─────────────▶ │  detector.py  │ ──────────▶ │ InfluxDB  │
│  (metrics)   │  topic:        │ (Z-score      │   Flux      │ (tsdb)    │
│              │  system-metrics│  detector)    │             │           │
└──────────────┘                └───────────────┘             └─────┬─────┘
                                                                    │ query
                                                             ┌──────▼──────┐
                                                             │   Grafana   │
                                                             │ (dashboard) │
                                                             └─────────────┘
```

| Component | Technology | Role |
|-----------|-----------|------|
| Message broker | Redpanda (Kafka-compatible) | Streams telemetry events |
| Producer | Python + `kafka-python` | Generates synthetic CPU/latency metrics with injected chaos spikes |
| Detector | Python + `numpy` | Consumes the stream, computes a rolling Z-score, flags anomalies |
| Time-series DB | InfluxDB 2.7 (Flux) | Stores metrics + anomaly scores |
| Visualization | Grafana 10.4 | Live dashboard with CPU, latency, Z-score and anomaly panels |

## Project Structure

```
cloud-telemetry-monitor/
├── docker-compose.yml         # Redpanda + InfluxDB + Grafana stack
├── requirements.txt           # Python dependencies
├── grafana-dashboard.json     # Provisioned Grafana dashboard definition
├── services/
│   ├── producer/
│   │   └── producer.py        # Metric generator → Kafka topic
│   └── detector/
│       └── detector.py        # Kafka consumer → Z-score anomaly detector → InfluxDB
└── .vscode/
    └── launch.json            # Run both scripts from VS Code
```

## Prerequisites

- [Docker](https://www.docker.com/) & Docker Compose
- Python 3.10+

## Getting Started

### 1. Start the infrastructure

```bash
docker compose up -d
```

This launches:
- **Redpanda** broker on `localhost:9092`
- **InfluxDB** on `localhost:8086`
- **Grafana** on `localhost:3000`

InfluxDB is auto-provisioned with:
- Org: `telemetry-org`
- Bucket: `system-metrics`
- Admin token: `my-super-secret-auth-token`

### 2. Install Python dependencies

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 3. Run the pipeline

Open two terminals (or use the VS Code **"Run Both"** launch config):

```bash
# Terminal 1 — metric producer
python services/producer/producer.py

# Terminal 2 — anomaly detector
python services/detector/detector.py
```

> **Windows note:** force UTF-8 so the detector's anomaly logging prints correctly:
> `set PYTHONUTF8=1 && set PYTHONIOENCODING=utf-8 && python services/detector/detector.py`

### 4. Open the dashboard

Navigate to <http://localhost:3000> (admin / admin). The dashboard
**"Samcloud Telemetry Monitor"** is provisioned automatically and shows live
CPU usage, latency, Z-score and anomaly counts.

## How Anomaly Detection Works

The detector keeps a sliding window (size 30) of CPU readings and computes a
rolling **Z-score**:

```
z = |cpu − mean(window)| / std(window)
```

A sample is flagged as an anomaly when `z > 3.0`. The producer injects an
artificial spike (CPU ~92–99%, latency 480–920 ms) every 20 samples so the
detection is easy to observe.

## Configuration

All connection settings live at the top of each service script:

| Variable | Default |
|----------|---------|
| `BROKER_URL` | `localhost:9092` |
| `INFLUX_URL` | `http://localhost:8086` |
| `INFLUX_TOKEN` | `my-super-secret-auth-token` |
| `INFLUX_ORG` | `telemetry-org` |
| `INFLUX_BUCKET` | `system-metrics` |

### Secrets

Copy `.env.example` to a local `.env` and fill in real values before going to
production. The `.env` file is git-ignored so credentials never get committed.
Remember to **rotate** the default tokens/passwords shown in the example file.


## Sharing the Dashboard Publicly (optional)

Expose Grafana over HTTPS without a port-forward using a Cloudflare quick tunnel:

```bash
cloudflared tunnel --url http://localhost:3000
```

Anonymous "Viewer" access is enabled in `docker-compose.yml`
(`GF_AUTH_ANONYMOUS_ENABLED=true`), so anyone with the tunnel URL can watch the
live dashboard without logging in.

## License

MIT
