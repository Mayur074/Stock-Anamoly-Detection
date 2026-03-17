# 📈 Real-Time Stock Market Anomaly Detection

A production-style streaming pipeline that detects price anomalies in real time across 8 S&P 500 stocks using **Apache Kafka**, **Isolation Forest**, and a live **Streamlit dashboard**.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-black)
![Scikit-learn](https://img.shields.io/badge/Scikit--learn-1.4-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-1.35-red)

---

## 🏗️ Architecture

```
yfinance / Live Feed
        │
        ▼
  Kafka Producer  ──────►  Kafka Topic: stock-prices
                                    │
                                    ▼
                          Kafka Consumer (Detector)
                           - Rolling window (50 pts)
                           - Feature engineering
                           - Isolation Forest (re-fit per tick)
                                    │
                                    ▼
                             PostgreSQL DB
                                    │
                                    ▼
                          Streamlit Dashboard
                           - Live price charts
                           - Anomaly alert table
                           - Score distribution
```

## 🚀 Quickstart

### 1. Clone and install
```bash
git clone https://github.com/yourgithub/stock-anomaly-detection.git
cd stock-anomaly-detection
pip install -r requirements.txt
```

### 2. Start infrastructure (Kafka + PostgreSQL)
```bash
cd docker
docker-compose up -d
```
Wait ~20 seconds for services to be ready.

### 3. Start the producer (terminal 1)
```bash
python src/producer.py
```
Streams 1-minute OHLCV bars for 8 tickers. Injects random ±8–12% spikes ~5% of the time for demo purposes.

### 4. Start the detector (terminal 2)
```bash
python src/detector.py
```
Consumes from Kafka, maintains per-ticker rolling windows, re-fits Isolation Forest on each tick, and writes results to PostgreSQL.

### 5. Launch the dashboard (terminal 3)
```bash
streamlit run dashboard/app.py
```
Open http://localhost:8501

---

## 🔬 How It Works

### Feature Engineering
For each incoming price tick, three features are computed from a 50-point rolling window:
- **Percentage return** — `(close_t - close_{t-1}) / close_{t-1}`
- **Absolute return** — magnitude of the move
- **Rolling Z-score** — how many standard deviations from the window mean

### Isolation Forest
- Unsupervised algorithm — no labeled anomaly data required
- Works by randomly partitioning features; anomalies are isolated faster (shorter path length)
- `contamination=0.05` → expects ~5% of points to be anomalous
- Model is re-fit on every new tick using the full rolling window

### PostgreSQL Schema
```sql
CREATE TABLE price_events (
    id          SERIAL PRIMARY KEY,
    ticker      TEXT        NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    close       FLOAT       NOT NULL,
    pct_return  FLOAT,
    anomaly     BOOLEAN     DEFAULT FALSE,
    score       FLOAT,           -- lower = more anomalous
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
```

---

## 📊 Dashboard Features

| Panel | Description |
|---|---|
| KPI Cards | Total events, anomaly count, anomaly rate, latest price |
| Price Chart | Close price with red ✕ markers on detected anomalies |
| Return Bars | Per-tick % return, red for anomalous ticks |
| Alert Table | Last 50 anomaly events with score and return |
| Score Distribution | Histogram overlay of normal vs anomalous scores |

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data Ingestion | Apache Kafka 7.5 (Confluent) |
| Data Source | yfinance (swap for Alpaca/Polygon in production) |
| ML Model | Isolation Forest (scikit-learn) |
| Storage | PostgreSQL 15 |
| Dashboard | Streamlit + Plotly |
| Infrastructure | Docker Compose |

---

## 📁 Project Structure

```
stock-anomaly-detection/
├── src/
│   ├── producer.py       # Kafka producer: streams stock prices
│   └── detector.py       # Kafka consumer: Isolation Forest anomaly detection
├── dashboard/
│   └── app.py            # Streamlit live dashboard
├── docker/
│   └── docker-compose.yml  # Kafka + Zookeeper + PostgreSQL
├── requirements.txt
└── README.md
```

---

## 🔧 Configuration

All config constants are at the top of each file:

| File | Variable | Default | Description |
|---|---|---|---|
| `producer.py` | `TICKERS` | 8 stocks | Tickers to stream |
| `detector.py` | `WINDOW_SIZE` | 50 | Rolling window length |
| `detector.py` | `CONTAMINATION` | 0.05 | Expected anomaly rate |
| Both | `KAFKA_BROKER` | localhost:9092 | Kafka address |

---

## 📌 Notes

- This project uses `yfinance` for free data during development. For real-time production use, replace the producer with a WebSocket connection to Alpaca Markets or Polygon.io.
- The Isolation Forest model is intentionally re-fit on every tick to simulate online learning. For higher throughput, use incremental updates or a separate retraining schedule.

---

## 👤 Author

**Mayur Sangle** — M.S. Data Science, University of Maryland College Park  
[LinkedIn](https://linkedin.com/in/mayur-sangle-04m) • [GitHub](https://github.com/Mayur074) • [Email](msangle074@gmail.com)
