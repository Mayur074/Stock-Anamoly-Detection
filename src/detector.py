"""
detector.py
-----------
Kafka consumer that maintains a rolling window of price returns
per ticker and runs Isolation Forest to flag anomalies in real time.
Persists results to PostgreSQL for the dashboard to read.
"""

import json
import logging
from collections import defaultdict, deque
from datetime import datetime

import numpy as np
import psycopg2
from kafka import KafkaConsumer
from sklearn.ensemble import IsolationForest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [DETECTOR] %(message)s")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
TOPIC          = "stock-prices"
GROUP_ID       = "anomaly-detector"

WINDOW_SIZE    = 50       # minimum data points before model fires
CONTAMINATION  = 0.05     # expected anomaly rate (5%)
ANOMALY_LABEL  = -1

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "stockdb",
    "user":     "postgres",
    "password": "postgres",
}

# ── Database helpers ──────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS price_events (
    id          SERIAL PRIMARY KEY,
    ticker      TEXT        NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    close       FLOAT       NOT NULL,
    pct_return  FLOAT,
    anomaly     BOOLEAN     DEFAULT FALSE,
    score       FLOAT,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
"""

INSERT_SQL = """
INSERT INTO price_events (ticker, ts, close, pct_return, anomaly, score)
VALUES (%s, %s, %s, %s, %s, %s);
"""


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    return conn


def save_event(conn, ticker, ts, close, pct_return, is_anomaly, score):
    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, (ticker, ts, close, pct_return, is_anomaly, score))


# ── Feature engineering ───────────────────────────────────────────────────────
def build_features(window: deque) -> np.ndarray:
    """
    Constructs a feature matrix from a rolling window of close prices.
    Features: [pct_return, abs_return, rolling_zscore]
    """
    prices  = np.array([p for p in window], dtype=float)
    returns = np.diff(prices) / prices[:-1]         # percentage returns
    if len(returns) < 2:
        return None

    abs_ret  = np.abs(returns)
    mean_ret = np.mean(returns)
    std_ret  = np.std(returns) + 1e-9
    zscores  = (returns - mean_ret) / std_ret

    # Each row = one time step; last row = most recent
    features = np.column_stack([returns, abs_ret, zscores])
    return features


# ── Per-ticker state ──────────────────────────────────────────────────────────
windows = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
models  = {}


def update_model(ticker: str, window: deque) -> tuple[bool, float] | None:
    """
    Re-fits Isolation Forest on the full window and scores the latest point.
    Returns (is_anomaly, anomaly_score) or None if window is too small.
    """
    features = build_features(window)
    if features is None or len(features) < WINDOW_SIZE - 1:
        return None

    model = IsolationForest(
        n_estimators=100,
        contamination=CONTAMINATION,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(features)
    models[ticker] = model

    # Score only the most recent data point
    latest   = features[-1].reshape(1, -1)
    label    = model.predict(latest)[0]          # -1 = anomaly, 1 = normal
    score    = model.score_samples(latest)[0]    # lower = more anomalous

    is_anomaly = label == ANOMALY_LABEL
    return is_anomaly, float(score)


# ── Main consumer loop ────────────────────────────────────────────────────────
def run():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    conn = get_db()
    logger.info("Detector started — waiting for messages …")

    for msg in consumer:
        record  = msg.value
        ticker  = record["ticker"]
        close   = record["close"]
        ts      = record["timestamp"]

        window  = windows[ticker]
        prev    = window[-1] if window else None
        window.append(close)

        pct_return = ((close - prev) / prev) if prev else 0.0
        result     = update_model(ticker, window)

        if result is None:
            logger.info(f"{ticker}: warming up ({len(window)}/{WINDOW_SIZE})")
            save_event(conn, ticker, ts, close, pct_return, False, 0.0)
            continue

        is_anomaly, score = result
        save_event(conn, ticker, ts, close, pct_return, is_anomaly, score)

        tag = "🚨 ANOMALY" if is_anomaly else "  normal  "
        logger.info(f"{tag} | {ticker} | close={close:.2f} | return={pct_return:+.3%} | score={score:.4f}")


if __name__ == "__main__":
    run()
