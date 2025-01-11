"""
producer.py
-----------
Simulates a real-time stock price feed by streaming OHLCV data
into a Kafka topic. In production, replace yfinance with a live
broker WebSocket (Alpaca, Polygon.io, etc.).
"""

import json
import time
import random
import logging
from datetime import datetime

import yfinance as yf
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PRODUCER] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = "localhost:9092"
TOPIC = "stock-prices"

TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "JPM"]


def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def fetch_latest_price(ticker: str) -> dict | None:
    """
    Fetch the latest 1-minute bar for a ticker via yfinance.
    Injects random spikes occasionally to simulate anomalies.
    """
    try:
        df = yf.download(ticker, period="1d", interval="1m", progress=False)
        if df.empty:
            return None
        row = df.iloc[-1]

        price = float(row["Close"].iloc[0]) if hasattr(row["Close"], "iloc") else float(row["Close"])

        # Inject artificial anomaly ~5% of the time for demo purposes
        if random.random() < 0.05:
            spike = random.choice([0.08, 0.10, 0.12, -0.08, -0.10])
            price = price * (1 + spike)
            logger.info(f"  [INJECTED ANOMALY] {ticker} spike={spike:+.0%}")

        return {
            "ticker": ticker,
            "timestamp": datetime.utcnow().isoformat(),
            "open":  float(row["Open"].iloc[0])  if hasattr(row["Open"],  "iloc") else float(row["Open"]),
            "high":  float(row["High"].iloc[0])  if hasattr(row["High"],  "iloc") else float(row["High"]),
            "low":   float(row["Low"].iloc[0])   if hasattr(row["Low"],   "iloc") else float(row["Low"]),
            "close": price,
            "volume":float(row["Volume"].iloc[0])if hasattr(row["Volume"],"iloc") else float(row["Volume"]),
        }
    except Exception as e:
        logger.error(f"Failed to fetch {ticker}: {e}")
        return None


def run():
    producer = get_producer()
    logger.info(f"Producer started → topic: {TOPIC}")

    while True:
        for ticker in TICKERS:
            record = fetch_latest_price(ticker)
            if record:
                producer.send(TOPIC, value=record)
                logger.info(f"Sent {ticker} close={record['close']:.2f}")
        producer.flush()
        time.sleep(60)   # emit once per minute per ticker


if __name__ == "__main__":
    run()
