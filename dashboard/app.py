"""
dashboard/app.py
----------------
Streamlit dashboard — auto-refreshes every 30 s.
Shows live price feed, anomaly alerts table, and per-ticker charts.
"""

import time
import psycopg2
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Stock Anomaly Monitor",
    page_icon="📈",
    layout="wide",
)

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "stockdb",
    "user":     "postgres",
    "password": "postgres",
}

REFRESH_INTERVAL = 30  # seconds


# ── Data helpers ──────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def load_data(ticker: str | None = None, limit: int = 500) -> pd.DataFrame:
    conn = get_conn()
    where = f"WHERE ticker = '{ticker}'" if ticker else ""
    sql = f"""
        SELECT ticker, ts, close, pct_return, anomaly, score
        FROM price_events
        {where}
        ORDER BY ts DESC
        LIMIT {limit}
    """
    df = pd.read_sql(sql, conn)
    df["ts"] = pd.to_datetime(df["ts"])
    return df.sort_values("ts")


def load_anomalies(limit: int = 50) -> pd.DataFrame:
    conn = get_conn()
    sql = f"""
        SELECT ticker, ts, close, pct_return, score
        FROM price_events
        WHERE anomaly = TRUE
        ORDER BY ts DESC
        LIMIT {limit}
    """
    df = pd.read_sql(sql, conn)
    df["ts"] = pd.to_datetime(df["ts"])
    return df


# ── Layout ─────────────────────────────────────────────────────────────────────
st.title("📈 Real-Time Stock Anomaly Detection")
st.caption("Powered by Apache Kafka + Isolation Forest • Auto-refreshes every 30 s")

# Sidebar
st.sidebar.header("Settings")
tickers_available = ["ALL", "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "JPM"]
selected = st.sidebar.selectbox("Ticker", tickers_available)
lookback = st.sidebar.slider("Data points to show", 50, 500, 200, step=50)
auto_refresh = st.sidebar.checkbox("Auto refresh", value=True)

# ── KPI row ───────────────────────────────────────────────────────────────────
ticker_filter = None if selected == "ALL" else selected
df = load_data(ticker_filter, lookback)
anomalies = load_anomalies()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Events", f"{len(df):,}")
col2.metric("Anomalies Detected", f"{df['anomaly'].sum():,}")
col3.metric(
    "Anomaly Rate",
    f"{df['anomaly'].mean()*100:.1f}%" if len(df) > 0 else "—"
)
col4.metric(
    "Latest Price" if ticker_filter else "Tickers Monitored",
    f"${df['close'].iloc[-1]:.2f}" if ticker_filter and len(df) > 0
    else str(df["ticker"].nunique())
)

st.divider()

# ── Price + anomaly chart ─────────────────────────────────────────────────────
st.subheader("Price Feed with Anomaly Flags")

if len(df) == 0:
    st.info("No data yet — make sure the producer and detector are running.")
else:
    tickers_plot = [ticker_filter] if ticker_filter else df["ticker"].unique().tolist()

    for tkr in tickers_plot[:4]:   # cap at 4 charts to avoid overflow
        sub = df[df["ticker"] == tkr] if "ticker" in df.columns else df
        if len(sub) < 2:
            continue

        normal  = sub[sub["anomaly"] == False]
        anomaly = sub[sub["anomaly"] == True]

        fig = make_subplots(
            rows=2, cols=1,
            row_heights=[0.7, 0.3],
            shared_xaxes=True,
            vertical_spacing=0.05,
        )

        # Price line
        fig.add_trace(go.Scatter(
            x=normal["ts"], y=normal["close"],
            mode="lines", name="Price", line=dict(color="#2196F3", width=1.5)
        ), row=1, col=1)

        # Anomaly markers
        if len(anomaly):
            fig.add_trace(go.Scatter(
                x=anomaly["ts"], y=anomaly["close"],
                mode="markers", name="Anomaly",
                marker=dict(color="red", size=10, symbol="x")
            ), row=1, col=1)

        # Return bars
        colors = ["red" if a else "#90CAF9" for a in sub["anomaly"]]
        fig.add_trace(go.Bar(
            x=sub["ts"], y=sub["pct_return"] * 100,
            name="Return %", marker_color=colors, showlegend=False
        ), row=2, col=1)

        fig.update_layout(
            title=tkr, height=420,
            margin=dict(l=0, r=0, t=40, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        fig.update_yaxes(title_text="Price ($)", row=1, col=1)
        fig.update_yaxes(title_text="Return %", row=2, col=1)
        st.plotly_chart(fig, use_container_width=True)

# ── Anomaly alerts table ───────────────────────────────────────────────────────
st.subheader("🚨 Recent Anomaly Alerts")
if len(anomalies) == 0:
    st.success("No anomalies detected yet.")
else:
    display = anomalies.copy()
    display["pct_return"] = (display["pct_return"] * 100).round(2).astype(str) + "%"
    display["score"]      = display["score"].round(4)
    display.columns       = ["Ticker", "Timestamp", "Close ($)", "Return", "Anomaly Score"]
    st.dataframe(display, use_container_width=True, hide_index=True)

# ── Score distribution ─────────────────────────────────────────────────────────
if len(df) > 10:
    st.subheader("Anomaly Score Distribution")
    fig2 = go.Figure()
    fig2.add_trace(go.Histogram(
        x=df["score"], nbinsx=40,
        marker_color="#2196F3", opacity=0.75, name="All points"
    ))
    if df["anomaly"].any():
        fig2.add_trace(go.Histogram(
            x=df[df["anomaly"]]["score"], nbinsx=20,
            marker_color="red", opacity=0.75, name="Anomalies"
        ))
    fig2.update_layout(barmode="overlay", height=300, margin=dict(t=20))
    fig2.update_xaxes(title="Score (lower = more anomalous)")
    fig2.update_yaxes(title="Count")
    st.plotly_chart(fig2, use_container_width=True)

# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(REFRESH_INTERVAL)
    st.rerun()
