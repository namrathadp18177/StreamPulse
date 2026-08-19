"""
StreamPulse Dashboard
-----------------------
Streamlit app that tails the "anomalies" Kafka topic (produced by the
PySpark Structured Streaming job) and renders a live-updating view of
per-device windowed stats and flagged anomalies.

Run:
    streamlit run app.py
"""
import json
import os
import threading
import time
from collections import deque

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("SINK_TOPIC", "anomalies")
MAX_ROWS = 500

st.set_page_config(page_title="StreamPulse", layout="wide")
st.title("StreamPulse — Real-Time Streaming Analytics")
st.caption(f"Consuming `{TOPIC}` from `{KAFKA_BOOTSTRAP}`")


@st.cache_resource
def get_buffer():
    """A thread-safe rolling buffer shared across Streamlit reruns."""
    return {"rows": deque(maxlen=MAX_ROWS), "lock": threading.Lock(), "started": False}


def consume_loop(buffer):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="streampulse-dashboard",
    )
    for msg in consumer:
        with buffer["lock"]:
            buffer["rows"].append(msg.value)


buffer = get_buffer()
if not buffer["started"]:
    buffer["started"] = True
    t = threading.Thread(target=consume_loop, args=(buffer,), daemon=True)
    t.start()

placeholder = st.empty()

with buffer["lock"]:
    rows = list(buffer["rows"])

if not rows:
    st.info("Waiting for events... start the producer and streaming job to see live data.")
else:
    df = pd.DataFrame(rows)
    df["window_start"] = pd.to_datetime(df["window_start"])

    col1, col2, col3 = st.columns(3)
    col1.metric("Events in buffer", len(df))
    col2.metric("Active devices", df["device_id"].nunique())
    col3.metric("Anomalies (visible window)", int(df["is_anomalous"].sum()))

    st.subheader("Rolling window mean value per device")
    pivot = df.pivot_table(index="window_start", columns="device_id", values="window_mean", aggfunc="mean")
    st.line_chart(pivot)

    st.subheader("Flagged anomalies")
    anomalies = df[df["is_anomalous"]].sort_values("window_start", ascending=False)
    st.dataframe(anomalies, use_container_width=True)

    st.subheader("Raw stream tail")
    st.dataframe(df.sort_values("window_start", ascending=False).head(50), use_container_width=True)

time.sleep(2)
st.rerun()
