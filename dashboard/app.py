"""Live fleet dashboard: reads the gold/silver Delta tables directly (delta-rs, no Spark needed).
Run:  streamlit run dashboard/app.py   ->  http://localhost:8501
"""
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telemetry.config import TABLES  # noqa: E402

st.set_page_config(page_title="Turbofan Telemetry", layout="wide")


def load(name, columns=None):
    try:
        return DeltaTable(str(TABLES[name])).to_pandas(columns=columns)
    except Exception:
        return pd.DataFrame()


@st.fragment(run_every=10)
def live():
    state = load("engine_state")
    alerts = load("alerts")
    dlq = load("dlq", ["dlq_reason"])
    fleet = load("fleet_minute")
    c = st.columns(5)
    c[0].metric("Engines reporting", len(state))
    c[1].metric("Critical", int((state.alert_level == 2).sum()) if len(state) else 0)
    c[2].metric("Warning", int((state.alert_level == 1).sum()) if len(state) else 0)
    c[3].metric("Alerts raised", len(alerts))
    c[4].metric("Dead-lettered events", len(dlq))

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Fleet health per minute (event time)")
        if len(fleet):
            f = fleet.sort_values("window_start").pivot_table(index="window_start", columns="fleet", values="avg_health_index")
            st.line_chart(f)
        else:
            st.caption("Windows appear once the watermark has passed them (2 minutes after newer events arrive).")
        st.subheader("Engines closest to failure")
        if len(state):
            top = state.sort_values("ewma_health", ascending=False).head(15)
            top["status"] = top.alert_level.map({0: "OK", 1: "WARNING", 2: "CRITICAL"})
            st.dataframe(top[["engine_id", "fleet", "last_cycle", "ewma_health", "status", "s11", "s4"]], hide_index=True, use_container_width=True)
    with right:
        st.subheader("Latest alerts")
        if len(alerts):
            st.dataframe(alerts.sort_values("event_time", ascending=False).head(20)[["engine_id", "level", "cycle", "ewma_health", "event_time"]],
                         hide_index=True, use_container_width=True)
        st.subheader("Dead-letter reasons")
        if len(dlq):
            st.bar_chart(dlq.dlq_reason.value_counts())


st.title("Turbofan Telemetry: fleet health")
st.caption("NASA C-MAPSS engines streamed through Kafka → Spark Structured Streaming → Delta Lake. Refreshes every 10 s.")
live()
