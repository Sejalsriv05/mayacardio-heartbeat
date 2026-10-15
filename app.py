from pathlib import Path
import json
import os
import sqlite3
import sys
from datetime import datetime

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

st.set_page_config(
    page_title="MayaCardio | Clinical Telemetry",
    page_icon="♥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------- Design system ----------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;600;700&family=Manrope:wght@400;500;600;700;800&display=swap');

:root {
  --bg: #080e16; --panel: #0e1723; --border: #1e2b3b;
  --text: #e8f0f8; --muted: #8394a8; --teal: #42e3c2;
}
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.stApp { background: var(--bg); color: var(--text); }
[data-testid="stHeader"] { background: rgba(8,14,22,.92); }
[data-testid="stSidebar"] {
  background: #0b131e; border-right: 1px solid var(--border);
}
[data-testid="stSidebar"] > div:first-child { padding-top: 1.5rem; }
.block-container { padding-top: 2rem; padding-bottom: 3rem; max-width: 1600px; }
h1,h2,h3 { font-family: 'Manrope', sans-serif !important; letter-spacing: -.5px; }
h1 { font-weight: 800 !important; font-size: 2rem !important; }
h2 { font-size: 1.25rem !important; }
p, label { color: var(--text); }
small, .muted { color: var(--muted); }
.brand { font-family: 'Manrope',sans-serif; font-size: 1.35rem; font-weight: 800; letter-spacing: -.7px; }
.brand span { color: var(--teal); }
.eyebrow { color: var(--teal); text-transform: uppercase; font: 500 10px 'DM Mono',monospace; letter-spacing: 2px; }
.page-sub { color: #91a2b6; font-size: 14px; margin-top: -9px; margin-bottom: 24px; }
.metric {
  background: linear-gradient(145deg,#111d2b,#0d1621);
  border: 1px solid var(--border); border-radius: 13px;
  padding: 18px 19px; min-height: 124px;
}
.metric-label { font-size: 11px; color: #91a2b6; text-transform: uppercase; letter-spacing: 1.1px; }
.metric-value { font: 700 28px 'Manrope',sans-serif; color: #f2f7fc; margin: 12px 0 5px; }
.metric-note { font-size: 11px; color: #8295a9; }
.panel {
  background: #0d1621; border: 1px solid var(--border);
  border-radius: 14px; padding: 20px 22px; margin-bottom: 16px;
}
.panel-title { font: 700 14px 'Manrope',sans-serif; margin-bottom: 3px; }
.panel-sub { color: #8295a9; font-size: 12px; margin-bottom: 14px; }
.pill {
  display: inline-block; border: 1px solid #245b55; color: #66e9ce;
  background: #102a2a; padding: 5px 9px; border-radius: 6px;
  font: 10px 'DM Mono',monospace; letter-spacing: .5px;
}
.pill-muted { border-color: #344254; color: #a5b3c3; background: #17212e; }
hr { border-color: #1e2b3b !important; }
div[data-testid="stDataFrame"] { border: 1px solid var(--border); border-radius: 10px; }
.stButton button, .stDownloadButton button {
  border-radius: 8px; border: 1px solid #2a645c;
  background: #102b2b; color: #8df4dd; font-weight: 600;
}
.stButton button:hover, .stDownloadButton button:hover {
  border-color: #42e3c2; color: white; background: #163b38;
}
div[data-testid="stAlert"] { border-radius: 10px; }
footer { visibility: hidden; }
#MainMenu { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


def metric(label, value, note):
    st.markdown(
        f'<div class="metric"><div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div>'
        f'<div class="metric-note">{note}</div></div>',
        unsafe_allow_html=True,
    )


def panel_heading(title, subtitle=""):
    st.markdown(
        f'<div class="panel-title">{title}</div>'
        f'<div class="panel-sub">{subtitle}</div>',
        unsafe_allow_html=True,
    )


def load_database(path):
    if not path.exists():
        return None, "Database file not found."
    try:
        con = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True, timeout=3)
        con.row_factory = sqlite3.Row
        con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        return con, None
    except sqlite3.Error as exc:
        return None, str(exc)


def get_table_names(con):
    return {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}


def get_sessions(con):
    tables = get_table_names(con)
    if "samples" not in tables:
        return []
    return [r[0] for r in con.execute(
        "SELECT session_id FROM samples GROUP BY session_id ORDER BY MAX(sample_index) DESC"
    ).fetchall()]


def read_session(con, session):
    samples, losses = [], []
    tables = get_table_names(con)
    if "samples" in tables:
        samples = con.execute("""
            SELECT session_id, sensor, sample_index, values_json, synced
            FROM samples WHERE session_id = ?
            ORDER BY sensor, sample_index
        """, (session,)).fetchall()
    if "losses" in tables:
        losses = con.execute("""
            SELECT sensor, from_index, to_index, reason
            FROM losses WHERE session_id = ?
            ORDER BY sensor, from_index
        """, (session,)).fetchall()
    return samples, losses


def sample_frame(rows):
    records = []
    for row in rows:
        try:
            values = json.loads(row["values_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        records.append({
            "sensor": int(row["sensor"]),
            "index": int(row["sample_index"]),
            "time_s": int(row["sample_index"]) / 400,
            "values": values,
            "synced": int(row["synced"]),
        })
    return pd.DataFrame(records)


def export_jsonl(rows, losses, cloud_only=False):
    output = []
    for row in rows:
        if cloud_only and not int(row["synced"]):
            continue
        try:
            values = json.loads(row["values_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        output.append(json.dumps({
            "sensor": int(row["sensor"]),
            "index": int(row["sample_index"]),
            "values": values,
        }))
    for loss in losses:
        output.append(json.dumps({
            "sensor": int(loss["sensor"]),
            "from_index": int(loss["from_index"]),
            "to_index": loss["to_index"],
            "reason": loss["reason"],
        }))
    return "\n".join(output) + ("\n" if output else "")


# ---------- Sidebar ----------
with st.sidebar:
    st.markdown(
        '<div class="brand">Maya<span>Cardio</span></div>'
        '<div class="eyebrow" style="margin-top:6px">HEARTBEAT INTELLIGENCE</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")
    page = st.radio(
        "WORKSPACE",
        ["Overview", "Signal analysis", "Reliability center", "Session explorer"],
        label_visibility="visible",
    )
    st.markdown("---")
    st.markdown('<div class="eyebrow">DATA SOURCE</div>', unsafe_allow_html=True)
    default_db = os.environ.get("HEARTBEAT_DB", str(ROOT / "heartbeat.db"))
    db_text = st.text_input("SQLite database path", value=default_db)
    refresh = st.button("↻  Refresh data", use_container_width=True)
    if refresh:
        st.rerun()
    st.markdown("---")
    st.markdown('<div class="eyebrow">SYSTEM STATUS</div>', unsafe_allow_html=True)
    st.markdown(
        '<span class="pill pill-muted">LOCAL DATABASE VIEW</span>',
        unsafe_allow_html=True,
    )
    st.caption("Read-only inspection · No sensor connection is assumed.")
    st.markdown("---")
    st.caption("MayaCardio · Systems Engineering Assessment")
    st.caption("Synthetic signals only when explicitly in demo mode.")


# ---------- Load actual stored data ----------
db_path = Path(db_text).expanduser()
con, db_error = load_database(db_path)
if con is None:
    st.markdown('<div class="eyebrow">TELEMETRY PLATFORM / LOCAL</div>', unsafe_allow_html=True)
    st.title("MayaCardio")
    st.markdown('<div class="page-sub">Resilient heartbeat telemetry and signal intelligence.</div>', unsafe_allow_html=True)
    st.error(f"Unable to read database: {db_error}")
    st.info("Check the database path in the sidebar. The default is heartbeat.db in the project root.")
    st.stop()

try:
    sessions = get_sessions(con)
    selected_session = None
    samples, losses = [], []
    if sessions:
        with st.sidebar:
            selected_session = st.selectbox("SESSION", sessions)
        samples, losses = read_session(con, selected_session)

    df = sample_frame(samples)
    total = len(df)
    cloud_count = int(df["synced"].sum()) if total else 0
    backlog = total - cloud_count
    loss_count = len(losses)
    max_index = int(df["index"].max()) if total else None
    duration = (max_index + 1) / 400 if max_index is not None else 0
    sensor1 = df[df["sensor"] == 1].copy() if total else pd.DataFrame()
    sensor2 = df[df["sensor"] == 2].copy() if total else pd.DataFrame()

    # ---------- Header ----------
    st.markdown('<div class="eyebrow">CLINICAL TELEMETRY / DEVICE OPERATIONS</div>', unsafe_allow_html=True)
    st.title({
        "Overview": "System overview",
        "Signal analysis": "Signal analysis",
        "Reliability center": "Reliability center",
        "Session explorer": "Session explorer",
    }[page])
    st.markdown(
        '<div class="page-sub">A clear view of recorded signals, synchronization, and data integrity.</div>',
        unsafe_allow_html=True,
    )
    h1, h2 = st.columns([4, 1])
    with h1:
        if selected_session:
            st.markdown(f"**Active record:** `{selected_session}`")
        else:
            st.markdown("**No recorded sessions** · Waiting for the first stored sample.")
    with h2:
        st.markdown(
            '<span class="pill">DATABASE READ-ONLY</span>',
            unsafe_allow_html=True,
        )

    if page == "Overview":
        cols = st.columns(4)
        with cols[0]:
            metric("Samples stored", f"{total:,}", "Saved sample records")
        with cols[1]:
            metric("Cloud acknowledged", f"{cloud_count:,}", "Records marked synced")
        with cols[2]:
            metric("Sync backlog", f"{backlog:,}", "Stored but not acknowledged")
        with cols[3]:
            metric("Recorded losses", f"{loss_count:,}", "Explicit loss records")

        st.markdown("")
        left, right = st.columns([1.65, 1])
        with left:
            with st.container(border=True):
                panel_heading("Signal telemetry", "Latest stored samples · 400 Hz per sensor")
                if total:
                    sensor_choice = st.segmented_control(
                        "Choose sensor", ["Sensor 1", "Sensor 2"], default="Sensor 1"
                    )
                    chosen = sensor1 if sensor_choice == "Sensor 1" else sensor2
                    if not chosen.empty:
                        channels = len(chosen.iloc[0]["values"])
                        chart_data = pd.DataFrame({
                            **{
                                f"Channel {i + 1}": chosen["values"].apply(
                                    lambda x, idx=i: x[idx] if len(x) > idx else None
                                ).values
                                for i in range(channels)
                            },
                            "Time (s)": chosen["time_s"].values,
                        }).set_index("Time (s)")
                        st.line_chart(chart_data.tail(1200), height=290)
                        st.caption(f"Showing up to 1,200 recorded samples · {channels} channels")
                    else:
                        st.info("No stored samples for this sensor.")
                else:
                    st.markdown("### No telemetry recorded yet")
                    st.write("The chart will populate when sample records are saved to the local SQLite database.")
                    st.progress(0)
                    st.caption("No synthetic measurements are being shown.")
        with right:
            with st.container(border=True):
                panel_heading("Collection summary", "Evidence from the local database")
                st.markdown(f"**Session duration (index-based):** {duration:.2f} s")
                st.markdown(f"**Sensor 1 records:** {len(sensor1):,}")
                st.markdown(f"**Sensor 2 records:** {len(sensor2):,}")
                st.markdown(f"**Distinct sessions:** {len(sessions)}")
                st.markdown(f"**Database:** `{db_path.name}`")
                st.markdown("---")
                if backlog:
                    st.warning(f"{backlog:,} stored records are not marked as synced.")
                elif total:
                    st.success("All stored records are marked as synced.")
                else:
                    st.info("No sample records available to evaluate synchronization.")

        with st.container(border=True):
            panel_heading("Recent data-loss records", "Reported gaps are preserved; missing values are never fabricated.")
            if losses:
                st.dataframe(pd.DataFrame([dict(x) for x in losses]).head(10), use_container_width=True, hide_index=True)
            else:
                st.caption("No loss records for this session." if selected_session else "No session exists yet.")

    elif page == "Signal analysis":
        st.markdown("### Recorded channel statistics")
        st.caption("Statistics are calculated from samples currently stored. They are not clinical measurements.")
        if df.empty:
            st.info("No stored samples to analyze. Connect a sensor collector and record data first.")
        else:
            sensor_choice = st.selectbox("Sensor", [1, 2], format_func=lambda x: f"Sensor {x}")
            selected = df[df["sensor"] == sensor_choice]
            channels = len(selected.iloc[0]["values"]) if not selected.empty else 0
            rows = []
            for channel in range(channels):
                vals = selected["values"].apply(lambda v: v[channel] if len(v) > channel else None).dropna()
                rows.append({
                    "Channel": f"Channel {channel + 1}",
                    "Samples": len(vals),
                    "Mean": round(float(vals.mean()), 3) if len(vals) else None,
                    "Minimum": int(vals.min()) if len(vals) else None,
                    "Maximum": int(vals.max()) if len(vals) else None,
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            chart = pd.DataFrame({
                f"Channel {i + 1}": selected["values"].apply(
                    lambda v: v[i] if len(v) > i else None
                ).values
                for i in range(channels)
            })
            st.markdown("### Signal waveform")
            st.line_chart(chart.tail(1600), height=350)
            st.caption("Plot contains stored samples only. The horizontal axis represents record order.")
            if sensor_choice == 1:
                st.info("Heart-rate estimation is available through the pipeline's window-processing results when the five-second window requirements are satisfied.")
            else:
                st.info("Sensor 2 is configured with two channels; the pipeline calculates signal magnitude from available paired samples.")

    elif page == "Reliability center":
        st.markdown("### Data integrity & synchronization")
        a, b, c = st.columns(3)
        with a:
            metric("Stored records", f"{total:,}", "Local sample rows")
        with b:
            metric("Pending sync", f"{backlog:,}", "Not marked acknowledged")
        with c:
            metric("Loss entries", f"{loss_count:,}", "Recorded discontinuities")
        st.markdown("")
        with st.container(border=True):
            panel_heading("Synchronization distribution", "Based on each sample's persisted synced flag")
            if total:
                st.bar_chart(pd.DataFrame({
                    "Records": [cloud_count, backlog]
                }, index=["Acknowledged", "Pending"]), height=220)
            else:
                st.info("Synchronization cannot be evaluated until samples are recorded.")
        with st.container(border=True):
            panel_heading("Loss ledger", "The source records the exact known range and reason. Unknown endpoints remain null.")
            if losses:
                loss_df = pd.DataFrame([dict(x) for x in losses])
                st.dataframe(loss_df, use_container_width=True, hide_index=True)
                st.download_button(
                    "Export loss ledger CSV",
                    loss_df.to_csv(index=False),
                    file_name=f"{selected_session or 'session'}-loss-ledger.csv",
                    mime="text/csv",
                )
            else:
                st.success("No loss records found for this session.")
        st.caption("This page reports persisted state only. It does not claim the sensors or cloud endpoint are currently reachable.")

    elif page == "Session explorer":
        st.markdown("### Session records")
        if not sessions:
            st.info("The database contains no sessions with stored samples.")
        else:
            st.write(f"**Selected session:** `{selected_session}`")
            st.dataframe(pd.DataFrame([{
                "Sensor": int(r["sensor"]),
                "Sample index": int(r["sample_index"]),
                "Values": r["values_json"],
                "Cloud synced": bool(r["synced"]),
            } for r in samples[:1000]]), use_container_width=True, hide_index=True)
            if len(samples) > 1000:
                st.caption("Table preview limited to the first 1,000 records. The export includes all applicable records.")
            st.markdown("### Export session")
            e1, e2 = st.columns(2)
            with e1:
                st.download_button(
                    "↓ Export device JSONL",
                    export_jsonl(samples, losses, cloud_only=False),
                    file_name=f"{selected_session}-device.jsonl",
                    mime="application/x-ndjson",
                    use_container_width=True,
                )
            with e2:
                st.download_button(
                    "↓ Export acknowledged cloud JSONL",
                    export_jsonl(samples, losses, cloud_only=True),
                    file_name=f"{selected_session}-cloud.jsonl",
                    mime="application/x-ndjson",
                    use_container_width=True,
                )
            st.caption("Exports contain stored sample records followed by loss records. Cloud export includes only samples marked synced.")

    st.markdown("---")
    st.markdown(
        '<div style="display:flex;justify-content:space-between;color:#718398;font-size:11px">'
        '<span>MAYACARDIO · HEARTBEAT INTELLIGENCE</span>'
        f'<span>LOCAL DATA VIEW · {datetime.now().strftime("%d %b %Y, %H:%M")}</span>'
        '</div>',
        unsafe_allow_html=True,
    )
finally:
    con.close()
