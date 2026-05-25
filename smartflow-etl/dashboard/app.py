"""
dashboard/app.py
────────────────
Streamlit dashboard for SmartFlow ETL.
Reads directly from DuckDB — daily_metrics and fct_trips tables.

Run: streamlit run dashboard/app.py
     Opens: http://localhost:8501
"""

import os
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(THIS_DIR)
#DB_PATH  = os.path.join(ROOT_DIR, "data", "smartflow.duckdb")
DB_PATH = r"C:\Users\varsh\OneDrive\Desktop\smartflow-etl\data\smartflow.duckdb"
st.set_page_config(
    page_title  = "SmartFlow ETL — NYC Taxi Dashboard",
    page_icon   = "🚕",
    layout      = "wide",
    initial_sidebar_state = "expanded",
)

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_daily_metrics() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    df  = con.execute("SELECT * FROM main_metrics.daily_metrics ORDER BY trip_date").fetchdf()
    con.close()
    df["trip_date"] = pd.to_datetime(df["trip_date"])
    return df

@st.cache_data(ttl=300)
def load_fct_trips() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    df  = con.execute("""
        SELECT pickup_hour, time_of_day, vendor_name, payment_type_label,
               distance_bucket, fare_bucket, tip_category, trip_zone_type,
               fare_amount, total_amount, tip_amount, trip_distance,
               trip_duration_mins, avg_speed_mph, is_airport_trip,
               pickup_day_name, pickup_day_of_week
        FROM main_marts.fct_trips
    """).fetchdf()
    con.close()
    return df

@st.cache_data(ttl=300)
def load_quarantine_stats() -> dict:
    quarantine_dir = os.path.join(ROOT_DIR, "data", "quarantine")
    files = [f for f in os.listdir(quarantine_dir) if f.endswith(".parquet")] if os.path.exists(quarantine_dir) else []
    if not files:
        return {"total": 0, "reasons": {}}
    import glob
    dfs = [pd.read_parquet(os.path.join(quarantine_dir, f)) for f in files]
    df  = pd.concat(dfs)
    return {
        "total":   len(df),
        "reasons": df["_anomaly_reason"].value_counts().to_dict() if "_anomaly_reason" in df.columns else {},
    }

# ── Load data ─────────────────────────────────────────────────────────────────
daily   = load_daily_metrics()
trips   = load_fct_trips()
qstats  = load_quarantine_stats()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/48/taxi.png", width=48)
    st.title("SmartFlow ETL")
    st.caption("AI-Powered NYC Taxi Pipeline")
    st.divider()

    date_range = st.date_input(
        "Date range",
        value=(daily["trip_date"].min(), daily["trip_date"].max()),
        min_value=daily["trip_date"].min(),
        max_value=daily["trip_date"].max(),
    )
    st.divider()
    st.markdown("**Pipeline layers**")
    st.success("✓ Ingestion")
    st.success("✓ AI Schema Mapping")
    st.success("✓ AI Data Cleaning")
    st.success("✓ Anomaly Detection")
    st.success("✓ dbt Transformation")
    st.success("✓ Validation (12/12)")

# ── Filter by date ────────────────────────────────────────────────────────────
if len(date_range) == 2:
    start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
    daily_f = daily[(daily["trip_date"] >= start) & (daily["trip_date"] <= end)]
else:
    daily_f = daily

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🚕 NYC Taxi — SmartFlow ETL Dashboard")
st.caption(f"Powered by DuckDB · dbt · scikit-learn · Dagster  |  {len(trips):,} clean trips loaded")
st.divider()

# ── KPI row ───────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total Trips",    f"{daily_f['total_trips'].sum():,.0f}")
k2.metric("Total Revenue",  f"${daily_f['total_revenue'].sum():,.0f}")
k3.metric("Avg Fare",       f"${daily_f['avg_fare'].mean():.2f}")
k4.metric("Avg Tip %",      f"{trips['tip_amount'].mean() / trips['fare_amount'].mean() * 100:.1f}%")
k5.metric("Airport Trips",  f"{daily_f['airport_trips'].sum():,.0f}")
k6.metric("Quarantined",    f"{qstats['total']:,}", delta=f"-{qstats['total']/50000*100:.1f}% anomaly rate", delta_color="inverse")

st.divider()

# ── Row 1: Revenue trend + Trip volume ────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Daily Revenue & 7-Day Trend")
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=daily_f["trip_date"], y=daily_f["total_revenue"],
        name="Daily Revenue", marker_color="#4C9BE8", opacity=0.7,
    ))
    fig.add_trace(go.Scatter(
        x=daily_f["trip_date"], y=daily_f["revenue_7day_avg"],
        name="7-Day Avg", line=dict(color="#FF6B6B", width=2.5),
    ))
    fig.update_layout(
        height=320, margin=dict(t=10, b=10),
        legend=dict(orientation="h", y=1.1),
        yaxis_tickprefix="$", plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Trip Volume by Day of Week")
    dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    dow = trips.groupby("pickup_day_name").size().reindex(dow_order).reset_index()
    dow.columns = ["day", "trips"]
    fig2 = px.bar(dow, x="day", y="trips", color="trips",
                  color_continuous_scale="Blues", height=320)
    fig2.update_layout(margin=dict(t=10, b=10), coloraxis_showscale=False,
                       plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig2, use_container_width=True)

# ── Row 2: Hourly heatmap + Payment mix ──────────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("Trips by Hour of Day")
    hourly = trips.groupby("pickup_hour").agg(
        trips=("fare_amount","count"),
        avg_fare=("fare_amount","mean")
    ).reset_index()
    fig3 = make_subplots(specs=[[{"secondary_y": True}]])
    fig3.add_trace(go.Bar(x=hourly["pickup_hour"], y=hourly["trips"],
                          name="Trips", marker_color="#4C9BE8", opacity=0.7), secondary_y=False)
    fig3.add_trace(go.Scatter(x=hourly["pickup_hour"], y=hourly["avg_fare"].round(2),
                              name="Avg Fare", line=dict(color="#FF6B6B", width=2)), secondary_y=True)
    fig3.update_layout(height=300, margin=dict(t=10,b=10),
                       legend=dict(orientation="h", y=1.1),
                       plot_bgcolor="rgba(0,0,0,0)")
    fig3.update_yaxes(title_text="Trips", secondary_y=False)
    fig3.update_yaxes(title_text="Avg Fare ($)", secondary_y=True)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("Payment Type Distribution")
    pay = trips["payment_type_label"].value_counts().reset_index()
    pay.columns = ["payment", "count"]
    fig4 = px.pie(pay, names="payment", values="count",
                  color_discrete_sequence=px.colors.qualitative.Set2, height=300)
    fig4.update_layout(margin=dict(t=10, b=10))
    st.plotly_chart(fig4, use_container_width=True)

# ── Row 3: Distance buckets + Tip behaviour ───────────────────────────────────
col5, col6 = st.columns(2)

with col5:
    st.subheader("Trip Distance Segments")
    dist_order = ["Under 1 mile","1–3 miles","3–7 miles","7–15 miles","15+ miles"]
    dist = trips.groupby("distance_bucket").agg(
        trips=("fare_amount","count"),
        avg_fare=("fare_amount","mean"),
    ).reindex(dist_order).reset_index()
    fig5 = px.bar(dist, x="distance_bucket", y="trips",
                  color="avg_fare", color_continuous_scale="Teal",
                  labels={"distance_bucket":"Distance","avg_fare":"Avg Fare ($)"},
                  height=300)
    fig5.update_layout(margin=dict(t=10,b=10), plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig5, use_container_width=True)

with col6:
    st.subheader("Tip Behaviour")
    tip = trips["tip_category"].value_counts().reset_index()
    tip.columns = ["category", "count"]
    fig6 = px.pie(tip, names="category", values="count",
                  color_discrete_sequence=["#2ecc71","#3498db","#e74c3c"],
                  height=300)
    fig6.update_layout(margin=dict(t=10, b=10))
    st.plotly_chart(fig6, use_container_width=True)

# ── Row 4: Cumulative revenue + Anomaly breakdown ─────────────────────────────
col7, col8 = st.columns(2)

with col7:
    st.subheader("Cumulative Revenue")
    fig7 = px.area(daily_f, x="trip_date", y="cumulative_revenue",
                   color_discrete_sequence=["#4C9BE8"], height=280)
    fig7.update_layout(margin=dict(t=10,b=10), yaxis_tickprefix="$",
                       plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig7, use_container_width=True)

with col8:
    st.subheader("Anomaly Detection Results")
    total_raw   = 50_000
    clean       = len(trips)
    quarantined = qstats["total"]
    fig8 = go.Figure(go.Waterfall(
        orientation="v",
        measure=["absolute","relative","relative"],
        x=["Raw Rows","Cleaned (AI)","Quarantined"],
        y=[total_raw, clean - total_raw, -quarantined],
        text=[f"{total_raw:,}", f"+{clean-total_raw:,}", f"-{quarantined:,}"],
        connector={"line":{"color":"rgb(63, 63, 63)"}},
        increasing={"marker":{"color":"#2ecc71"}},
        decreasing={"marker":{"color":"#e74c3c"}},
    ))
    fig8.update_layout(height=280, margin=dict(t=10,b=10),
                       plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig8, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption("SmartFlow ETL · Built with Python · DuckDB · dbt · scikit-learn · Dagster · Streamlit")