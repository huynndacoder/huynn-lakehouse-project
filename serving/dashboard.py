import os
import time
import numpy as np
import pandas as pd
import requests
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
from plotly.subplots import make_subplots

TIMEZONE_OFFSET = timezone(timedelta(hours=7))  # GMT+7

# --- Configuration & Setup ---
st.set_page_config(
    page_title="NYC Taxi Real-Time Lakehouse", page_icon="🚕", layout="wide"
)

API_BASE_URL = os.environ.get("API_BASE_URL", "http://analytics-api:8000/api/v1")
try:
    API_KEY = st.secrets["API_KEY"]
except (FileNotFoundError, KeyError):
    API_KEY = os.getenv("API_KEY", "demo-api-key-2024")

TIMEOUT_SEC = 60
CACHE_TTL_SEC = 10  # Cache for 10 seconds for faster updates


# --- Helper Functions ---
def get_api_headers() -> dict:
    return {"X-API-Key": API_KEY, "Accept": "application/json"}


@st.cache_data(ttl=CACHE_TTL_SEC, show_spinner=False)
def fetch_data(endpoint: str, params: dict = None, mode: str = None) -> dict:
    url = f"{API_BASE_URL}/{endpoint.lstrip('/')}"
    # Add mode parameter for API routing if not already in params
    if params is None:
        params = {}
    if mode is not None and "mode" not in params:
        params["mode"] = mode
    try:
        response = requests.get(
            url, headers=get_api_headers(), params=params, timeout=TIMEOUT_SEC
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("success"):
            return payload.get("data")
        else:
            st.error(f"API returned error: {payload.get('message')}")
    except requests.exceptions.RequestException as e:
        st.warning(f"API call failed: {endpoint}")
    return None


def create_mock_data(endpoint: str) -> dict:
    if endpoint == "dashboard/stats":
        return {
            "total_trips_today": 2847,
            "total_revenue_today": 47832.50,
            "avg_fare_today": 16.80,
            "top_zones": [{"zone_name": "JFK", "trips": 450, "revenue": 12500}],
        }
    elif endpoint == "analytics/time-series":
        base = datetime.now()
        return {
            "timestamps": [(base - timedelta(hours=i)).isoformat() for i in range(24)][
                ::-1
            ],
            "values": np.random.randint(500, 2000, 24).tolist(),
        }
    elif endpoint == "analytics/zones":
        return [
            {
                "zone_name": f"Zone {i}",
                "pickups": 1000 + i * 100,
                "revenue": 15000 + i * 500,
                "avg_fare": 15.0 + i * 0.5,
                "borough": ["Manhattan", "Brooklyn", "Queens"][i % 3],
            }
            for i in range(5)
        ]
    elif endpoint == "analytics/weather-impact":
        return [
            {
                "date": "2026-04-01",
                "hour": f"{h:02d}:00",
                "weather_condition": "Clear",
                "temperature": 20.0,
                "humidity": 60.0,
                "trips": 1000 + i * 100,
                "avg_fare": 15.0 + i * 0.5,
                "avg_distance": 5.0 + i * 0.2,
            }
            for i, h in enumerate(range(8, 18))
        ]
    elif endpoint == "realtime/activity":
        return [
            {
                "zone_id": i,
                "zone_name": f"Zone {i}",
                "pickup_count": 50 + i * 10,
                "revenue_last_hour": 500 + i * 50,
                "activity_score": 0.5 + i * 0.05,
            }
            for i in range(10)
        ]
    return None


def create_mock_data(endpoint: str) -> dict:
    if endpoint == "dashboard/stats":
        return {
            "total_trips_today": 2847,
            "total_revenue_today": 47832.50,
            "avg_fare_today": 16.80,
            "top_zones": [{"zone_name": "JFK", "trips": 450, "revenue": 12500}],
        }
    elif endpoint == "analytics/time-series":
        base = datetime.now()
        return {
            "timestamps": [(base - timedelta(hours=i)).isoformat() for i in range(24)][
                ::-1
            ],
            "values": np.random.randint(500, 2000, 24).tolist(),
        }
    elif endpoint == "analytics/zones":
        return [
            {
                "zone_name": f"Zone {i}",
                "pickups": np.random.randint(100, 1000),
                "revenue": np.random.uniform(2000, 15000),
            }
            for i in range(1, 11)
        ]
    return {}


# --- UI Components ---
def render_header(data_mode: str):
    status_color = "🟢" if data_mode == "Real-Time" else "🔵"
    latency = "< 30s" if data_mode == "Real-Time" else "< 5 min"

    st.title("🚕 NYC Taxi Lakehouse Dashboard")
    st.markdown(
        f"**Powered by:** Kafka, Spark Structured Streaming, Iceberg, ClickHouse, and FastAPI"
    )

    cols = st.columns(4)
    cols[0].metric("System Status", f"{status_color} Online")
    cols[1].metric(
        "Last Update", datetime.now(TIMEZONE_OFFSET).strftime("%H:%M:%S (GMT+7)")
    )
    cols[2].metric("Data Freshness", latency)
    cols[3].metric("Active Pipelines", "3 / 3")
    st.divider()


def render_kpi_cards(
    stats: dict, previous_stats: dict = None, mode: str = "Historical"
):
    if not stats:
        st.warning("No data available for KPIs.")
        return

    trips = stats.get("total_trips", 0)
    revenue = stats.get("total_revenue", 0.0)
    avg_fare = stats.get("avg_fare", 0.0)
    temperature = stats.get("temperature", 0)
    humidity = stats.get("humidity", 0)
    precipitation = stats.get("precipitation", 0)
    active_zones = stats.get("active_zones", 0)

    prev_trips = previous_stats.get("total_trips") if previous_stats else None
    prev_revenue = previous_stats.get("total_revenue") if previous_stats else None
    prev_avg_fare = previous_stats.get("avg_fare") if previous_stats else None
    prev_active_zones = previous_stats.get("active_zones") if previous_stats else None

    trip_delta = trips - prev_trips if prev_trips is not None else None
    revenue_delta = revenue - prev_revenue if prev_revenue is not None else None
    avg_fare_delta = avg_fare - prev_avg_fare if prev_avg_fare is not None else None
    zones_delta = (
        active_zones - prev_active_zones if prev_active_zones is not None else None
    )

    st.subheader("📊 Key Performance Indicators")

    label_trips = "Total Trips Today" if mode == "Real-Time" else "Total Trips"
    label_revenue = "Total Revenue Today" if mode == "Real-Time" else "Total Revenue"

    cols = st.columns(4)

    cols[0].metric(
        label_trips,
        f"{trips:,}" if trips is not None else "N/A",
        delta=f"{trip_delta:+,}" if trip_delta is not None else None,
    )
    cols[1].metric(
        label_revenue,
        f"${revenue:,.2f}" if revenue is not None else "N/A",
        delta=f"${revenue_delta:+,.2f}" if revenue_delta is not None else None,
    )
    cols[2].metric(
        "Average Fare",
        f"${avg_fare:.2f}" if avg_fare is not None else "N/A",
        delta=f"${avg_fare_delta:+.2f}" if avg_fare_delta is not None else None,
    )
    cols[3].metric(
        "Active Zones",
        f"{active_zones}" if active_zones is not None else "N/A",
        delta=f"{zones_delta:+d}" if zones_delta is not None else None,
    )

    if temperature or humidity:
        st.caption(
            f"🌤️ Weather: {temperature}°C | Humidity: {humidity}% | Precipitation: {precipitation}mm"
        )


def render_time_series_chart(data: dict, title: str = "Trip Volume Over Time"):
    if not data or "timestamps" not in data or not data["timestamps"]:
        st.info("Time series data unavailable.")
        return

    df = pd.DataFrame(
        {"Time": pd.to_datetime(data["timestamps"]), "Trips": data["values"]}
    )
    df = df.sort_values("Time")

    fig = px.line(df, x="Time", y="Trips", title=title, markers=True)
    fig.update_layout(
        height=400, hovermode="x unified", margin=dict(l=0, r=0, t=40, b=0)
    )
    st.plotly_chart(fig, use_container_width=True)


def render_zone_performance(data, boroughs=None):
    if not data:
        st.info("No zone data available.")
        return

    df = pd.DataFrame(data).head(5)

    if "trips" not in df.columns and "pickups" in df.columns:
        df["trips"] = df["pickups"]
    if "revenue" not in df.columns:
        df["revenue"] = df.get("revenue", 0)

    if "zone_name" not in df.columns:
        df["zone_name"] = df.get(
            "zone_name", df.get("Zone", df.get("zone_id", "Unknown"))
        )

    ZONE_COLORS = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
    ]

    borough_label = f" ({', '.join(boroughs)})" if boroughs else ""
    st.subheader(f"🚕 Top 5 Zones Performance{borough_label}")

    fig = make_subplots(
        rows=1, cols=2, subplot_titles=("Trip Count by Zone", "Revenue by Zone")
    )

    fig.add_trace(
        go.Bar(
            x=df["zone_name"],
            y=df["trips"],
            marker_color=ZONE_COLORS[: len(df)],
            name="Trips",
            text=df["trips"],
            textposition="outside",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=df["zone_name"],
            y=df["revenue"],
            marker_color=ZONE_COLORS[: len(df)],
            name="Revenue",
            text=[f"${r:,.0f}" for r in df["revenue"]],
            textposition="outside",
        ),
        row=1,
        col=2,
    )

    fig.update_layout(height=400, showlegend=False, margin=dict(l=0, r=0, t=40, b=80))
    fig.update_xaxes(tickangle=45, row=1, col=1)
    fig.update_xaxes(tickangle=45, row=1, col=2)
    st.plotly_chart(fig, use_container_width=True)


def render_real_time_map(data: list = None):
    st.subheader("📍 Live Fleet Activity")

    if not data:
        df = pd.DataFrame(
            {
                "lat": np.random.normal(40.7589, 0.05, 100),
                "lon": np.random.normal(-73.9851, 0.05, 100),
                "activity": np.random.randint(1, 10, 100),
            }
        )
    else:
        df = pd.DataFrame(data)

    try:
        fig = px.scatter_mapbox(
            df,
            lat="lat",
            lon="lon",
            size="activity",
            color_discrete_sequence=["fuchsia"],
            zoom=10,
            height=400,
        )
        fig.update_layout(
            mapbox_style="open-street-map", margin={"r": 0, "t": 0, "l": 0, "b": 0}
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.warning(f"Map render issue, using fallback.")
        st.map(df)


def render_analytics_section(
    weather_params: dict = None, zone_data=None, boroughs=None
):
    st.divider()
    st.subheader("📊 Advanced Analytics")
    tab1, tab2, tab3, tab4 = st.tabs(
        ["Weather Impact", "Demand Prediction", "Zone Analysis", "Real-time Activity"]
    )

    with tab1:
        weather_data = fetch_data("analytics/weather-impact", weather_params)
        if weather_data:
            df_w = pd.DataFrame(weather_data)
            if not df_w.empty:
                weather_colors = {
                    "Clear": "#FFD700",
                    "Light Rain": "#87CEEB",
                    "Rainy": "#4682B4",
                    "Cold": "#00BFFF",
                    "Hot": "#FF4500",
                    "No Weather Data": "#808080",
                }

                st.subheader("📊 Weather Impact Analysis")

                # Convert date and hour to datetime for proper sorting
                df_w["datetime"] = pd.to_datetime(df_w["date"] + " " + df_w["hour"])
                df_w = df_w.sort_values("datetime")

                # Scatter plot: each dot = one hour, colored by weather condition
                fig = px.scatter(
                    df_w,
                    x="trips",
                    y="avg_fare",
                    color="weather_condition",
                    color_discrete_map=weather_colors,
                    hover_data=["date", "hour", "temperature", "avg_distance"],
                    title="Hourly Trips vs Average Fare by Weather<br><sup>Each dot represents one hour</sup>",
                    labels={
                        "trips": "Trips",
                        "avg_fare": "Average Fare ($)",
                        "weather_condition": "Weather",
                        "temperature": "Temp (°C)",
                        "avg_distance": "Avg Distance (mi)",
                    },
                )
                fig.update_traces(
                    marker=dict(size=12, opacity=0.8, line=dict(width=1, color="white"))
                )
                fig.update_layout(
                    height=500,
                    legend=dict(
                        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                    ),
                )
                st.plotly_chart(fig, use_container_width=True)

                # Weather condition distribution
                st.subheader("📅 Weather Distribution")
                weather_counts = (
                    df_w.groupby("weather_condition")
                    .agg({"trips": "sum", "avg_fare": "mean"})
                    .reset_index()
                    .sort_values("trips", ascending=False)
                )

                col1, col2, col3, col4 = st.columns(4)
                for idx, (_, row) in enumerate(weather_counts.iterrows()):
                    cols = [col1, col2, col3, col4]
                    if idx < 4:
                        icon = {
                            "Clear": "☀️",
                            "Light Rain": "🌦️",
                            "Rainy": "🌧️",
                            "Cold": "❄️",
                            "Hot": "🔥",
                            "No Weather Data": "❓",
                        }.get(row["weather_condition"], "🌡️")
                        cols[idx].metric(
                            f"{icon} {row['weather_condition']}",
                            f"{row['trips']:,.0f} trips"
                            if row["trips"] is not None
                            else "N/A",
                            f"${row['avg_fare']:.2f} avg fare"
                            if row.get("avg_fare") is not None
                            else "N/A",
                        )
        else:
            st.info("No weather data available")

    with tab2:
        demand_data = fetch_data("predictions/demand")
        if demand_data:
            df_pred = pd.DataFrame(demand_data)
            if not df_pred.empty:
                fig = px.bar(
                    df_pred.head(10),
                    x="zone_name",
                    y="predicted_demand",
                    color="predicted_demand",
                    color_continuous_scale="Viridis",
                    title="Top 10 Zone Demand Predictions (Next Hour)",
                )
                fig.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

                avg_confidence = df_pred["confidence_score"].mean()
                st.metric(
                    "Average Model Confidence",
                    f"{avg_confidence * 100:.1f}%"
                    if pd.notna(avg_confidence)
                    else "N/A",
                )
        else:
            st.info("No demand prediction data available")

    with tab3:
        if zone_data:
            df_z = pd.DataFrame(zone_data)
            if not df_z.empty:
                borough_label = f" ({', '.join(boroughs)})" if boroughs else " (All)"
                st.subheader(f"🚕 Zone Analysis{borough_label}")

                # Use avg_fare if available, ensure non-negative for size
                if "avg_fare" in df_z.columns:
                    df_z["size_col"] = df_z["avg_fare"].clip(lower=0)
                    size_col = "size_col"
                else:
                    size_col = None

                fig = px.scatter(
                    df_z,
                    x="pickups" if "pickups" in df_z.columns else "trips",
                    y="revenue",
                    size=size_col,
                    color="borough" if "borough" in df_z.columns else None,
                    hover_name="zone_name",
                    title="Zone Performance (Size = Avg Fare)",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No zone data available for selected filters")
        else:
            st.info("No zone data available")

    with tab4:
        rt_data = fetch_data("realtime/activity")
        if rt_data:
            df_rt = pd.DataFrame(rt_data)
            if not df_rt.empty:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Active Zones", len(df_rt))
                c2.metric("Total Pickups", f"{df_rt['pickup_count'].sum():,}")
                c3.metric("Revenue (1hr)", f"${df_rt['revenue_last_hour'].sum():,.0f}")
                avg_activity = df_rt["activity_score"].mean()
                c4.metric(
                    "Avg Activity",
                    f"{avg_activity:.1f}" if pd.notna(avg_activity) else "N/A",
                )
        else:
            st.info("No real-time activity data available")


def render_sidebar(data_mode: str) -> dict:
    st.sidebar.header("🎛️ Controls")

    if data_mode == "Historical":
        max_range_days = 30  # Limit to 30 days for fast queries
        default_start = datetime(2025, 1, 1).date()  # Default to start of 2025
        default_end = datetime(2025, 1, 2).date()  # Default to Jan 2, 2025

        st.sidebar.caption(f"📅 Max range: {max_range_days} days")
        start_date = st.sidebar.date_input(
            "Start Date", default_start, help=f"Maximum range is {max_range_days} days"
        )
        end_date = st.sidebar.date_input(
            "End Date", default_end, help=f"Maximum range is {max_range_days} days"
        )

        # Enforce max date range
        if start_date and end_date:
            days_diff = (end_date - start_date).days
            if days_diff > max_range_days:
                st.sidebar.warning(
                    f"⚠️ Date range ({days_diff} days) exceeds max ({max_range_days}). Using last {max_range_days} days."
                )
                start_date = end_date - timedelta(days=max_range_days)
    else:
        st.sidebar.info("Date filters disabled in Real-Time mode")
        start_date = None
        end_date = None

    boroughs = st.sidebar.multiselect(
        "Borough",
        ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"],
        default=["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"],
    )

    # Debounced fare range slider - only updates on button click
    if "fare_range" not in st.session_state:
        st.session_state.fare_range = (10, 50)

    st.sidebar.slider(
        "Fare Range ($)",
        0,
        150,
        key="fare_range",
        help="Adjust and click 'Apply Filters' to update",
    )
    fare_range = st.session_state.fare_range

    st.sidebar.divider()

    if "refresh_key" not in st.session_state:
        st.session_state.refresh_key = 0

    col1, col2 = st.sidebar.columns(2)
    if col1.button("🔄 Refresh"):
        st.session_state.refresh_key += 1
        st.rerun()
    if col2.button("🗑️ Clear Cache"):
        st.cache_data.clear()
        st.rerun()

    return {
        "start_date": start_date,
        "end_date": end_date,
        "boroughs": boroughs,
        "fare_range": fare_range,
        "refresh_key": st.session_state.refresh_key,
    }


# --- Main Execution Flow ---
def main():
    if "refresh_counter" not in st.session_state:
        st.session_state.refresh_counter = 0

    data_mode = st.sidebar.radio(
        "📊 Data Mode",
        options=["Historical", "Real-Time"],
        index=1,  # Default to Real-Time
        horizontal=True,
        help="Historical: Use date filters. Real-Time: Latest hour, auto-refresh.",
    )

    render_header(data_mode)
    filters = render_sidebar(data_mode)

    start_date = filters.get("start_date")
    end_date = filters.get("end_date")
    fare_range = filters.get("fare_range", (0, 150))
    boroughs = filters.get("boroughs", [])

    if data_mode == "Historical":
        # Use date filters for historical data
        date_params = {}
        if start_date:
            date_params["start_date"] = start_date.isoformat()
        if end_date:
            date_params["end_date"] = end_date.isoformat()

        # Always include date range to avoid full table scans
        if not date_params:
            default_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            default_end = datetime.now().strftime("%Y-%m-%d")
            date_params = {"start_date": default_start, "end_date": default_end}

        # Zone params include boroughs filter (no limit - show all zones for analysis)
        zone_params = {**date_params}
        if boroughs:
            zone_params["boroughs"] = ",".join(boroughs)

        ts_params = {"metric": "trip_count", "interval": "hour", **date_params}
        weather_params = {
            "start_date": date_params.get("start_date"),
            "end_date": date_params.get("end_date"),
            "mode": "historical",
        }
        ts_title = "Trip Volume"

    else:
        # Real-time mode: today's data using date range (faster than hours_back with Iceberg)
        today = datetime.now(TIMEZONE_OFFSET).strftime("%Y-%m-%d")
        stats_params = {"start_date": today, "end_date": today}

        # Zone params include boroughs filter (no limit - show all zones for analysis)
        zone_params = {"start_date": today, "end_date": today}
        if boroughs:
            zone_params["boroughs"] = ",".join(boroughs)

        ts_params = {
            "metric": "trip_count",
            "interval": "hour",
            "start_date": today,
            "end_date": today,
        }
        weather_params = {"start_date": today, "end_date": today, "mode": "realtime"}
        ts_title = "Trip Volume (Today - Real-Time)"

    # Fetch Data
    api_mode = "historical" if data_mode == "Historical" else "realtime"
    stats_data = fetch_data(
        "dashboard/stats",
        date_params if data_mode == "Historical" else stats_params,
        mode=api_mode,
    )
    if not stats_data:
        st.warning("API Unavailable. Loading Fallback Data.")
        stats_data = create_mock_data("dashboard/stats")

    ts_data = fetch_data("analytics/time-series", ts_params, mode=api_mode)
    if not ts_data:
        ts_data = create_mock_data("analytics/time-series")

    zone_data = fetch_data("analytics/zones", zone_params, mode=api_mode)
    if not zone_data:
        zone_data = create_mock_data("analytics/zones")

    # Calculate previous period stats for delta indicators
    previous_stats = None
    if data_mode == "Historical" and start_date and end_date:
        try:
            start_dt = datetime.strptime(start_date.isoformat(), "%Y-%m-%d")
            end_dt = datetime.strptime(end_date.isoformat(), "%Y-%m-%d")
            date_range_days = (end_dt - start_dt).days + 1
            prev_start = (start_dt - timedelta(days=date_range_days)).strftime(
                "%Y-%m-%d"
            )
            prev_end = (start_dt - timedelta(days=1)).strftime("%Y-%m-%d")

            # Cache key based on date range
            cache_key = f"prev_stats_{prev_start}_{prev_end}"
            if cache_key not in st.session_state:
                st.session_state[cache_key] = fetch_data(
                    "dashboard/stats",
                    {"start_date": prev_start, "end_date": prev_end},
                    mode="historical",
                )
            previous_stats = st.session_state[cache_key]
        except Exception:
            previous_stats = None
    elif data_mode == "Real-Time":
        previous_stats = None

    render_kpi_cards(stats_data, previous_stats=previous_stats, mode=data_mode)

    st.divider()

    # Mode info
    if data_mode == "Historical" and start_date and end_date:
        st.caption(f"📅 Showing data from **{start_date}** to **{end_date}**")
    elif data_mode == "Real-Time":
        st.caption("⚡ Showing latest real-time data (last hour)")

    render_time_series_chart(ts_data, title=ts_title)
    render_zone_performance(zone_data, boroughs=boroughs)

    render_real_time_map()
    render_analytics_section(weather_params, zone_data=zone_data, boroughs=boroughs)


if __name__ == "__main__":
    main()
