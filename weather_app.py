import streamlit as st
import plotly.express as px
from db_connection import get_city_ranking, get_anomalies, get_forecast, get_clusters

st.set_page_config(
    page_title="Global Weather Dashboard",
    page_icon="🌍",
    layout="wide"
)

st.title("🌍 Global Weather Dashboard")
st.caption("Live data from Databricks · 100 cities · Updates hourly")

# ── Cache data so we don't query on every interaction ────────────────────
@st.cache_data(ttl=3600)   # refresh cache every hour
def load_ranking():
    return get_city_ranking()

@st.cache_data(ttl=3600)
def load_anomalies():
    return get_anomalies()

@st.cache_data(ttl=3600)
def load_clusters():
    return get_clusters()

df_ranking  = load_ranking()
df_anomalies = load_anomalies()
df_clusters  = load_clusters()

# ── KPI tiles ─────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

hottest = df_ranking.iloc[0]
coldest = df_ranking.iloc[-1]

col1.metric("🔥 Hottest city",  hottest["city"],
            f"{hottest['temperature_c']}°C")
col2.metric("❄️ Coldest city",  coldest["city"],
            f"{coldest['temperature_c']}°C")
col3.metric("⚠️ Anomalies today", len(df_anomalies))
col4.metric("🌡️ Cities tracked", len(df_ranking))

st.divider()

# ── Two column layout ──────────────────────────────────────────────────────
left, right = st.columns(2)

with left:
    st.subheader("Current temperature ranking")
    st.dataframe(
        df_ranking[["temp_rank", "city", "country",
                    "temperature_c", "feels_like_c",
                    "humidity_pct", "weather_description"]],
        use_container_width=True,
        hide_index=True
    )

with right:
    st.subheader("Anomalies detected today")
    if len(df_anomalies) == 0:
        st.success("No anomalies detected today!")
    else:
        st.dataframe(
            df_anomalies,
            use_container_width=True,
            hide_index=True
        )

st.divider()

# ── Forecast section ───────────────────────────────────────────────────────
st.subheader("7-day temperature forecast")

cities = sorted(df_ranking["city"].tolist())
selected_city = st.selectbox("Select a city", cities, index=cities.index("London"))

@st.cache_data(ttl=3600)
def load_forecast(city):
    return get_forecast(city)

df_forecast = load_forecast(selected_city)

if len(df_forecast) > 0:
    fig = px.line(
        df_forecast,
        x="forecast_date",
        y="predicted_temp_c",
        title=f"7-day forecast — {selected_city}",
        labels={"predicted_temp_c": "Temperature (°C)",
                "forecast_date": "Date"}
    )
    fig.update_traces(
        line_color="#1D9E75",
        line_width=2,
        mode="lines+markers+text",
        marker=dict(size=8, color="#1D9E75"),
        text=df_forecast["predicted_temp_c"].astype(str) + "°C",
        textposition="top center",
        textfont=dict(color="#1D9E75", size=12)
    )
    fig.update_layout(
        yaxis=dict(range=[
            df_forecast["predicted_low_c"].min() - 2,
            df_forecast["predicted_high_c"].max() + 2
        ])
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Cluster section ────────────────────────────────────────────────────────
st.subheader("City climate clusters")

fig2 = px.scatter(
    df_clusters,
    x="mean_temp_c",
    y="mean_humidity",
    color="climate_cluster",
    hover_name="city",
    title="Cities grouped by climate type",
    labels={"mean_temp_c": "Avg Temperature (°C)",
            "mean_humidity": "Avg Humidity (%)"}
)
st.plotly_chart(fig2, use_container_width=True)
