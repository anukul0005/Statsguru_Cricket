"""
IPL Stats – Streamlit Dashboard
Run: streamlit run dashboard.py
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IPL Stats Dashboard",
    page_icon="🏏",
    layout="wide",
)

st.markdown("""
<style>
  [data-testid="stMetricValue"] { font-size: 28px; font-weight: 800; }
  .block-container { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)

# ── Load data ──────────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    df = pd.read_csv("match_summary.csv", dtype=str).fillna("")
    df["date"]      = pd.to_datetime(df["date"], errors="coerce")
    df["year"]      = df["date"].dt.year.astype("Int64").astype(str).str.replace("<NA>", "")
    df["inn1_runs"] = pd.to_numeric(df["inn1_runs"], errors="coerce")
    df["inn2_runs"] = pd.to_numeric(df["inn2_runs"], errors="coerce")
    df["is_ipl"]    = df["series"].str.contains("Premier League", case=False, na=False)
    return df.sort_values("date", ascending=False)

DF = load_data()

# ── Sidebar filters ────────────────────────────────────────────────────────────
st.sidebar.header("🔍 Filters")
st.sidebar.markdown("All charts and stats update with these filters.")

# IPL only toggle
ipl_only = st.sidebar.checkbox("IPL matches only", value=False)

all_teams = sorted(
    set(DF["inn1_team"].unique()) | set(DF["inn2_team"].unique()) - {""}
)
all_grounds  = sorted([g for g in DF["ground"].unique() if g])
all_series   = sorted([s for s in DF["series"].unique() if s])
all_years    = sorted([y for y in DF["year"].unique() if y], reverse=True)
ipl_grounds  = sorted([
    g for g in DF[DF["is_ipl"]]["ground"].unique() if g
])

sel_team   = st.sidebar.selectbox("Team",   ["All teams"]   + all_teams)
sel_ground = st.sidebar.selectbox("Ground", ["All grounds"] + all_grounds)
if ipl_only:
    sel_ipl_venue = st.sidebar.selectbox("IPL Venue", ["All IPL venues"] + ipl_grounds)
else:
    sel_ipl_venue = "All IPL venues"
sel_series = st.sidebar.selectbox("Series", ["All series"]  + all_series)
sel_year   = st.sidebar.selectbox("Year",   ["All years"]   + all_years)

# ── Apply filters ──────────────────────────────────────────────────────────────
df = DF.copy()

if ipl_only:
    df = df[df["is_ipl"]]

if sel_team != "All teams":
    df = df[(df["inn1_team"] == sel_team) | (df["inn2_team"] == sel_team)]
if sel_ground != "All grounds":
    df = df[df["ground"] == sel_ground]
if ipl_only and sel_ipl_venue != "All IPL venues":
    df = df[df["ground"] == sel_ipl_venue]
if sel_series != "All series":
    df = df[df["series"] == sel_series]
if sel_year != "All years":
    df = df[df["year"] == sel_year]

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("## 🏏 IPL Stats Dashboard")
st.caption(f"Showing **{len(df)}** matches · [← Back to Match List](http://127.0.0.1:8000)")
st.divider()

if df.empty:
    st.warning("No matches found for the selected filters.")
    st.stop()

# ── KPI row ────────────────────────────────────────────────────────────────────
inn1 = df["inn1_runs"].dropna()

avg_inn1   = round(inn1.mean(), 1)   if len(inn1) > 0 else 0
p90_val    = round(inn1.quantile(0.90)) if len(inn1) > 0 else 0
p75_val    = round(inn1.quantile(0.75)) if len(inn1) > 0 else 0
top10_n    = int((inn1 >= p90_val).sum())
top25_n    = int(((inn1 >= p75_val) & (inn1 < p90_val)).sum())
max_inn1   = int(inn1.max())         if len(inn1) > 0 else 0

toss_df    = df[df["toss"].str.contains("elected to", case=False, na=False)]
toss_total = len(toss_df)
bat_n      = int(toss_df["toss"].str.contains("elected to bat",   case=False).sum())
field_n    = int(toss_df["toss"].str.contains("elected to field", case=False).sum())
bat_pct    = round(100 * bat_n   / toss_total, 1) if toss_total else 0
field_pct  = round(100 * field_n / toss_total, 1) if toss_total else 0

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Avg 1st Inn Score",  avg_inn1)
k2.metric("Max 1st Inn Score",  max_inn1)
k3.metric(f"Top 0–10% (≥{p90_val})",  f"{top10_n} matches")
k4.metric(f"Top 10–25% ({p75_val}–{p90_val})", f"{top25_n} matches")
k5.metric(f"Chose to Bat",   f"{bat_n}  ({bat_pct}%)")
k6.metric(f"Chose to Field", f"{field_n}  ({field_pct}%)")

st.divider()

# ── Row 1: Score distribution  +  Toss decision ───────────────────────────────
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("1st Innings Score Distribution")
    fig_hist = px.histogram(
        inn1, nbins=30,
        labels={"value": "1st Inn Runs", "count": "Matches"},
        color_discrete_sequence=["#0f3460"],
    )
    fig_hist.add_vline(
        x=avg_inn1, line_dash="dash", line_color="#e94560",
        annotation=dict(
            text=f"Avg<br><b>{avg_inn1}</b>",
            font=dict(color="#e94560", size=12),
            bgcolor="white", bordercolor="#e94560", borderwidth=1,
            yref="paper", y=0.98, yanchor="top",
        ),
        annotation_position="top left",
    )
    fig_hist.add_vline(
        x=p90_val, line_dash="dot", line_color="#7c3aed",
        annotation=dict(
            text=f"90th pct<br><b>{p90_val}</b>",
            font=dict(color="#7c3aed", size=12),
            bgcolor="white", bordercolor="#7c3aed", borderwidth=1,
            yref="paper", y=0.80, yanchor="top",
        ),
        annotation_position="top right",
    )
    fig_hist.update_layout(
        showlegend=False,
        margin=dict(t=30, b=20, l=10, r=10),
        height=340,
        plot_bgcolor="#f8f9fb",
        yaxis=dict(gridcolor="#e5e9f0"),
        xaxis=dict(gridcolor="#e5e9f0"),
    )
    st.plotly_chart(fig_hist, use_container_width=True)

with col2:
    st.subheader("Toss Decision Split")
    if toss_total > 0:
        fig_pie = go.Figure(go.Pie(
            labels=["Bat", "Field"],
            values=[bat_n, field_n],
            hole=0.5,
            marker_colors=["#2563eb", "#059669"],
            textinfo="label+percent",
        ))
        fig_pie.update_layout(margin=dict(t=20, b=20), height=320, showlegend=False)
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No toss data available.")

st.divider()

# ── Row 2: Avg 1st inn by year  +  Avg 1st inn by venue ──────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("Avg 1st Innings Score by Year")
    yr_df = (
        df[df["inn1_runs"].notna()]
        .groupby("year")["inn1_runs"]
        .agg(avg="mean", count="count")
        .reset_index()
    )
    yr_df["avg"] = yr_df["avg"].round(1)
    yr_df = yr_df[yr_df["year"] != ""].sort_values("year")
    fig_yr = px.bar(
        yr_df, x="year", y="avg",
        text="avg",
        labels={"year": "Year", "avg": "Avg Runs"},
        color_discrete_sequence=["#0f3460"],
    )
    fig_yr.update_traces(textposition="outside")
    fig_yr.update_layout(margin=dict(t=20, b=20), height=340, yaxis_range=[0, yr_df["avg"].max() * 1.15])
    st.plotly_chart(fig_yr, use_container_width=True)

with col4:
    st.subheader("Avg 1st Innings Score by Venue (top 15)")
    venue_df = (
        df[df["inn1_runs"].notna()]
        .groupby("ground")["inn1_runs"]
        .agg(avg="mean", count="count")
        .reset_index()
    )
    venue_df = venue_df[venue_df["count"] >= 3].sort_values("avg", ascending=False).head(15)
    venue_df["avg"] = venue_df["avg"].round(1)
    venue_df["ground_short"] = venue_df["ground"].str.split(",").str[0].str[:30]
    fig_venue = px.bar(
        venue_df, x="avg", y="ground_short",
        orientation="h", text="avg",
        labels={"avg": "Avg Runs", "ground_short": "Venue"},
        color_discrete_sequence=["#16213e"],
    )
    fig_venue.update_traces(textposition="outside")
    fig_venue.update_layout(margin=dict(t=20, b=20), height=340, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_venue, use_container_width=True)

st.divider()

# ── Row 3: Highest 1st inn scores table ───────────────────────────────────────
st.subheader("Top 20 Highest 1st Innings Scores")
top_scores = (
    df[df["inn1_runs"].notna()]
    .sort_values("inn1_runs", ascending=False)
    .head(20)[["date", "inn1_team", "inn1_score", "inn2_team", "inn2_score", "result", "ground", "series"]]
    .rename(columns={
        "date": "Date", "inn1_team": "Batting Team", "inn1_score": "1st Inn",
        "inn2_team": "Chasing Team", "inn2_score": "2nd Inn",
        "result": "Result", "ground": "Ground", "series": "Series",
    })
    .reset_index(drop=True)
)
top_scores.index += 1
top_scores["Date"] = pd.to_datetime(top_scores["Date"]).dt.strftime("%d %b %Y")
st.dataframe(top_scores, use_container_width=True, height=420)
