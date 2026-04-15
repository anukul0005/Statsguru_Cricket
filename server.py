"""
IPL Stats – FastAPI server
Run: uvicorn server:app --reload
"""

import math
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

CSV_PATH = Path("match_summary.csv")
PAGE_SIZE = 25

STREAMLIT = str(Path(__file__).parent / "env311" / "Scripts" / "streamlit.exe")

@asynccontextmanager
async def lifespan(_: FastAPI):
    proc = subprocess.Popen(
        [STREAMLIT, "run", "dashboard.py",
         "--server.port", "8501", "--server.headless", "true"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=Path(__file__).parent,
    )
    yield
    proc.terminate()

app = FastAPI(title="India T20 Match Stats", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ── Load data once at startup ─────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH, dtype=str).fillna("")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date", ascending=False)
    df["date_str"] = df["date"].dt.strftime("%d %b %Y")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df

DF: pd.DataFrame = load_data()

IPL_SERIES_PATTERN = "Premier League"

def get_filter_options():
    teams = sorted(
        set(DF["inn1_team"].unique()) | set(DF["inn2_team"].unique())
    )
    teams = [t for t in teams if t]
    grounds = sorted([g for g in DF["ground"].unique() if g])
    ipl_grounds = sorted([
        g for g in DF[DF["series"].str.contains(IPL_SERIES_PATTERN, case=False, na=False)]["ground"].unique()
        if g
    ])
    series_list = sorted([s for s in DF["series"].unique() if s])
    years = sorted(
        set(DF["date"].str[:4].unique()) - {""},
        reverse=True,
    )
    return teams, grounds, ipl_grounds, series_list, years


# ── API ───────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    q: Optional[str] = Query(default=""),
    team: Optional[str] = Query(default=""),
    ground: Optional[str] = Query(default=""),
    ipl_venue: Optional[str] = Query(default=""),
    series: Optional[str] = Query(default=""),
    year: Optional[str] = Query(default=""),
    page: int = Query(default=1, ge=1),
):
    df = DF.copy()

    if q:
        mask = (
            df["result"].str.contains(q, case=False, na=False)
            | df["inn1_team"].str.contains(q, case=False, na=False)
            | df["inn2_team"].str.contains(q, case=False, na=False)
            | df["ground"].str.contains(q, case=False, na=False)
            | df["inn1_bat1_name"].str.contains(q, case=False, na=False)
            | df["inn2_bat1_name"].str.contains(q, case=False, na=False)
        )
        df = df[mask]

    if team:
        df = df[(df["inn1_team"] == team) | (df["inn2_team"] == team)]
    if ground:
        df = df[df["ground"] == ground]
    if ipl_venue == "__all_ipl__":
        df = df[df["series"].str.contains(IPL_SERIES_PATTERN, case=False, na=False)]
    elif ipl_venue:
        df = df[
            (df["ground"] == ipl_venue)
            & df["series"].str.contains(IPL_SERIES_PATTERN, case=False, na=False)
        ]
    if series:
        df = df[df["series"] == series]
    if year:
        df = df[df["date"].str.startswith(year)]

    # ── Avg 1st innings total + percentile bands (on full filtered set) ────────
    inn1_runs = pd.to_numeric(df["inn1_runs"], errors="coerce").dropna()
    avg_first_innings = round(inn1_runs.mean(), 1) if len(inn1_runs) > 0 else None

    if len(inn1_runs) > 0:
        p90 = round(inn1_runs.quantile(0.90))
        p75 = round(inn1_runs.quantile(0.75))
        top10_count    = int((inn1_runs >= p90).sum())
        top10_25_count = int(((inn1_runs >= p75) & (inn1_runs < p90)).sum())
    else:
        p90 = p75 = top10_count = top10_25_count = None

    # ── Toss decision stats (on full filtered set) ────────────────────────────
    toss_df = df[df["toss"].str.contains("elected to", case=False, na=False)]
    toss_total    = len(toss_df)
    bat_count     = int(toss_df["toss"].str.contains("elected to bat",   case=False, na=False).sum())
    field_count   = int(toss_df["toss"].str.contains("elected to field", case=False, na=False).sum())
    bat_pct       = round(100 * bat_count   / toss_total, 1) if toss_total else 0
    field_pct     = round(100 * field_count / toss_total, 1) if toss_total else 0

    total = len(df)
    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    page = min(page, total_pages)
    start = (page - 1) * PAGE_SIZE
    page_df = df.iloc[start : start + PAGE_SIZE]
    matches = page_df.to_dict(orient="records")

    teams, grounds, ipl_grounds, series_list, years = get_filter_options()

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "matches": matches,
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "page_size": PAGE_SIZE,
            # filter state
            "q": q,
            "sel_team": team,
            "sel_ground": ground,
            "sel_ipl_venue": ipl_venue,
            "sel_series": series,
            "sel_year": year,
            # innings stats
            "avg_first_innings": avg_first_innings,
            "p90": p90,
            "p75": p75,
            "top10_count": top10_count,
            "top10_25_count": top10_25_count,
            # toss stats
            "toss_total": toss_total,
            "bat_count": bat_count,
            "bat_pct": bat_pct,
            "field_count": field_count,
            "field_pct": field_pct,
            # filter options
            "teams": teams,
            "grounds": grounds,
            "ipl_grounds": ipl_grounds,
            "series_list": series_list,
            "years": years,
        },
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=True)
