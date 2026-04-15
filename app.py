"""
India T20 Match Scorecard Scraper
----------------------------------
Scrapes scorecard summaries for all T20 matches played in India (2008-present)
from ESPN Cricinfo Statsguru (3070 matches).

For every match the output contains:
  - Match metadata  : date, ground, result, toss
  - Innings 1 & 2   : team, total (runs/wkts/overs)
                      top-2 batsmen (name, runs, balls, not-out*)
                      top-2 bowlers (name, wkts/runs, overs)

Output files:
  match_ids.csv      - match IDs collected from Statsguru
  match_summary.csv  - one row per match with full summary
"""

import re
import time
import random
import logging
import csv
from pathlib import Path
from typing import Optional

from curl_cffi import requests as cf_requests
from bs4 import BeautifulSoup
import json
import pandas as pd

# ── Config ──────────────────────────────────────────────────────────────────

STATSGURU_URL = (
    "https://stats.espncricinfo.com/ci/engine/stats/index.html"
    "?class=6;host=6;spanmax2=31+Dec+2100;spanmin2=01+Jan+2008"
    ";spanval2=span;template=results;type=aggregate;view=results"
)
MATCH_URL     = "https://www.espncricinfo.com/ci/engine/match/{match_id}.html"
HOMEPAGE_URL  = "https://www.espncricinfo.com/"

MATCH_IDS_FILE   = Path("match_ids.csv")
SUMMARY_FILE     = Path("match_summary.csv")

MIN_DELAY     = 2.5   # seconds between match requests
MAX_DELAY     = 5.0
REWARM_EVERY  = 50    # re-visit homepage every N matches to refresh cookies

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Session helpers ──────────────────────────────────────────────────────────

def make_session() -> cf_requests.Session:
    return cf_requests.Session(impersonate="chrome124")


def warm_session(session: cf_requests.Session):
    """Visit homepage to acquire session cookies."""
    try:
        r = session.get(HOMEPAGE_URL, timeout=20)
        log.info("Session warmed (homepage HTTP %s)", r.status_code)
    except Exception as e:
        log.warning("Homepage warm-up failed: %s", e)


def polite_get(
    session: cf_requests.Session, url: str, retries: int = 3
) -> Optional[cf_requests.Response]:
    """GET with exponential back-off; returns None on persistent failure."""
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=25)
            if resp.status_code == 200:
                return resp
            log.warning("HTTP %s for %s (attempt %d)", resp.status_code, url, attempt)
        except Exception as exc:
            log.warning("Request error: %s (attempt %d)", exc, attempt)
        backoff = MIN_DELAY * (2 ** attempt) + random.uniform(0, 1)
        log.info("Sleeping %.1fs before retry …", backoff)
        time.sleep(backoff)
    return None


def sleep_politely():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

# ── Step 1 – Collect match IDs from Statsguru ───────────────────────────────

def collect_match_ids(session: cf_requests.Session) -> list:
    if MATCH_IDS_FILE.exists():
        log.info("Loading existing match IDs from %s", MATCH_IDS_FILE)
        return pd.read_csv(MATCH_IDS_FILE)["match_id"].astype(str).tolist()

    match_ids = []
    page = 1

    while True:
        url = f"{STATSGURU_URL};page={page}"
        log.info("Fetching Statsguru page %d …", page)
        resp = polite_get(session, url)
        if resp is None:
            log.error("Cannot fetch page %d – stopping.", page)
            break

        soup = BeautifulSoup(resp.text, "lxml")
        # Each result row renders as its own div.engine-dd containing exactly one match link.
        new_ids = []
        for edd in soup.find_all("div", class_="engine-dd"):
            a = edd.find("a", href=re.compile(r"/ci/engine/match/\d+\.html"))
            if a:
                new_ids.append(re.search(r"/match/(\d+)\.html", a["href"]).group(1))
        if not new_ids:
            log.info("No match links on page %d – last page reached.", page)
            break

        match_ids.extend(new_ids)
        log.info("  Page %d: +%d matches (total %d)", page, len(new_ids), len(match_ids))

        # PaginationLink anchors contain an <img> child so string= never matches.
        # Instead detect the "Next" PaginationLink by its text content.
        pagination_links = soup.find_all("a", class_="PaginationLink")
        has_next = any(
            "next" in a.get_text(strip=True).lower()
            for a in pagination_links
        )
        if not has_next:
            log.info("No 'Next' pagination link found – done.")
            break

        page += 1
        sleep_politely()

    # Deduplicate, preserve order
    seen: set = set()
    unique = [x for x in match_ids if not (x in seen or seen.add(x))]
    log.info("Unique match IDs: %d", len(unique))
    pd.DataFrame({"match_id": unique}).to_csv(MATCH_IDS_FILE, index=False)
    log.info("Saved → %s", MATCH_IDS_FILE)
    return unique

# ── Step 2 – Parse a match page ──────────────────────────────────────────────

def _top_batsmen(inn: dict, n: int = 2) -> list:
    bats = sorted(
        inn.get("inningBatsmen", []),
        key=lambda x: x.get("runs") or 0,
        reverse=True,
    )
    return bats[:n]


def _top_bowlers(inn: dict, n: int = 2) -> list:
    bowls = sorted(
        inn.get("inningBowlers", []),
        key=lambda x: (x.get("wickets") or 0, -((x.get("conceded") or 999))),
        reverse=True,
    )
    return bowls[:n]


def parse_match_page(match_id: str, html: str) -> Optional[dict]:
    """
    Returns a flat dict with one row of summary data, or None if parsing fails.
    """
    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not m:
        log.warning("No __NEXT_DATA__ in match %s", match_id)
        return None

    try:
        raw = json.loads(m.group(1))
        page_data = raw["props"]["appPageProps"]["data"]
        match   = page_data["match"]
        content = page_data["content"]
    except (KeyError, json.JSONDecodeError) as e:
        log.warning("JSON parse error for match %s: %s", match_id, e)
        return None

    # ── Match metadata ──
    ground  = match.get("ground", {})
    teams   = {t["team"]["id"]: t["team"]["longName"] for t in match.get("teams", [])}

    toss_winner_id = match.get("tossWinnerTeamId")
    toss_choice    = match.get("tossWinnerChoice")   # 1=bat, 2=field
    toss_choice_str = (
        "bat" if toss_choice == 1 else "field" if toss_choice == 2 else ""
    )

    row: dict = {
        "match_id"   : match_id,
        "title"      : match.get("title", ""),
        "date"       : (match.get("startDate") or "")[:10],
        "ground"     : ground.get("longName", ""),
        "city"       : ground.get("town", {}).get("longName", "") if isinstance(ground.get("town"), dict) else "",
        "result"     : match.get("statusText", ""),
        "toss"       : f"{teams.get(toss_winner_id, '')} elected to {toss_choice_str}",
        "series"     : match.get("series", {}).get("longName", "") if isinstance(match.get("series"), dict) else "",
    }

    # ── Innings ──
    innings = content.get("innings", [])
    for idx in range(2):
        prefix = f"inn{idx + 1}"
        if idx < len(innings):
            inn = innings[idx]
            team  = inn.get("team", {}).get("longName", "")
            runs  = inn.get("runs", "")
            wkts  = inn.get("wickets", "")
            overs = inn.get("overs", "")
            row[f"{prefix}_team"]    = team
            row[f"{prefix}_score"]   = f"{runs}/{wkts} ({overs} Overs)"
            row[f"{prefix}_runs"]    = runs
            row[f"{prefix}_wickets"] = wkts
            row[f"{prefix}_overs"]   = overs

            # Top 2 batsmen
            for bi, bat in enumerate(_top_batsmen(inn), 1):
                p = bat.get("player", {})
                not_out = "*" if not bat.get("isOut") else ""
                row[f"{prefix}_bat{bi}_name"]   = p.get("longName", "") + not_out
                row[f"{prefix}_bat{bi}_runs"]   = bat.get("runs", "")
                row[f"{prefix}_bat{bi}_balls"]  = bat.get("balls", "")

            # Top 2 bowlers
            for bi, bowl in enumerate(_top_bowlers(inn), 1):
                p = bowl.get("player", {})
                row[f"{prefix}_bowl{bi}_name"]    = p.get("longName", "")
                row[f"{prefix}_bowl{bi}_wickets"] = bowl.get("wickets", "")
                row[f"{prefix}_bowl{bi}_runs"]    = bowl.get("conceded", "")
                row[f"{prefix}_bowl{bi}_overs"]   = bowl.get("overs", "")
        else:
            # Pad missing innings with empty strings
            row[f"{prefix}_team"] = row[f"{prefix}_runs"] = ""
            row[f"{prefix}_wickets"] = row[f"{prefix}_overs"] = ""
            for bi in range(1, 3):
                for f in ["name", "runs", "balls"]:
                    row[f"{prefix}_bat{bi}_{f}"] = ""
                for f in ["name", "wickets", "runs", "overs"]:
                    row[f"{prefix}_bowl{bi}_{f}"] = ""

    return row

# ── Step 3 – Main scraping loop ──────────────────────────────────────────────

def already_scraped() -> set:
    if not SUMMARY_FILE.exists():
        return set()
    try:
        df = pd.read_csv(SUMMARY_FILE, usecols=["match_id"])
        return set(df["match_id"].astype(str).unique())
    except Exception:
        return set()


def append_row(row: dict):
    write_header = not SUMMARY_FILE.exists()
    with open(SUMMARY_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def scrape_scorecards(session: cf_requests.Session, match_ids: list):
    done = already_scraped()
    remaining = [m for m in match_ids if m not in done]
    log.info("To scrape: %d  |  Already done: %d", len(remaining), len(done))

    for i, match_id in enumerate(remaining, 1):
        # Periodically re-warm session
        if i % REWARM_EVERY == 0:
            log.info("Re-warming session …")
            warm_session(session)
            time.sleep(random.uniform(2, 4))

        url = MATCH_URL.format(match_id=match_id)
        log.info("[%d/%d] Match %s", i, len(remaining), match_id)

        resp = polite_get(session, url)
        if resp is None:
            # Session might have gone stale – try re-warming once
            log.warning("Fetch failed – re-warming session and retrying …")
            warm_session(session)
            time.sleep(3)
            resp = polite_get(session, url)

        if resp is None:
            log.error("Skipping match %s after retries.", match_id)
            sleep_politely()
            continue

        row = parse_match_page(match_id, resp.text)
        if row:
            append_row(row)
        else:
            log.warning("No scorecard data parsed for match %s", match_id)

        sleep_politely()

        if i % 100 == 0:
            log.info("── Checkpoint: %d scraped ──", i)

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    session = make_session()

    log.info("=== Warming session ===")
    warm_session(session)

    log.info("=== Step 1: Collect match IDs from Statsguru ===")
    match_ids = collect_match_ids(session)

    log.info("=== Step 2: Scrape scorecards ===")
    scrape_scorecards(session, match_ids)

    log.info("=== Done ===")
    for f in [MATCH_IDS_FILE, SUMMARY_FILE]:
        if f.exists():
            rows = sum(1 for _ in open(f)) - 1
            log.info("  %-22s  %d rows  (%.1f KB)", f.name, rows, f.stat().st_size / 1024)
