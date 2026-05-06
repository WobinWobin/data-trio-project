"""
enrich_letterboxd.py
====================
1. Reads all output_1.csv … output_8.csv
2. Derives Letterboxd slug from movie_title
3. Scrapes letterboxd.com/film/<slug>/ for:
       num_ratings, avg_rating, director_url, actors_url
4. Merges everything → letterboxd_enriched.csv

Run on YOUR machine (not in a sandboxed environment):
    python enrich_letterboxd.py

Deps auto-installed on first run.
"""
from __future__ import annotations

import csv
import json
import re
import subprocess
import sys
from pathlib import Path

# ── auto-install deps ─────────────────────────────────────────────────────────
for pkg, mod in [
    ("curl_cffi",     "curl_cffi"),
    ("beautifulsoup4","bs4"),
    ("pandas",        "pandas"),
    ("tqdm",          "tqdm"),
]:
    try:
        __import__(mod)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

import pandas as pd
from tqdm import tqdm

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
INPUT_PATTERN   = "output_{n}.csv"   # output_1.csv … output_8.csv
INPUT_RANGE     = range(1, 9)
OUTPUT_CSV      = "letterboxd_enriched.csv"
FILM_CACHE_FILE = "film_cache.jsonl" # resumable — never delete mid-run
WORKERS         = 16
MAX_ACTORS      = 5
LBX_BASE        = "https://letterboxd.com"
# ══════════════════════════════════════════════════════════════════════════════


# ── write helpers module to disk so Pool workers can import it ────────────────
HELPERS_PATH = Path("_enrich_helpers.py")
HELPERS_SRC = (
    '"""Auto-generated — do not edit by hand."""\n'
    "from __future__ import annotations\n"
    "import json, re, time\n"
    "from typing import Optional\n"
    "from curl_cffi import requests as cffi_requests\n"
    "from bs4 import BeautifulSoup\n"
    "\n"
    'LBX_BASE   = "https://letterboxd.com"\n'
    "MAX_ACTORS = 5\n"
    "\n"
    "_SESSION: Optional[cffi_requests.Session] = None\n"
    "\n"
    "def session():\n"
    "    global _SESSION\n"
    "    if _SESSION is None:\n"
    '        _SESSION = cffi_requests.Session(impersonate="chrome124")\n'
    "        _SESSION.headers.update({\n"
    '            "Accept-Language": "en-US,en;q=0.9",\n'
    '            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",\n'
    "        })\n"
    "    return _SESSION\n"
    "\n"
    "\n"
    "def get_with_retry(url, *, max_retries=6, timeout=25):\n"
    "    backoff = 1.0\n"
    "    for _ in range(max_retries):\n"
    "        try:\n"
    "            r = session().get(url, timeout=timeout)\n"
    "        except Exception:\n"
    "            time.sleep(backoff); backoff = min(backoff * 2, 60); continue\n"
    "        if r.status_code == 200:\n"
    '            if len(r.text) < 8000 and "Just a moment" in r.text:\n'
    "                time.sleep(backoff); backoff = min(backoff * 2, 60); continue\n"
    "            return r\n"
    "        if r.status_code == 404:\n"
    "            return r\n"
    "        if r.status_code in (403, 429, 500, 502, 503, 504):\n"
    "            wait = backoff\n"
    "            if r.status_code == 429:\n"
    "                try: wait = float(r.headers.get('Retry-After', backoff))\n"
    "                except (TypeError, ValueError): pass\n"
    "            time.sleep(wait); backoff = min(backoff * 2, 120); continue\n"
    "        return r\n"
    "    return None\n"
    "\n"
    "\n"
    "def scrape_film_page(slug):\n"
    "    empty = dict(slug=slug, num_ratings=None, avg_rating=None,\n"
    "                 director_url=None, actors_url=None)\n"
    "    url = f'{LBX_BASE}/film/{slug}/'\n"
    "    r = get_with_retry(url)\n"
    "    if r is None or r.status_code != 200:\n"
    "        return empty\n"
    "    text = r.text\n"
    "    soup = BeautifulSoup(text, 'html.parser')\n"
    "\n"
    "    # avg_rating and num_ratings from JSON-LD\n"
    "    avg_rating = None\n"
    "    num_ratings = None\n"
    "    for tag in soup.select('script[type=\"application/ld+json\"]'):\n"
    "        raw = tag.string or ''\n"
    "        if not raw.strip(): continue\n"
    "        try: d = json.loads(raw)\n"
    "        except Exception: continue\n"
    "        agg = d.get('aggregateRating') or {}\n"
    "        if agg.get('ratingValue') and avg_rating is None:\n"
    "            try: avg_rating = float(agg['ratingValue'])\n"
    "            except (TypeError, ValueError): pass\n"
    "        if agg.get('ratingCount') and num_ratings is None:\n"
    "            try: num_ratings = int(agg['ratingCount'])\n"
    "            except (TypeError, ValueError): pass\n"
    "        if avg_rating is not None and num_ratings is not None: break\n"
    "\n"
    "    # fallbacks from inline JS\n"
    "    if num_ratings is None:\n"
    "        m = re.search(r'\"ratingCount\"\\s*:\\s*(\\d+)', text)\n"
    "        if m:\n"
    "            try: num_ratings = int(m.group(1))\n"
    "            except ValueError: pass\n"
    "    if avg_rating is None:\n"
    "        m = re.search(r'\"ratingValue\"\\s*:\\s*([0-9.]+)', text)\n"
    "        if m:\n"
    "            try: avg_rating = float(m.group(1))\n"
    "            except ValueError: pass\n"
    "    if avg_rating is None:\n"
    "        m = re.search(r'weightedAverage[\"\\']?\\s*:\\s*([0-9.]+)', text)\n"
    "        if m:\n"
    "            try: avg_rating = float(m.group(1))\n"
    "            except ValueError: pass\n"
    "\n"
    "    # num_ratings fallback from filmstat link\n"
    "    if num_ratings is None:\n"
    "        for sel in ('li.filmstat-watches a', 'a.icon-watched', '.filmstat-watches .value'):\n"
    "            el = soup.select_one(sel)\n"
    "            if el:\n"
    "                raw = (el.get('title') or el.get_text(strip=True) or '').replace(',', '').strip()\n"
    "                m2 = re.match(r'([0-9.]+)([KkMm]?)', raw)\n"
    "                if m2:\n"
    "                    val = float(m2.group(1))\n"
    "                    suf = m2.group(2).upper()\n"
    "                    if suf == 'K': val *= 1_000\n"
    "                    elif suf == 'M': val *= 1_000_000\n"
    "                    num_ratings = int(val); break\n"
    "\n"
    "    # director_url\n"
    "    director_url = None\n"
    "    for sel in (\n"
    "        \"#film-crew a[href*='/director/']\",\n"
    "        \".crew-list a[href*='/director/']\",\n"
    "        \"span.directorlist a[href*='/director/']\",\n"
    "        \"a[href*='/director/']\",\n"
    "    ):\n"
    "        el = soup.select_one(sel)\n"
    "        if el:\n"
    "            href = el.get('href', '')\n"
    "            if href: director_url = '/' + href.strip('/') + '/'; break\n"
    "\n"
    "    # actors_url\n"
    "    actors_url = None\n"
    "    cast_els = []\n"
    "    for sel in (\n"
    "        \"#film-cast a[href*='/actor/']\",\n"
    "        \".cast-list a[href*='/actor/']\",\n"
    "        \"a[href*='/actor/']\",\n"
    "    ):\n"
    "        cast_els = soup.select(sel)\n"
    "        if cast_els: break\n"
    "    if cast_els:\n"
    "        seen = []\n"
    "        for a in cast_els:\n"
    "            href = a.get('href', '')\n"
    "            if '/actor/' not in href: continue\n"
    "            clean = '/' + href.strip('/') + '/'\n"
    "            if clean not in seen: seen.append(clean)\n"
    "            if len(seen) >= MAX_ACTORS: break\n"
    "        if seen: actors_url = ','.join(seen)\n"
    "\n"
    "    return dict(slug=slug, num_ratings=num_ratings, avg_rating=avg_rating,\n"
    "                director_url=director_url, actors_url=actors_url)\n"
    "\n"
    "\n"
    "def worker(slug):\n"
    "    return scrape_film_page(slug)\n"
)

HELPERS_PATH.write_text(HELPERS_SRC, encoding="utf-8")
print(f"Helpers written to {HELPERS_PATH}")

sys.path.insert(0, str(Path(".").resolve()))


# ── utilities ─────────────────────────────────────────────────────────────────

def derive_slug(title):
    """Convert movie title to Letterboxd URL slug."""
    if not title or not isinstance(title, str):
        return ""
    s = title.lower()
    s = re.sub(r"['''\u2018\u2019]", "", s)
    s = re.sub(r"[^a-z0-9\s-]", " ", s)
    s = re.sub(r"\s+", "-", s.strip())
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def load_all_csvs(pattern, rng):
    frames = []
    for n in rng:
        path = Path(pattern.replace("{n}", str(n)))
        if not path.exists():
            print(f"  [skip] {path} — not found")
            continue
        df = pd.read_csv(path, dtype=str)
        frames.append(df)
        print(f"  [load] {str(path):20s}  {len(df):>8,} rows")
    if not frames:
        raise FileNotFoundError(
            "No output_n.csv files found. "
            "Make sure INPUT_PATTERN and INPUT_RANGE match your files."
        )
    combined = pd.concat(frames, ignore_index=True)
    print(f"\n  Total rows : {len(combined):,}")
    print(f"  Users      : {combined['username'].nunique():,}")
    print(f"  Titles     : {combined['movie_title'].nunique():,}")
    return combined


def load_cache(cache_file):
    cache = {}
    p = Path(cache_file)
    if p.exists():
        with p.open(encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                    cache[rec["slug"]] = rec
                except Exception:
                    pass
    print(f"  Cache: {len(cache):,} entries loaded from {cache_file}")
    return cache


def scrape_missing(slugs_todo, cache_file, workers):
    from multiprocessing import Pool
    from _enrich_helpers import worker as _worker

    if not slugs_todo:
        print("  Nothing to scrape — all cached.")
        return {}

    print(f"  {len(slugs_todo):,} slugs to scrape  ({workers} workers)")
    new_results = {}
    fh = open(cache_file, "a", buffering=1, encoding="utf-8")
    try:
        with Pool(processes=workers) as pool:
            for rec in tqdm(
                pool.imap_unordered(_worker, slugs_todo, chunksize=4),
                total=len(slugs_todo),
                desc="Scraping films",
                unit="film",
            ):
                fh.write(json.dumps(rec) + "\n")
                new_results[rec["slug"]] = rec
    finally:
        fh.close()
    print(f"  Scraped {len(new_results):,} new film pages")
    return new_results


def merge_and_write(df, cache, output_csv):
    film_df = pd.DataFrame(list(cache.values()))
    # ensure columns exist even if cache is empty
    for col in ["num_ratings", "avg_rating", "director_url", "actors_url"]:
        if col not in film_df.columns:
            film_df[col] = None

    film_df = film_df[["slug", "num_ratings", "avg_rating",
                        "director_url", "actors_url"]]

    merged = df.merge(film_df, on="slug", how="left")
    merged = merged.drop(columns=["slug"])  # was only a join key

    merged.to_csv(output_csv, index=False, quoting=csv.QUOTE_MINIMAL)
    size_mb = Path(output_csv).stat().st_size / 1024 / 1024
    print(f"\n  Written → {output_csv}  ({len(merged):,} rows, {size_mb:.1f} MB)")
    print(f"  num_ratings  filled: {merged['num_ratings'].notna().sum():,}")
    print(f"  avg_rating   filled: {merged['avg_rating'].notna().sum():,}")
    print(f"  director_url filled: {merged['director_url'].notna().sum():,}")
    print(f"  actors_url   filled: {merged['actors_url'].notna().sum():,}")
    return merged


# ── main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("STEP 1 — Load output_n.csv files")
    print("=" * 60)
    df = load_all_csvs(INPUT_PATTERN, INPUT_RANGE)

    print("\n" + "=" * 60)
    print("STEP 2 — Derive Letterboxd slugs from movie_title")
    print("=" * 60)
    df["slug"] = df["movie_title"].apply(derive_slug)
    unique_slugs = [s for s in df["slug"].unique() if s]
    print(f"  Unique slugs: {len(unique_slugs):,}")

    print("\n" + "=" * 60)
    print("STEP 3 — Load film cache (resume support)")
    print("=" * 60)
    cache = load_cache(FILM_CACHE_FILE)
    slugs_todo = [s for s in unique_slugs if s not in cache]
    print(f"  Already cached : {len(unique_slugs) - len(slugs_todo):,}")
    print(f"  To scrape now  : {len(slugs_todo):,}")

    print("\n" + "=" * 60)
    print("STEP 4 — Scrape Letterboxd film pages")
    print("=" * 60)
    new_results = scrape_missing(slugs_todo, FILM_CACHE_FILE, WORKERS)
    cache.update(new_results)

    print("\n" + "=" * 60)
    print("STEP 5 — Merge & write final CSV")
    print("=" * 60)
    merge_and_write(df, cache, OUTPUT_CSV)

    print("\nAll done.")
