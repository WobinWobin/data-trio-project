"""Auto-written by the v4 notebook. Don't edit by hand.
If you do edit, restart the kernel before re-running."""
from __future__ import annotations
import re, time
from typing import Optional
from curl_cffi import requests
from bs4 import BeautifulSoup

LBX_BASE  = "https://letterboxd.com"
TMDB_BASE = "https://api.themoviedb.org/3"

_SESSION: Optional[requests.Session] = None
def session():
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session(impersonate="chrome124")
        _SESSION.headers.update({
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
    return _SESSION

def get_with_retry(url, *, max_retries=5, timeout=20):
    backoff = 1.0
    for _ in range(max_retries):
        try:
            r = session().get(url, timeout=timeout)
        except Exception:
            time.sleep(backoff); backoff *= 2; continue
        if r.status_code == 200:
            if len(r.text) < 8000 and "Just a moment" in r.text:
                time.sleep(backoff); backoff *= 2; continue
            return r
        if r.status_code == 404:
            return r
        if r.status_code in (403, 429, 500, 502, 503, 504):
            time.sleep(backoff); backoff *= 2; continue
        return r
    return None

RATING_RE    = re.compile(r"rated-(\d+)")
TMDB_LINK_RE = re.compile(r"themoviedb\.org/movie/(\d+)")

def scrape_user_films(username, max_pages=1000):
    out, seen = [], set()
    page = 1
    while page <= max_pages:
        r = get_with_retry(f"{LBX_BASE}/{username}/films/page/{page}/")
        if r is None or r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        items = (soup.select("li.poster-container")
                 or soup.select("li.griditem")
                 or soup.select("li[data-owner-rating], li[data-film-id]"))
        if not items:
            break
        new = 0
        for item in items:
            poster = item.select_one(
                "div[data-film-slug], div[data-target-link^='/film/'], "
                "div.film-poster, div.really-lazy-load"
            )
            if poster is None and item.get("data-film-slug"):
                poster = item
            if poster is None:
                continue
            slug = poster.get("data-film-slug")
            if not slug:
                target = poster.get("data-target-link") or ""
                if target.startswith("/film/"):
                    slug = target.strip("/").split("/")[-1]
            if not slug or slug in seen:
                continue
            seen.add(slug)
            img = poster.select_one("img")
            title = (img.get("alt") if img else None) or slug.replace("-", " ").title()
            rating = None
            owner = item.get("data-owner-rating")
            if owner and owner not in ("0", ""):
                try: rating = int(owner) / 2.0
                except (TypeError, ValueError): pass
            if rating is None:
                rs = item.select_one("span.rating")
                if rs:
                    m = RATING_RE.search(" ".join(rs.get("class", [])))
                    if m: rating = int(m.group(1)) / 2.0
            out.append({"username": username, "slug": slug, "title": title, "rating": rating})
            new += 1
        if new == 0 or not soup.select_one("div.paginate-nextprev a.next"):
            break
        page += 1
    return out

def resolve_tmdb_id(slug):
    r = get_with_retry(f"{LBX_BASE}/film/{slug}/")
    if r is None or r.status_code != 200:
        return {"slug": slug, "tmdb_id": None, "title": None}
    m = TMDB_LINK_RE.search(r.text)
    tmdb_id = int(m.group(1)) if m else None
    soup = BeautifulSoup(r.text, "html.parser")
    title_tag = soup.select_one("h1.headline-1, h1.filmtitle, h1")
    title = title_tag.get_text(strip=True) if title_tag else None
    return {"slug": slug, "tmdb_id": tmdb_id, "title": title}

def fetch_tmdb(args):
    tmdb_id, api_key = args
    url = f"{TMDB_BASE}/movie/{tmdb_id}?api_key={api_key}&language=en-US"
    backoff = 0.5
    for _ in range(5):
        try:
            r = session().get(url, timeout=20)
        except Exception:
            time.sleep(backoff); backoff *= 2; continue
        if r.status_code == 200:
            d = r.json()
            return {"tmdb_id": tmdb_id,
                    "title": d.get("title") or d.get("original_title"),
                    "genres": [g["name"] for g in d.get("genres", [])]}
        if r.status_code == 429:
            time.sleep(float(r.headers.get("Retry-After", backoff))); backoff *= 2; continue
        if r.status_code in (500, 502, 503, 504):
            time.sleep(backoff); backoff *= 2; continue
        return {"tmdb_id": tmdb_id, "title": None, "genres": []}
    return {"tmdb_id": tmdb_id, "title": None, "genres": []}

def phase1_worker(username):
    """Re-raises errors instead of silently swallowing — Pool will surface them."""
    return username, scrape_user_films(username)
