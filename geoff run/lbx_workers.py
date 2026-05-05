"""Worker functions for the Letterboxd scrape notebook.

This file is imported by `letterboxd_scrape.ipynb` so that
multiprocessing.Pool workers can pickle these functions on macOS (spawn).
"""
from __future__ import annotations

import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

LBX_BASE = "https://letterboxd.com"
TMDB_BASE = "https://api.themoviedb.org/3"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Session helper — one Session per worker process, reused across calls.
# ---------------------------------------------------------------------------
_SESSION: Optional[requests.Session] = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
        _SESSION = s
    return _SESSION


def _get(url: str, *, max_retries: int = 4, timeout: int = 25) -> Optional[requests.Response]:
    """GET with simple exponential backoff for 429s and transient errors."""
    backoff = 2.0
    for attempt in range(max_retries):
        try:
            r = _session().get(url, timeout=timeout)
        except requests.RequestException:
            time.sleep(backoff)
            backoff *= 2
            continue
        if r.status_code == 200:
            return r
        if r.status_code == 404:
            return r  # caller decides
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff *= 2
            continue
        return r
    return None


# ---------------------------------------------------------------------------
# Phase 1 — scrape one user's full /films/ list, paginated.
# ---------------------------------------------------------------------------
RATING_RE = re.compile(r"rated-(\d+)")


def scrape_user_films(username: str, max_pages: int = 1000) -> list[dict]:
    """Return list of {username, slug, title, rating} for every film the user logged.

    Letterboxd shows 72 posters per page on /username/films/page/N/.
    Rating class `rated-N` maps to N/2 stars (1=0.5 ... 10=5.0).
    Unrated films will have rating=None.
    """
    out: list[dict] = []
    seen_slugs: set[str] = set()
    page = 1
    while page <= max_pages:
        url = f"{LBX_BASE}/{username}/films/page/{page}/"
        r = _get(url)
        if r is None or r.status_code == 404:
            break
        if r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select("li.poster-container")
        if not items:
            break
        new_this_page = 0
        for item in items:
            poster = item.select_one("div[data-film-slug], div.film-poster, div.really-lazy-load")
            if poster is None:
                continue
            slug = poster.get("data-film-slug")
            if not slug:
                target = poster.get("data-target-link") or ""
                if target.startswith("/film/"):
                    slug = target.strip("/").split("/")[-1]
            if not slug:
                continue
            img = poster.select_one("img")
            title = (img.get("alt") if img else None) or slug.replace("-", " ").title()

            rating: Optional[float] = None
            # Preferred: <li data-owner-rating="N"> (only present if rated)
            owner_rating = item.get("data-owner-rating")
            if owner_rating and owner_rating not in ("0", ""):
                try:
                    rating = int(owner_rating) / 2.0
                except (TypeError, ValueError):
                    rating = None
            if rating is None:
                rating_span = item.select_one("span.rating")
                if rating_span is not None:
                    cls = " ".join(rating_span.get("class", []))
                    m = RATING_RE.search(cls)
                    if m:
                        rating = int(m.group(1)) / 2.0

            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            new_this_page += 1
            out.append({
                "username": username,
                "slug": slug,
                "title": title,
                "rating": rating,
            })
        # Pagination: stop if the page returned fewer than expected OR no .next link.
        if new_this_page == 0:
            break
        next_link = soup.select_one("div.paginate-nextprev a.next")
        if not next_link:
            break
        page += 1
    return out


# ---------------------------------------------------------------------------
# Phase 2 — resolve a Letterboxd film slug to a TMDB id by scraping the film page.
# Letterboxd embeds a "TMDb" outbound link of the form
#   https://www.themoviedb.org/movie/<id>/
# on each film page. We extract that.
# ---------------------------------------------------------------------------
TMDB_LINK_RE = re.compile(r"themoviedb\.org/movie/(\d+)")


def resolve_tmdb_id(slug: str) -> dict:
    """Return {slug, tmdb_id, title} (tmdb_id may be None if not found)."""
    url = f"{LBX_BASE}/film/{slug}/"
    r = _get(url)
    if r is None or r.status_code != 200:
        return {"slug": slug, "tmdb_id": None, "title": None}
    m = TMDB_LINK_RE.search(r.text)
    tmdb_id = int(m.group(1)) if m else None
    # Pull canonical title
    soup = BeautifulSoup(r.text, "html.parser")
    title_tag = soup.select_one("h1.headline-1, h1.filmtitle, h1")
    title = title_tag.get_text(strip=True) if title_tag else None
    return {"slug": slug, "tmdb_id": tmdb_id, "title": title}


# ---------------------------------------------------------------------------
# Phase 3 — fetch genres for a TMDB id.
# ---------------------------------------------------------------------------

def fetch_tmdb_genres(args: tuple[int, str]) -> dict:
    """args = (tmdb_id, api_key). Returns {tmdb_id, title, genres: [str, ...]}.
    Uses the v3 movies endpoint. genres is empty list on failure.
    """
    tmdb_id, api_key = args
    url = f"{TMDB_BASE}/movie/{tmdb_id}?api_key={api_key}&language=en-US"
    backoff = 1.5
    for attempt in range(5):
        try:
            r = _session().get(url, timeout=20)
        except requests.RequestException:
            time.sleep(backoff)
            backoff *= 2
            continue
        if r.status_code == 200:
            data = r.json()
            return {
                "tmdb_id": tmdb_id,
                "title": data.get("title") or data.get("original_title"),
                "genres": [g["name"] for g in data.get("genres", [])],
            }
        if r.status_code == 429:
            # Honor Retry-After if present
            wait = float(r.headers.get("Retry-After", backoff))
            time.sleep(wait)
            backoff *= 2
            continue
        if r.status_code in (500, 502, 503, 504):
            time.sleep(backoff)
            backoff *= 2
            continue
        # 401/404 etc — give up
        return {"tmdb_id": tmdb_id, "title": None, "genres": []}
    return {"tmdb_id": tmdb_id, "title": None, "genres": []}
