"""Auto-generated — do not edit by hand."""
from __future__ import annotations
import json, re, time
from typing import Optional
from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup

LBX_BASE   = "https://letterboxd.com"
MAX_ACTORS = 5

_SESSION: Optional[cffi_requests.Session] = None

def session():
    global _SESSION
    if _SESSION is None:
        _SESSION = cffi_requests.Session(impersonate="chrome124")
        _SESSION.headers.update({
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
    return _SESSION


def get_with_retry(url, *, max_retries=6, timeout=25):
    backoff = 1.0
    for _ in range(max_retries):
        try:
            r = session().get(url, timeout=timeout)
        except Exception:
            time.sleep(backoff); backoff = min(backoff * 2, 60); continue
        if r.status_code == 200:
            if len(r.text) < 8000 and "Just a moment" in r.text:
                time.sleep(backoff); backoff = min(backoff * 2, 60); continue
            return r
        if r.status_code == 404:
            return r
        if r.status_code in (403, 429, 500, 502, 503, 504):
            wait = backoff
            if r.status_code == 429:
                try: wait = float(r.headers.get('Retry-After', backoff))
                except (TypeError, ValueError): pass
            time.sleep(wait); backoff = min(backoff * 2, 120); continue
        return r
    return None


def scrape_film_page(slug):
    empty = dict(slug=slug, num_ratings=None, avg_rating=None,
                 director_url=None, actors_url=None)
    url = f'{LBX_BASE}/film/{slug}/'
    r = get_with_retry(url)
    if r is None or r.status_code != 200:
        return empty
    text = r.text
    soup = BeautifulSoup(text, 'html.parser')

    # avg_rating and num_ratings from JSON-LD
    avg_rating = None
    num_ratings = None
    for tag in soup.select('script[type="application/ld+json"]'):
        raw = tag.string or ''
        if not raw.strip(): continue
        try: d = json.loads(raw)
        except Exception: continue
        agg = d.get('aggregateRating') or {}
        if agg.get('ratingValue') and avg_rating is None:
            try: avg_rating = float(agg['ratingValue'])
            except (TypeError, ValueError): pass
        if agg.get('ratingCount') and num_ratings is None:
            try: num_ratings = int(agg['ratingCount'])
            except (TypeError, ValueError): pass
        if avg_rating is not None and num_ratings is not None: break

    # fallbacks from inline JS
    if num_ratings is None:
        m = re.search(r'"ratingCount"\s*:\s*(\d+)', text)
        if m:
            try: num_ratings = int(m.group(1))
            except ValueError: pass
    if avg_rating is None:
        m = re.search(r'"ratingValue"\s*:\s*([0-9.]+)', text)
        if m:
            try: avg_rating = float(m.group(1))
            except ValueError: pass
    if avg_rating is None:
        m = re.search(r'weightedAverage["\']?\s*:\s*([0-9.]+)', text)
        if m:
            try: avg_rating = float(m.group(1))
            except ValueError: pass

    # num_ratings fallback from filmstat link
    if num_ratings is None:
        for sel in ('li.filmstat-watches a', 'a.icon-watched', '.filmstat-watches .value'):
            el = soup.select_one(sel)
            if el:
                raw = (el.get('title') or el.get_text(strip=True) or '').replace(',', '').strip()
                m2 = re.match(r'([0-9.]+)([KkMm]?)', raw)
                if m2:
                    val = float(m2.group(1))
                    suf = m2.group(2).upper()
                    if suf == 'K': val *= 1_000
                    elif suf == 'M': val *= 1_000_000
                    num_ratings = int(val); break

    # director_url
    director_url = None
    for sel in (
        "#film-crew a[href*='/director/']",
        ".crew-list a[href*='/director/']",
        "span.directorlist a[href*='/director/']",
        "a[href*='/director/']",
    ):
        el = soup.select_one(sel)
        if el:
            href = el.get('href', '')
            if href: director_url = '/' + href.strip('/') + '/'; break

    # actors_url
    actors_url = None
    cast_els = []
    for sel in (
        "#film-cast a[href*='/actor/']",
        ".cast-list a[href*='/actor/']",
        "a[href*='/actor/']",
    ):
        cast_els = soup.select(sel)
        if cast_els: break
    if cast_els:
        seen = []
        for a in cast_els:
            href = a.get('href', '')
            if '/actor/' not in href: continue
            clean = '/' + href.strip('/') + '/'
            if clean not in seen: seen.append(clean)
            if len(seen) >= MAX_ACTORS: break
        if seen: actors_url = ','.join(seen)

    return dict(slug=slug, num_ratings=num_ratings, avg_rating=avg_rating,
                director_url=director_url, actors_url=actors_url)


def worker(slug):
    return scrape_film_page(slug)
