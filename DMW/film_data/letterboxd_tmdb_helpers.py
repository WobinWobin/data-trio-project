"""
letterboxd_tmdb_helpers.py
Utility functions for scraping Letterboxd and fetching TMDB details.
"""

import re
import json
import time
import random
import requests
import dateutil.parser as date_parser
import datetime
from bs4 import BeautifulSoup
from requests_html import HTMLSession

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LETTERBOXD_BASE_URL = "https://letterboxd.com"
TMDB_VERSION = "3"
TMDB_BASE_URL = f"https://api.themoviedb.org/{TMDB_VERSION}"

# Keys to pull from TMDB movie response
MOVIE_KEYS = [
    "belongs_to_collection", "budget", "original_language",
    "production_companies", "production_countries", "release_date",
    "revenue", "runtime"
]

# For list-type keys, specify which sub-fields to keep
LIST_KEYS = {
    "production_companies": ["name", "id"],
    "production_countries": ["name"],
}

# ---------------------------------------------------------------------------
# TMDB helpers
# ---------------------------------------------------------------------------

def get_tmdb_movie_url(movie_id: str, api_key: str) -> str:
    return f"{TMDB_BASE_URL}/movie/{movie_id}?api_key={api_key}"


def get_tmdb_company_url(company_id: str, api_key: str) -> str:
    return f"{TMDB_BASE_URL}/company/{company_id}?api_key={api_key}"


def fetch_raw_movie_details(movie_id: str, api_key: str) -> dict | None:
    """Fetch full movie details from TMDB. Returns None on failure."""
    if not movie_id:
        return None
    url = get_tmdb_movie_url(movie_id, api_key)
    try:
        response = requests.get(url, timeout=10)
        if response.ok:
            return response.json()
    except requests.RequestException as e:
        print(f"  [TMDB] Request error for movie {movie_id}: {e}")
    return None


def trim_dict(d: dict, keys: list, list_keys: dict | None) -> dict | any:
    """Return a smaller dict containing only the requested keys."""
    if keys is None or d is None:
        return d
    if len(keys) == 1:
        return d.get(keys[0])
    result = {}
    for key in keys:
        val = d.get(key)
        if list_keys and key in list_keys and isinstance(val, list):
            val = trim_list_of_dicts(val, list_keys[key])
        result[key] = val
    return result


def trim_list_of_dicts(lst: list, keys: list) -> list:
    """Keep only specified keys inside each dict of a list."""
    if lst is None:
        return []
    return [trim_dict(item, keys, None) for item in lst if isinstance(item, dict)]


def get_selective_movie_details(movie_id: str, api_key: str,
                                keys: list = MOVIE_KEYS,
                                list_keys: dict = LIST_KEYS) -> dict | None:
    """Fetch TMDB movie details and return only the selected fields."""
    raw = fetch_raw_movie_details(movie_id, api_key)
    if raw is None:
        return None
    return trim_dict(raw, keys, list_keys)


# ---------------------------------------------------------------------------
# Post-processing / derived fields
# ---------------------------------------------------------------------------

def is_date(string: str, fuzzy: bool = False) -> bool:
    try:
        date_parser.parse(string, fuzzy=fuzzy)
        return True
    except (ValueError, TypeError):
        return False


def add_derived_fields(movie: dict, country_groups: list) -> dict:
    """
    Enrich a movie dict with derived fields:
    - in_franchise, profit, release_year, movie_age, release_decade,
      production_country_group
    """
    if "belongs_to_collection" in movie:
        movie["in_franchise"] = movie["belongs_to_collection"] is not None
        # Trim collection info
        if isinstance(movie["belongs_to_collection"], dict):
            movie["belongs_to_collection"] = trim_dict(
                movie["belongs_to_collection"], ["name", "id"], None
            )

    if "revenue" in movie and "budget" in movie:
        movie["profit"] = (movie["revenue"] or 0) - (movie["budget"] or 0)

    release_date = movie.get("release_date", "")
    if is_date(release_date):
        parsed = date_parser.parse(release_date)
        movie["release_year"] = parsed.year
        curr_year = datetime.datetime.now().year
        movie["movie_age"] = curr_year - parsed.year
        movie["release_decade"] = (parsed.year // 10) * 10
    else:
        movie["release_year"] = ""
        movie["movie_age"] = ""
        movie["release_decade"] = ""

    # Map production countries to continents
    if "production_countries" in movie:
        movie["production_country_group"] = []
        for country_entry in movie["production_countries"]:
            country_name = country_entry.lower() if isinstance(country_entry, str) else ""
            for cg in country_groups:
                cg_name = cg["country"].lower()
                if country_name in cg_name or cg_name in country_name:
                    movie["production_country_group"].append(cg["continent"])
                    break

    return movie


# ---------------------------------------------------------------------------
# Letterboxd scraping helpers
# ---------------------------------------------------------------------------

def extract_number(text: str) -> int:
    """Extract the first integer (ignoring commas) from a string."""
    matches = re.findall(r"[0-9,]+", text)
    if matches:
        return int(matches[0].replace(",", ""))
    return 0


import random

import cloudscraper

def get_page_soup(url: str) -> BeautifulSoup | None:
    """Uses cloudscraper to bypass Cloudflare/403 blocks."""
    try:
        # This creates a scraper instance that mimics a real browser's TLS fingerprint
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        
        # Add a longer, more human-like jitter
        time.sleep(random.uniform(5, 10)) 
        
        response = scraper.get(url, timeout=20)
        
        if response.status_code == 403:
            print(f"  [HTTP] 403 still persists. Cloudflare is blocking the connection.")
            return None
            
        return BeautifulSoup(response.content, "html.parser")
    except Exception as e:
        print(f"  [HTTP] Failed: {e}")
        return None


def scrape_film_page(film_url_path: str) -> dict | None:
    """
    Scrape a single Letterboxd film page.

    Parameters
    ----------
    film_url_path : str
        Relative path, e.g. '/film/parasite-2019/'

    Returns
    -------
    dict with keys: name, url, lid, tmdb_id, number_of_ratings,
                    avg_rating, genres, director, actors
    or None if the page is invalid.
    """
    full_url = f"{LETTERBOXD_BASE_URL}{film_url_path}"
    soup = get_page_soup(full_url)
    if soup is None:
        return None

    # --- ld+json block ---
    ld_json_tag = soup.find(type="application/ld+json")
    if not ld_json_tag:
        print(f"  [LB] No ld+json on {full_url}")
        return None

    match = re.search(r"\{.*\}", ld_json_tag.get_text(), re.DOTALL)
    if not match:
        return None
    try:
        info = json.loads(match.group(0))
    except json.JSONDecodeError as e:
        print(f"  [LB] JSON parse error on {full_url}: {e}")
        return None

    # --- poster / id ---
    poster = soup.find(attrs={"data-component-class": "globals.comps.FilmPosterComponent"})
    if poster is None:
        print(f"  [LB] No poster data on {full_url}")
        return None
    film_name = poster.get("data-film-name", "")
    film_lid  = poster.get("data-film-id", "")

    # --- ratings ---
    agg = info.get("aggregateRating")
    if agg is None:
        print(f"  [LB] No aggregate rating for '{film_name}' — skipping")
        return None
    number_of_ratings = agg.get("ratingCount", 0)
    avg_rating        = agg.get("ratingValue", 0.0)

    # --- genres ---
    genres = [g.lower() for g in info.get("genre", [])]

    # --- director ---
    directors = info.get("director", [])
    director = _url_to_slug(directors[0].get("sameAs", "")) if directors else ""

    # --- actors ---
    actors_raw = info.get("actors", [])
    actors = [_url_to_slug(a.get("sameAs", "")) for a in actors_raw]

    # --- tmdb id ---
    tmdb_tag = soup.find(attrs={"data-tmdb-type": "movie"})
    if tmdb_tag is None:
        print(f"  [LB] '{film_name}' is not a movie (TV?) — skipping")
        return None
    tmdb_id = tmdb_tag.get("data-tmdb-id", "")
    if not tmdb_id:
        print(f"  [LB] Empty tmdb_id for '{film_name}' — skipping")
        return None

    return {
        "name": film_name,
        "url": film_url_path,
        "lid": film_lid,
        "tmdb_id": tmdb_id,
        "number_of_ratings": number_of_ratings,
        "avg_rating": avg_rating,
        "genres": genres,
        "director": director,
        "actors": actors,
    }


def scrape_film_members_page(film_url_path: str) -> dict:
    """
    Fetch the /members page for a film and return likes + views counts.
    Returns {'number_of_likes': int, 'number_of_views': int}
    """
    full_url = f"{LETTERBOXD_BASE_URL}{film_url_path}members"
    soup = get_page_soup(full_url)
    result = {"number_of_likes": 0, "number_of_views": 0}
    if soup is None:
        return result

    likes_tag = soup.find(attrs={"href": f"{film_url_path}likes/"})
    views_tag = soup.find(attrs={"href": f"{film_url_path}members/"})

    if likes_tag and likes_tag.get("title"):
        result["number_of_likes"] = extract_number(likes_tag["title"])
    if views_tag and views_tag.get("title"):
        result["number_of_views"] = extract_number(views_tag["title"])
    return result


def get_popular_films_for_genre(genre: str, max_films: int = 100,
                                 delay_range: tuple = (2, 5)) -> list[dict]:
    """
    Scrape the most popular films for a given Letterboxd genre.

    Parameters
    ----------
    genre : str  e.g. 'action', 'drama'
    max_films : int  maximum films to collect (capped by available pages)
    delay_range : tuple  (min_seconds, max_seconds) between page requests

    Returns
    -------
    list of dicts: [{name, letterboxd_id, url}, ...]
    """
    films = []
    page_num = 1
    base = "https://letterboxd.com/films/genre/{genre}/page/{page}/"

    while len(films) < max_films:
        url = base.format(genre=genre, page=page_num)
        soup = get_page_soup(url)
        if soup is None:
            break

        film_list_tag = soup.find("ul", class_="poster-list")
        if film_list_tag is None:
            break  # no more pages

        items = film_list_tag.find_all("div", recursive=False)
        if not items:
            break

        for item in items:
            div = item.find("div")
            if div is None:
                continue
            films.append({
                "name": div.get("data-film-name", ""),
                "letterboxd_id": div.get("data-film-id", ""),
                "url": div.get("data-film-link", ""),
            })
            if len(films) >= max_films:
                break

        page_num += 1
        time.sleep(random.uniform(*delay_range))

    return films[:max_films]


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def load_json(filepath: str) -> dict | list:
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, filepath: str) -> None:
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"  Saved → {filepath}")


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

def _url_to_slug(url: str) -> str:
    """'/actor/brad-pitt/' → 'brad-pitt'"""
    parts = [p for p in url.split("/") if p]
    return parts[-1] if parts else url
