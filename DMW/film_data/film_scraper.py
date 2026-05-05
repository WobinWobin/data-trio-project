"""
film_scraper.py
Scrapes film data from Letterboxd and the TMDB API as two separate datasets.

Setup:
    pip install requests beautifulsoup4 lxml python-dateutil

    TMDB API key (Bearer token or v3 key):
        export TMDB_API_KEY="your_key_here"
    Get one free at: https://www.themoviedb.org/settings/api

Usage:
    from film_scraper import (
        scrape_film, scrape_batch,           # Letterboxd
        scrape_tmdb, scrape_tmdb_batch,      # TMDB
        TMDBClient,
    )

    # Letterboxd
    film = scrape_film("https://letterboxd.com/film/the-godfather/")
    films = scrape_batch(["https://letterboxd.com/film/the-godfather/", ...])

    # TMDB (needs a tmdb_id — grab it from the Letterboxd result)
    client = TMDBClient()
    tmdb = scrape_tmdb("238", client)
    tmdb_films = scrape_tmdb_batch(["238", "496243", ...], client)
"""

import datetime
import json
import logging
import os
import random
import re
import time
from dataclasses import asdict, dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------
_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
})

TMDB_BASE = "https://api.themoviedb.org/3"


def _get_html(url: str, retries: int = 3) -> BeautifulSoup:
    """Fetch a URL and return BeautifulSoup. Retries with back-off on failure."""
    for attempt in range(1, retries + 1):
        try:
            log.info("GET %s", url)
            resp = _SESSION.get(url, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "lxml")
        except requests.RequestException as exc:
            log.warning("Attempt %d failed: %s", attempt, exc)
            if attempt < retries:
                time.sleep(random.uniform(1, 3) * attempt)
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts.")


def _parse_count(text: str) -> Optional[int]:
    """'136,397' or '1.2K' -> int."""
    if not text:
        return None
    text = text.strip().replace(",", "")
    try:
        if text.upper().endswith("K"):
            return int(float(text[:-1]) * 1_000)
        if text.upper().endswith("M"):
            return int(float(text[:-1]) * 1_000_000)
        return int(float(text))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Letterboxd data model
# ---------------------------------------------------------------------------
@dataclass
class FilmData:
    movie_id: Optional[int] = None          # 1-based index assigned in scrape_batch
    name: Optional[str] = None
    url: Optional[str] = None               # e.g. "/film/the-godfather/"
    lid: Optional[str] = None               # Letterboxd internal film ID
    tmdb_id: Optional[str] = None
    number_of_ratings: Optional[int] = None
    avg_rating: Optional[float] = None
    genres: list = field(default_factory=list)
    director_url: Optional[str] = None      # full Letterboxd creator URL
    actors_urls: list = field(default_factory=list)  # full Letterboxd creator URLs
    number_of_likes: Optional[int] = None
    number_of_views: Optional[int] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v not in (None, [])}


# ---------------------------------------------------------------------------
# TMDB data model
# ---------------------------------------------------------------------------
@dataclass
class TMDBFilmData:
    movie_id: Optional[int] = None          # 1-based index assigned in scrape_tmdb_batch
    release_decade: Optional[int] = None
    production_companies: list = field(default_factory=list)  # [{"name": ..., "id": ...}]
    production_countries: list = field(default_factory=list)  # ["united states", ...]
    revenue: Optional[int] = None
    profit: Optional[int] = None
    movie_age: Optional[int] = None
    release_date: Optional[str] = None
    budget: Optional[int] = None
    name: Optional[str] = None
    tmdb_id: Optional[str] = None
    release_year: Optional[int] = None
    production_country_group: list = field(default_factory=list)  # continents
    runtime: Optional[int] = None
    in_franchise: Optional[bool] = None
    belongs_to_collection: Optional[dict] = None  # {"name": ..., "id": ...}
    original_language: Optional[str] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v not in (None, [])}


# ---------------------------------------------------------------------------
# Letterboxd scraper
# ---------------------------------------------------------------------------
def scrape_film(url: str) -> FilmData:
    """
    Scrape a single Letterboxd film page.

    Args:
        url: Full Letterboxd film URL, e.g. "https://letterboxd.com/film/the-godfather/"

    Returns:
        FilmData with all Letterboxd fields populated.
    """
    base = "https://letterboxd.com"
    if url.startswith("/"):
        full_url = base + url
        film_path = url if url.endswith("/") else url + "/"
    else:
        full_url = url
        film_path = "/" + url.split("letterboxd.com/")[-1].rstrip("/") + "/"

    film = FilmData(url=film_path)
    soup = _get_html(full_url)

    # -- JSON-LD: title, rating, genres, director, actors ------------------
    ld_tag = soup.find("script", type="application/ld+json")
    if ld_tag and ld_tag.string:
        try:
            raw = re.search(r"\{.*\}", ld_tag.string, re.DOTALL).group(0)
            ld = json.loads(raw)

            film.name = ld.get("name")

            agg = ld.get("aggregateRating", {})
            film.avg_rating = agg.get("ratingValue")
            film.number_of_ratings = agg.get("ratingCount")

            film.genres = [g.lower() for g in ld.get("genre", [])]

            directors = ld.get("director", [])
            if directors:
                film.director_url = directors[0].get("sameAs") or None

            film.actors_urls = [
                a["sameAs"] for a in ld.get("actors", []) if a.get("sameAs")
            ]
        except Exception as exc:
            log.warning("JSON-LD parse error on %s: %s", full_url, exc)

    # -- Letterboxd internal film ID ---------------------------------------
    # Stored as data-production-uid="film:51818" on the backdrop div.
    backdrop = soup.find(attrs={"data-production-uid": True})
    if backdrop:
        uid = backdrop.get("data-production-uid", "")
        film.lid = uid.split(":")[-1] if ":" in uid else uid or None

    # -- TMDB ID -----------------------------------------------------------
    tmdb_tag = soup.find(attrs={"data-tmdb-type": "movie"})
    if tmdb_tag:
        val = tmdb_tag.get("data-tmdb-id", "")
        film.tmdb_id = val if val else None

    # -- Likes & views ----------------------------------------------------
    # These counts are injected by JavaScript and are not present in the
    # static HTML. They require either the official Letterboxd API
    # (letterboxd.com/api-beta/) or a JS-capable headless browser.
    # number_of_likes and number_of_views will remain None.

    log.info("Letterboxd done: %s  (lid=%s, tmdb_id=%s)", film.name, film.lid, film.tmdb_id)
    return film


def scrape_batch(
    urls: list[str],
    delay_range: tuple = (2, 5),
    output_file: Optional[str] = None,
) -> list[FilmData]:
    """
    Scrape a list of Letterboxd film URLs.

    Args:
        urls:         List of Letterboxd film URLs.
        delay_range:  Random sleep in seconds between requests.
        output_file:  If provided, write results as a JSON array to this path.

    Returns:
        List of FilmData. movie_id is the 1-based position in the input list.
    """
    results: list[FilmData] = []
    with tqdm(urls, desc="Letterboxd", unit="film") as bar:
        for i, url in enumerate(bar, 1):
            slug = url.rstrip("/").split("/")[-1]
            bar.set_postfix(film=slug, refresh=True)
            try:
                film = scrape_film(url)
                film.movie_id = i
                bar.set_postfix(film=film.name or slug, refresh=True)
                results.append(film)
            except Exception as exc:
                log.error("Failed on item %d (%s): %s", i, url, exc)
                results.append(FilmData(movie_id=i))

        if i < len(urls):
            time.sleep(random.uniform(*delay_range))

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump([r.to_dict() for r in results], f, indent=2, ensure_ascii=False)


    return results


# ---------------------------------------------------------------------------
# TMDB client
# ---------------------------------------------------------------------------
class TMDBClient:
    """
    Thin wrapper around the TMDB v3 REST API.

    Accepts both credential formats:
      - v3 API key  (~32-char alphanumeric)  → sent as ?api_key= query param
      - Bearer token (long JWT starting with 'eyJ') → sent as Authorization header
    """

    def __init__(self, api_key: Optional[str] = None):
        key = api_key or os.environ.get("TMDB_API_KEY")
        if not key:
            raise ValueError(
                "No TMDB API key found. Set the TMDB_API_KEY environment variable "
                "or pass api_key= to TMDBClient()."
            )
        if key.startswith("eyJ"):
            self._headers = {"Authorization": f"Bearer {key}"}
            self._api_key_param = None
        else:
            self._headers = {}
            self._api_key_param = key

    def get(self, endpoint: str, **params) -> Optional[dict]:
        url = f"{TMDB_BASE}/{endpoint.lstrip('/')}"
        if self._api_key_param:
            params["api_key"] = self._api_key_param
        try:
            resp = _SESSION.get(url, params=params, headers=self._headers, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.warning("TMDB request failed for %s: %s", url, exc)
            return None

    def get_movie(self, tmdb_id: str) -> Optional[dict]:
        return self.get(f"movie/{tmdb_id}")

    def search_movie(self, title: str, year: Optional[int] = None) -> Optional[str]:
        """Search by title and return the best-match TMDB ID, or None."""
        params: dict = {"query": title, "include_adult": False}
        if year:
            params["year"] = year
        data = self.get("search/movie", **params)
        if data and data.get("results"):
            return str(data["results"][0]["id"])
        return None


# ---------------------------------------------------------------------------
# Country -> continent map
# ---------------------------------------------------------------------------
_COUNTRY_CONTINENT: dict[str, str] = {
    "Afghanistan": "Asia", "Albania": "Europe", "Algeria": "Africa",
    "Andorra": "Europe", "Angola": "Africa", "Argentina": "South America",
    "Armenia": "Asia", "Australia": "Oceania", "Austria": "Europe",
    "Azerbaijan": "Asia", "Bahrain": "Asia", "Bangladesh": "Asia",
    "Belarus": "Europe", "Belgium": "Europe", "Belize": "North America",
    "Bolivia": "South America", "Bosnia and Herzegovina": "Europe",
    "Brazil": "South America (Portuguese)", "Bulgaria": "Europe",
    "Cambodia": "Asia", "Cameroon": "Africa", "Canada": "North America",
    "Chile": "South America", "China": "Asia",
    "People's Republic of China": "Asia", "Colombia": "South America",
    "Costa Rica": "North America", "Croatia": "Europe", "Cuba": "North America",
    "Czech Republic": "Europe", "Denmark": "Europe", "Ecuador": "South America",
    "Egypt": "Africa", "El Salvador": "North America", "Estonia": "Europe",
    "Ethiopia": "Africa", "Finland": "Europe", "France": "Europe",
    "Georgia": "Asia", "Germany": "Europe", "Ghana": "Africa",
    "Greece": "Europe", "Guatemala": "North America", "Honduras": "North America",
    "Hungary": "Europe", "Iceland": "Europe", "India": "Asia",
    "Indonesia": "Asia", "Iran": "Asia", "Iraq": "Asia",
    "Republic of Ireland": "Europe", "Ireland": "Europe", "Israel": "Asia",
    "Italy": "Europe", "Jamaica": "North America", "Japan": "Asia",
    "Jordan": "Asia", "Kazakhstan": "Asia", "Kenya": "Africa",
    "South Korea": "Asia", "North Korea": "Asia", "Kuwait": "Asia",
    "Latvia": "Europe", "Lebanon": "Asia", "Lithuania": "Europe",
    "Luxembourg": "Europe", "Malaysia": "Asia", "Mexico": "North America",
    "Moldova": "Europe", "Mongolia": "Asia", "Morocco": "Africa",
    "Myanmar": "Asia", "Nepal": "Asia", "Netherlands": "Europe",
    "Kingdom of the Netherlands": "Europe", "New Zealand": "Oceania",
    "Nicaragua": "North America", "Nigeria": "Africa", "Norway": "Europe",
    "Oman": "Asia", "Pakistan": "Asia", "Panama": "North America",
    "Paraguay": "South America", "Peru": "South America",
    "Philippines": "Asia", "Poland": "Europe", "Portugal": "Europe",
    "Qatar": "Asia", "Romania": "Europe", "Russia": "Europe",
    "Rwanda": "Africa", "Saudi Arabia": "Asia", "Senegal": "Africa",
    "Serbia": "Europe", "Singapore": "Asia", "Slovakia": "Europe",
    "Slovenia": "Europe", "South Africa": "Africa", "Spain": "Europe",
    "Sri Lanka": "Asia", "Sudan": "Africa", "Sweden": "Europe",
    "Switzerland": "Europe", "Syria": "Asia", "Thailand": "Asia",
    "Tunisia": "Africa", "Turkey": "Asia", "Uganda": "Africa",
    "Ukraine": "Europe", "United Arab Emirates": "Asia",
    "United Kingdom": "Europe", "United States": "North America",
    "United States of America": "North America",
    "Uruguay": "South America", "Uzbekistan": "Asia",
    "Venezuela": "South America", "Vietnam": "Asia", "Yemen": "Asia",
    "Zambia": "Africa", "Zimbabwe": "Africa",
}


def _continent(country: str) -> Optional[str]:
    for k, v in _COUNTRY_CONTINENT.items():
        if k.lower() == country.lower():
            return v
    return None


# ---------------------------------------------------------------------------
# TMDB scraper
# ---------------------------------------------------------------------------
def scrape_tmdb(tmdb_id: str, client: TMDBClient) -> TMDBFilmData:
    """
    Fetch and shape a single film from the TMDB API.

    Args:
        tmdb_id: TMDB movie ID (string or int).
        client:  A TMDBClient instance.

    Returns:
        TMDBFilmData with all fields populated.
    """
    raw = client.get_movie(str(tmdb_id))
    if not raw:
        raise RuntimeError(f"TMDB returned nothing for ID {tmdb_id}")

    film = TMDBFilmData(tmdb_id=str(raw.get("id", tmdb_id)))
    film.name = raw.get("title")
    film.original_language = raw.get("original_language")
    film.runtime = raw.get("runtime") or None
    film.budget = raw.get("budget") or None
    film.revenue = raw.get("revenue") or None
    film.release_date = raw.get("release_date") or None

    # Computed fields
    if film.budget is not None and film.revenue is not None:
        film.profit = film.revenue - film.budget

    if film.release_date:
        try:
            import dateutil.parser as dp
            parsed = dp.parse(film.release_date)
            film.release_year = parsed.year
            film.release_decade = (parsed.year // 10) * 10
            film.movie_age = datetime.datetime.now().year - parsed.year
        except Exception:
            pass

    # Franchise
    btc = raw.get("belongs_to_collection")
    film.in_franchise = btc is not None
    if btc:
        film.belongs_to_collection = {"name": btc.get("name"), "id": btc.get("id")}

    # Production companies: name + id only
    film.production_companies = [
        {"name": c["name"], "id": c["id"]}
        for c in raw.get("production_companies", [])
        if c.get("name")
    ]

    # Production countries: lowercase name + continent group
    country_names = [c["name"] for c in raw.get("production_countries", []) if c.get("name")]
    film.production_countries = [n.lower() for n in country_names]
    film.production_country_group = list({
        cont for c in country_names
        if (cont := _continent(c)) is not None
    })

    log.info("TMDB done: %s (%s)", film.name, film.tmdb_id)
    return film


def scrape_tmdb_batch(
    tmdb_ids: list[str],
    client: Optional[TMDBClient] = None,
    delay_range: tuple = (0.3, 0.8),
    output_file: Optional[str] = None,
) -> list[TMDBFilmData]:
    """
    Fetch a list of films from the TMDB API.

    Args:
        tmdb_ids:     List of TMDB movie ID strings.
        client:       TMDBClient instance. Created from TMDB_API_KEY env var if not passed.
        delay_range:  Random sleep in seconds between requests (TMDB rate limit is generous).
        output_file:  If provided, write results as a JSON array to this path.

    Returns:
        List of TMDBFilmData. movie_id is the 1-based position in the input list.
    """
    client = client or TMDBClient()
    results: list[TMDBFilmData] = []

    with tqdm(tmdb_ids, desc="TMDB", unit="film") as bar:
        for i, tmdb_id in enumerate(bar, 1):
            bar.set_postfix(id=tmdb_id, refresh=True)
            try:
                film = scrape_tmdb(tmdb_id, client)
                film.movie_id = i
                bar.set_postfix(film=film.name or tmdb_id, refresh=True)
                results.append(film)
            except Exception as exc:
                log.error("Failed on TMDB ID %s: %s", tmdb_id, exc)
                results.append(TMDBFilmData(movie_id=i, tmdb_id=str(tmdb_id)))

        if i < len(tmdb_ids):
            time.sleep(random.uniform(*delay_range))

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump([r.to_dict() for r in results], f, indent=2, ensure_ascii=False)


    return results
