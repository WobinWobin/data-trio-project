"""
film_scraper.py
Scrapes film details from Letterboxd, then enriches with the TMDB API.

Setup:
    pip install requests beautifulsoup4 lxml python-dateutil

    Set your TMDB API key in one of two ways:
      1. Environment variable:  export TMDB_API_KEY="your_key"
      2. Pass it explicitly:    TMDBClient(api_key="your_key")

    Get a free key at: https://www.themoviedb.org/settings/api

Usage:
    from film_scraper import scrape_film, scrape_batch

    # Single film — pass either or both URLs
    film = scrape_film(
        letterboxd_url="https://letterboxd.com/film/the-godfather/",
        tmdb_id="238",          # optional if Letterboxd can find it
    )
    print(film.to_dict())

    # Batch — writes results to a JSON file
    films = scrape_batch([
        {"letterboxd_url": "https://letterboxd.com/film/the-godfather/"},
        {"letterboxd_url": "https://letterboxd.com/film/parasite-2019/"},
    ], output_file="films.json")
"""

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

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared HTTP helpers
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
# Data model — mirrors the film_letterboxd + film_tmdb JSON shapes in your repo
# ---------------------------------------------------------------------------
@dataclass
class FilmData:
    # --- Letterboxd fields (matches file_samples/film_letterboxd.json) ---
    name: Optional[str] = None
    url: Optional[str] = None          # e.g. "/film/the-godfather/"
    lid: Optional[str] = None          # Letterboxd internal ID
    tmdb_id: Optional[str] = None
    avg_rating: Optional[float] = None
    number_of_ratings: Optional[int] = None
    number_of_likes: Optional[int] = None
    number_of_views: Optional[int] = None
    genres: list = field(default_factory=list)
    director: Optional[str] = None     # slug, e.g. "francis-ford-coppola"
    actor: list = field(default_factory=list)  # list of slugs

    # --- TMDB fields (matches file_samples/film_tmdb.json + main.py extras) ---
    tmdb_title: Optional[str] = None
    original_language: Optional[str] = None
    runtime: Optional[int] = None      # minutes
    release_date: Optional[str] = None
    release_year: Optional[int] = None
    release_decade: Optional[int] = None
    movie_age: Optional[int] = None
    budget: Optional[int] = None
    revenue: Optional[int] = None
    profit: Optional[int] = None
    in_franchise: Optional[bool] = None
    belongs_to_collection: Optional[dict] = None
    production_companies: list = field(default_factory=list)  # [{"name":..,"id":..}]
    production_countries: list = field(default_factory=list)  # ["united states", ...]
    production_country_groups: list = field(default_factory=list)  # continents
    tagline: Optional[str] = None
    overview: Optional[str] = None

    def to_dict(self) -> dict:
        """Return only populated fields, matching the repo's JSON style."""
        return {k: v for k, v in asdict(self).items() if v not in (None, [])}


# ---------------------------------------------------------------------------
# Letterboxd scraper
# ---------------------------------------------------------------------------
def scrape_letterboxd(url: str) -> FilmData:
    """
    Scrape a Letterboxd film page.

    Args:
        url: Full or relative Letterboxd film URL.
              e.g. 'https://letterboxd.com/film/the-godfather/'
                or '/film/the-godfather/'

    Returns:
        FilmData with Letterboxd fields populated.
    """
    base = "https://letterboxd.com"
    if url.startswith("/"):
        full_url = base + url
        film_path = url
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
                same_as = directors[0].get("sameAs", "")
                film.director = same_as.rstrip("/").split("/")[-1]

            film.actor = [
                a.get("sameAs", "").rstrip("/").split("/")[-1]
                for a in ld.get("actors", [])
                if a.get("sameAs")
            ]
        except Exception as exc:
            log.warning("JSON-LD parse error on %s: %s", full_url, exc)

    # -- Letterboxd internal film ID ---------------------------------------
    poster = soup.find(attrs={"data-component-class": "globals.comps.FilmPosterComponent"})
    if poster:
        film.lid = poster.get("data-film-id")

    # -- TMDB ID -----------------------------------------------------------
    tmdb_tag = soup.find(attrs={"data-tmdb-type": "movie"})
    if tmdb_tag:
        tmdb_id = tmdb_tag.get("data-tmdb-id", "")
        film.tmdb_id = tmdb_id if tmdb_id else None

    # -- Likes & views (main page — avoids the 403 on /members/) -----------
    # Letterboxd renders these as nav links on the film page itself, e.g.:
    #   <a href="/film/the-godfather/likes/"   title="10,531 fans">
    #   <a href="/film/the-godfather/members/" title="136,397 watches">
    likes_tag = soup.find("a", href=re.compile(re.escape(film_path) + r"likes/"))
    if likes_tag:
        film.number_of_likes = _parse_count(likes_tag.get("title", "").split()[0])

    views_tag = soup.find("a", href=re.compile(re.escape(film_path) + r"members/"))
    if views_tag:
        film.number_of_views = _parse_count(views_tag.get("title", "").split()[0])

    log.info("Letterboxd done: %s  (lid=%s, tmdb_id=%s)", film.name, film.lid, film.tmdb_id)
    return film


# ---------------------------------------------------------------------------
# TMDB API client
# ---------------------------------------------------------------------------
class TMDBClient:
    """Thin wrapper around the TMDB v3 REST API."""

    def __init__(self, api_key: Optional[str] = None):
        key = api_key or os.environ.get("TMDB_API_KEY")
        if not key:
            raise ValueError(
                "No TMDB API key found. Set the TMDB_API_KEY environment variable "
                "or pass api_key= to TMDBClient()."
            )
        # TMDB issues two credential formats:
        #   - v3 API key: short ~32-char alphanumeric string  → ?api_key= query param
        #   - Bearer token (Read Access Token): long JWT starting with "eyJ" → Authorization header
        if key.startswith("eyJ"):
            self._headers = {"Authorization": f"Bearer {key}"}
            self._api_key_param = None
        else:
            self._headers = {}
            self._api_key_param = key

    def _get(self, endpoint: str, **params) -> Optional[dict]:
        """Make a GET request to TMDB and return the JSON response."""
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
        """Fetch full movie details by TMDB ID."""
        return self._get(f"movie/{tmdb_id}")

    def search_movie(self, title: str, year: Optional[int] = None) -> Optional[str]:
        """
        Search for a film by title (+ optional year) and return its TMDB ID.
        Returns the best-match TMDB ID string, or None if nothing found.
        """
        params = {"query": title, "include_adult": False}
        if year:
            params["year"] = year
        data = self._get("search/movie", **params)
        if data and data.get("results"):
            return str(data["results"][0]["id"])
        return None


# ---------------------------------------------------------------------------
# TMDB data enrichment
# ---------------------------------------------------------------------------
# Country -> continent map, based on the repo's continent_country_pairs.json
_COUNTRY_CONTINENT: dict[str, str] = {
    "Andorra": "Europe", "Afghanistan": "Asia", "Albania": "Europe",
    "Algeria": "Africa", "Angola": "Africa", "Argentina": "South America",
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


def _country_to_continent(country: str) -> Optional[str]:
    """Best-effort country -> continent lookup (case-insensitive)."""
    for k, v in _COUNTRY_CONTINENT.items():
        if k.lower() == country.lower():
            return v
    return None


def _enrich_with_tmdb(film: FilmData, raw: dict) -> FilmData:
    """
    Apply TMDB API response to a FilmData object.
    Mirrors the field selection in your repo's constants.py + main.py post_call_work().
    """
    import datetime
    import dateutil.parser as dp

    film.tmdb_title = raw.get("title")
    film.original_language = raw.get("original_language")
    film.runtime = raw.get("runtime")
    film.budget = raw.get("budget")
    film.revenue = raw.get("revenue")
    film.tagline = raw.get("tagline") or None
    film.overview = raw.get("overview") or None

    # Franchise
    btc = raw.get("belongs_to_collection")
    film.in_franchise = btc is not None
    if btc:
        film.belongs_to_collection = {"name": btc.get("name"), "id": btc.get("id")}

    # Profit
    if film.budget is not None and film.revenue is not None:
        film.profit = film.revenue - film.budget

    # Release date -> year / decade / age
    release_date = raw.get("release_date", "")
    film.release_date = release_date or None
    if release_date:
        try:
            parsed = dp.parse(release_date)
            film.release_year = parsed.year
            film.release_decade = (parsed.year // 10) * 10
            film.movie_age = datetime.datetime.now().year - parsed.year
        except ValueError:
            pass

    # Production companies: keep name + id only (matches your LIST_KEYS)
    film.production_companies = [
        {"name": c.get("name"), "id": c.get("id")}
        for c in raw.get("production_companies", [])
        if c.get("name")
    ]

    # Production countries: name only + continent group
    country_names = [c.get("name") for c in raw.get("production_countries", []) if c.get("name")]
    film.production_countries = [n.lower() for n in country_names]
    film.production_country_groups = list({
        cont for c in country_names
        if (cont := _country_to_continent(c)) is not None
    })

    # Fall back: use TMDB title if Letterboxd didn't give us a name
    if not film.name:
        film.name = film.tmdb_title
    if not film.tmdb_id:
        film.tmdb_id = str(raw.get("id", "")) or None

    return film


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def scrape_film(
    letterboxd_url: Optional[str] = None,
    tmdb_id: Optional[str] = None,
    tmdb_client: Optional[TMDBClient] = None,
) -> FilmData:
    """
    Scrape a film from Letterboxd and enrich it with TMDB data.

    Args:
        letterboxd_url: Letterboxd film page URL. If provided, the TMDB ID is
                        extracted from the page automatically unless you also
                        pass tmdb_id.
        tmdb_id:        TMDB movie ID. If you already know it you can skip
                        Letterboxd scraping by omitting letterboxd_url.
        tmdb_client:    An existing TMDBClient instance. If None, one is created
                        from the TMDB_API_KEY environment variable.

    Returns:
        FilmData with both Letterboxd and TMDB fields populated.

    Examples:
        # Scrape both sources (most common)
        film = scrape_film("https://letterboxd.com/film/the-godfather/")

        # TMDB only, no Letterboxd scraping
        film = scrape_film(tmdb_id="238")

        # Override the auto-detected TMDB ID
        film = scrape_film("https://letterboxd.com/film/the-godfather/", tmdb_id="238")
    """
    if not letterboxd_url and not tmdb_id:
        raise ValueError("Provide at least one of letterboxd_url or tmdb_id.")

    client = tmdb_client or TMDBClient()
    film = FilmData()

    # Step 1: Letterboxd
    if letterboxd_url:
        film = scrape_letterboxd(letterboxd_url)
        time.sleep(random.uniform(1, 2))

    # Step 2: Resolve TMDB ID (explicit > scraped > search fallback)
    effective_tmdb_id = tmdb_id or film.tmdb_id
    if not effective_tmdb_id and film.name:
        log.info("No TMDB ID found on page; searching TMDB for '%s'...", film.name)
        effective_tmdb_id = client.search_movie(film.name, year=film.release_year)
        if effective_tmdb_id:
            log.info("Found TMDB ID %s via search.", effective_tmdb_id)

    # Step 3: TMDB API call
    if effective_tmdb_id:
        raw = client.get_movie(effective_tmdb_id)
        if raw:
            film = _enrich_with_tmdb(film, raw)
        else:
            log.warning("TMDB returned nothing for ID %s.", effective_tmdb_id)
    else:
        log.warning("Could not determine a TMDB ID for this film.")

    return film


def scrape_batch(
    items: list[dict],
    tmdb_client: Optional[TMDBClient] = None,
    delay_range: tuple = (2, 5),
    output_file: Optional[str] = None,
) -> list[FilmData]:
    """
    Scrape a list of films and optionally save results as JSON.

    Args:
        items:        Each dict may contain:
                        {"letterboxd_url": "...", "tmdb_id": "..."}
                      Either key is optional but at least one must be present.
        tmdb_client:  Shared TMDBClient (created once from env var if not passed).
        delay_range:  Random sleep in seconds between requests.
        output_file:  If provided, write results as a JSON array to this path.

    Returns:
        List of FilmData objects (failed items become empty FilmData placeholders).

    Example:
        films = scrape_batch([
            {"letterboxd_url": "https://letterboxd.com/film/the-godfather/"},
            {"letterboxd_url": "https://letterboxd.com/film/parasite-2019/"},
        ], output_file="films.json")
    """
    client = tmdb_client or TMDBClient()
    results: list[FilmData] = []

    for i, item in enumerate(items, 1):
        log.info("-- Item %d / %d --", i, len(items))
        try:
            film = scrape_film(
                letterboxd_url=item.get("letterboxd_url"),
                tmdb_id=item.get("tmdb_id"),
                tmdb_client=client,
            )
            results.append(film)
        except Exception as exc:
            log.error("Failed on item %d (%s): %s", i, item, exc)
            results.append(FilmData())

        if i < len(items):
            sleep_s = random.uniform(*delay_range)
            log.info("Sleeping %.1fs...", sleep_s)
            time.sleep(sleep_s)

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump([r.to_dict() for r in results], f, indent=2, ensure_ascii=False)
        log.info("Results written to %s", output_file)

    return results



