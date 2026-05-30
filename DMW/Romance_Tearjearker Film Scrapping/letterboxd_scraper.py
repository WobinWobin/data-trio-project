"""
CineMatch - Letterboxd Scraper (Tristan)
=========================================
Scrapes Romance + Tearjerker lists using Edge + Selenium.

Output:
  romance_tears_films.csv  -- film details table
  romance_tears_lists.csv  -- list membership table

Requirements:
  pip install selenium beautifulsoup4 lxml pandas tqdm
  Place msedgedriver.exe in the same folder as this script.
"""

import pandas as pd
import time
import random
import re
from datetime import date
from bs4 import BeautifulSoup
from tqdm import tqdm

from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service

# ----------------------------------------------
# CONFIG
# ----------------------------------------------

LISTS_TO_SCRAPE = {
    "Romance":          "https://letterboxd.com/films/popular/genre/romance/",
    "Romance Classics": "https://letterboxd.com/bfi/list/the-ultimate-romance-watchlist/",
    "Tearjerkers":      "https://letterboxd.com/brynmawrfilm/list/community-curated-best-tearjerker-films/",
    "Saddest Films":    "https://letterboxd.com/icequeen86/list/the-50-most-saddest-movies-of-all-time/",
}

PERSONALITY_LABEL = "Romantic"

DELAY_MIN = 2.0
DELAY_MAX = 4.0

TODAY = str(date.today())

# ----------------------------------------------
# BROWSER SETUP
# Uses msedgedriver.exe -- place it in the same folder as this script.
# ----------------------------------------------

def make_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    # Uses the msedgedriver.exe you downloaded -- place it in the same
    # folder as this script, or update the path below
    service = Service(executable_path="msedgedriver.exe")
    driver = webdriver.Edge(service=service, options=opts)
    return driver


# ----------------------------------------------
# HELPERS
# ----------------------------------------------

def get_soup(driver, url: str) -> BeautifulSoup | None:
    """Navigate to a URL and return BeautifulSoup of the page."""
    try:
        driver.get(url)
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))  # let JS render
        return BeautifulSoup(driver.page_source, "lxml")
    except Exception as e:
        print(f"  Failed to load {url}: {e}")
        return None


# ----------------------------------------------
# STEP 1 -- Scrape film URLs from a list page
# ----------------------------------------------

def get_film_urls_from_list(driver, list_url: str) -> list[tuple[str, str]]:
    """
    Returns [(film_title, film_url), ...] from a Letterboxd list or browse page.
    Handles pagination automatically (up to 20 pages).
    """
    films = []
    page = 1

    while True:
        paged_url = list_url.rstrip("/") + f"/page/{page}/"
        soup = get_soup(driver, paged_url)

        if soup is None:
            break

        # Try all known Letterboxd selectors
        items = (
            soup.select("li.poster-container") or
            soup.select("li[class*='film-detail']") or
            soup.select("div.film-poster") or
            soup.select("li[class*='poster']") or
            soup.select("div[data-film-slug]") or
            soup.select("article.film")
        )

        if not items:
            break

        for item in items:
            # Try every known way Letterboxd embeds slug + title
            slug = None
            title = None

            # 1. data-film-slug on the <li> itself
            if item.get("data-film-slug"):
                slug = item["data-film-slug"]
                title = item.get("data-film-name")

            # 2. child div with data-film-slug (most common)
            if not slug:
                div = item.select_one("div[data-film-slug]")
                if div:
                    slug = div.get("data-film-slug")
                    title = div.get("data-film-name")

            # 3. child div with data-target-link like /film/some-slug/
            if not slug:
                div = item.select_one("div[data-target-link]")
                if div:
                    link = div.get("data-target-link", "")
                    parts = [p for p in link.strip("/").split("/") if p]
                    if len(parts) >= 2 and parts[0] == "film":
                        slug = parts[1]
                    title = div.get("data-film-name")

            # 4. fallback: any <a href="/film/..."> link
            if not slug:
                a = item.select_one('a[href*="/film/"]')
                if a:
                    href = a.get("href", "")
                    parts = [p for p in href.strip("/").split("/") if p]
                    if len(parts) >= 2 and parts[0] == "film":
                        slug = parts[1]

            # 5. title fallback: img alt text (strip "Poster for X (YEAR)" pattern)
            if not title:
                img = item.select_one("img")
                if img:
                    raw = img.get("alt") or img.get("title") or ""
                    # alt text is often "Poster for TITLE (YEAR)" -- strip the prefix
                    raw = re.sub(r"^Poster for\s+", "", raw)
                    # strip trailing year in parens e.g. " (2016)"
                    raw = re.sub(r"\s*\(\d{4}\)$", "", raw).strip()
                    title = raw or None

            if slug:
                film_url = f"https://letterboxd.com/film/{slug}/"
                films.append(((title or slug).strip(), film_url))
            else:
                # print first unmatched item so we can debug further if needed
                pass

        print(f"    Page {page}: {len(items)} items found, {len(films)} films collected so far")
        page += 1

        if page > 20:
            break

    return films


# ----------------------------------------------
# STEP 2 -- Scrape details from a single film page
# ----------------------------------------------

def scrape_film_details(driver, film_url: str) -> dict:
    """
    Visits a single Letterboxd film page and returns a dict
    matching the project spec fields exactly.
    """
    soup = get_soup(driver, film_url)
    result = {
        "film_title":     None,
        "year":           None,
        "genre":          None,
        "runtime_mins":   None,
        "avg_rating":     None,
        "num_ratings":    None,
        "num_reviews":    None,
        "country":        None,
        "language":       None,
        "letterboxd_url": film_url,
        "date_scraped":   TODAY,
    }

    if soup is None:
        return result

    # Title -- try multiple selectors
    for sel in ["h1.filmtitle span", "h1.filmtitle", "h1[itemprop='name']", ".headline-1"]:
        tag = soup.select_one(sel)
        if tag:
            result["film_title"] = tag.get_text(strip=True)
            break

    # Year -- try multiple selectors
    for sel in ["div.releaseyear a", "small.number a", ".releaseyear a", "a[href*='/films/year/']"]:
        tag = soup.select_one(sel)
        if tag:
            text = tag.get_text(strip=True)
            if re.match(r"\d{4}", text):
                result["year"] = text
                break

    # Genre -- links to /films/genre/
    genres = [a.get_text(strip=True) for a in soup.select("a[href*='/films/genre/']")]
    result["genre"] = "|".join(genres) if genres else None

    # Runtime -- scan all text nodes for "X mins"
    for candidate in soup.find_all(string=re.compile(r"\d+\s*mins?")):
        m = re.search(r"(\d+)\s*mins?", candidate)
        if m:
            result["runtime_mins"] = int(m.group(1))
            break

    # avg_rating -- meta tag first
    rating_meta = soup.find("meta", attrs={"name": "twitter:data2"})
    if rating_meta and rating_meta.get("content"):
        m = re.search(r"([\d.]+)", rating_meta["content"])
        if m:
            result["avg_rating"] = float(m.group(1))

    # avg_rating + num_ratings -- from JSON-LD or inline script
    if result["avg_rating"] is None or result["num_ratings"] is None:
        for script in soup.find_all("script"):
            text = script.string or ""
            if "ratingCount" in text or "meanRating" in text:
                if result["avg_rating"] is None:
                    m = re.search(r'"(?:meanRating|ratingValue)":\s*([\d.]+)', text)
                    if m:
                        result["avg_rating"] = float(m.group(1))
                if result["num_ratings"] is None:
                    m = re.search(r'"ratingCount":\s*(\d+)', text)
                    if m:
                        result["num_ratings"] = int(m.group(1))
                break

    # num_ratings -- fallback from visible text like "1.2M ratings"
    if result["num_ratings"] is None:
        for tag in soup.find_all(string=re.compile(r"[\d,.]+[KkMm]?\s*rating")):
            m = re.search(r"([\d,.]+)([KkMm]?)\s*rating", tag)
            if m:
                num = float(m.group(1).replace(",", ""))
                suffix = m.group(2).upper()
                if suffix == "K":
                    num *= 1_000
                elif suffix == "M":
                    num *= 1_000_000
                result["num_ratings"] = int(num)
                break

    # num_reviews -- JSON-LD block contains aggregateRating.reviewCount
    # The block is wrapped in CDATA comments so we strip those first
    try:
        import json as _json, re as _re
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            raw = script.string or ""
            # strip CDATA wrapper: /* <![CDATA[ */ ... /* ]]> */
            raw = _re.sub(r"/\*.*?\*/", "", raw, flags=_re.DOTALL).strip()
            if not raw:
                continue
            try:
                data = _json.loads(raw)
                review_count = (data.get("aggregateRating") or {}).get("reviewCount")
                if review_count is not None:
                    result["num_reviews"] = int(review_count)
                    break
            except Exception:
                continue
    except Exception as e:
        print(f"  num_reviews error: {e}")

    # Country
    countries = [a.get_text(strip=True) for a in soup.select("a[href*='/films/country/']")]
    result["country"] = "|".join(countries) if countries else None

    # Language
    languages = [a.get_text(strip=True) for a in soup.select("a[href*='/films/language/']")]
    result["language"] = "|".join(languages) if languages else None

    return result


# ----------------------------------------------
# STEP 3 -- Main scrape loop
# ----------------------------------------------

def scrape_all():
    print("\nStarting browser...")
    driver = make_driver()

    try:
        all_list_rows = []
        all_film_urls = {}

        print("\nSTEP 1: Collecting film URLs from lists\n")

        for list_name, list_url in LISTS_TO_SCRAPE.items():
            print(f"Scraping list: {list_name}")
            films = get_film_urls_from_list(driver, list_url)
            print(f"  {len(films)} films found\n")

            for title, film_url in films:
                all_list_rows.append({
                    "list_name":      list_name,
                    "list_url":       list_url,
                    "film_title":     title,
                    "letterboxd_url": film_url,
                    "date_scraped":   TODAY,
                })
                all_film_urls[film_url] = title

        lists_df = pd.DataFrame(all_list_rows)
        lists_df.to_csv("romance_tears_lists.csv", index=False)
        print(f"romance_tears_lists.csv saved -- {len(lists_df)} rows")

        print(f"\nSTEP 2: Scraping {len(all_film_urls)} unique film pages\n")

        film_details = []
        for film_url, title in tqdm(all_film_urls.items(), desc="Films"):
            details = scrape_film_details(driver, film_url)
            if details["film_title"] is None:
                details["film_title"] = title
            film_details.append(details)

        films_df = pd.DataFrame(film_details)
        films_df.to_csv("romance_tears_films.csv", index=False)
        print(f"\nromance_tears_films.csv saved -- {len(films_df)} rows")
        print("\nDone. romance_tears_films.csv and romance_tears_lists.csv are ready to send.\n")

    finally:
        driver.quit()  # always close the browser even if something crashes


if __name__ == "__main__":
    scrape_all()