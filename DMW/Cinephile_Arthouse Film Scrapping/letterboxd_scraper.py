"""
CineMatch - Letterboxd Scraper 
=========================================
Scrapes Cinephile + Arthouse lists using Edge + Selenium.
Runs 4 browsers in parallel for speed.

Output:
  cinephileart_films.csv  -- film details table
  cinephileart_lists.csv  -- list membership table

Requirements:
  pip install selenium beautifulsoup4 lxml pandas tqdm
  Place msedgedriver.exe in the same folder as this script.
"""

import pandas as pd
import time
import random
import re
import json
from datetime import date
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service

# ----------------------------------------------
# CONFIG
# ----------------------------------------------

LISTS_TO_SCRAPE = {
    # Cinephile Essentials
    "Cinephile Essentials":  "https://letterboxd.com/films/popular/genre/drama/",
    "Must See Cinema":       "https://letterboxd.com/dave/list/official-top-250-narrative-feature-films/",
    "Criterion":             "https://letterboxd.com/films/popular/on/criterion-channel/",

    # Arthouse
    "Arthouse":              "https://letterboxd.com/films/popular/genre/arthouse/",
    "Cannes Palme dOr":      "https://letterboxd.com/sleeplessness/list/palme-dor-winners/",
    "Foreign Language":      "https://letterboxd.com/films/popular/genre/foreign/",
}

PERSONALITY_LABEL = "Cinephile"
DELAY_MIN = 1.0
DELAY_MAX = 2.0
NUM_WORKERS = 4   # number of parallel browsers -- lower if your PC struggles
TODAY = str(date.today())

# ----------------------------------------------
# BROWSER SETUP
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
    service = Service(executable_path="msedgedriver.exe")
    driver = webdriver.Edge(service=service, options=opts)
    return driver


# ----------------------------------------------
# HELPERS
# ----------------------------------------------

def get_soup(driver, url: str) -> BeautifulSoup | None:
    try:
        driver.get(url)
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        return BeautifulSoup(driver.page_source, "lxml")
    except Exception as e:
        print(f"  Failed to load {url}: {e}")
        return None


# ----------------------------------------------
# STEP 1 -- Scrape film URLs from a list page
# ----------------------------------------------

def get_film_urls_from_list(driver, list_url: str) -> list[tuple[str, str]]:
    films = []
    page = 1

    while True:
        paged_url = list_url.rstrip("/") + f"/page/{page}/"
        soup = get_soup(driver, paged_url)

        if soup is None:
            break

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
            slug = None
            title = None

            if item.get("data-film-slug"):
                slug = item["data-film-slug"]
                title = item.get("data-film-name")

            if not slug:
                div = item.select_one("div[data-film-slug]")
                if div:
                    slug = div.get("data-film-slug")
                    title = div.get("data-film-name")

            if not slug:
                div = item.select_one("div[data-target-link]")
                if div:
                    link = div.get("data-target-link", "")
                    parts = [p for p in link.strip("/").split("/") if p]
                    if len(parts) >= 2 and parts[0] == "film":
                        slug = parts[1]
                    title = div.get("data-film-name")

            if not slug:
                a = item.select_one('a[href*="/film/"]')
                if a:
                    href = a.get("href", "")
                    parts = [p for p in href.strip("/").split("/") if p]
                    if len(parts) >= 2 and parts[0] == "film":
                        slug = parts[1]

            if not title:
                img = item.select_one("img")
                if img:
                    raw = img.get("alt") or img.get("title") or ""
                    raw = re.sub(r"^Poster for\s+", "", raw)
                    raw = re.sub(r"\s*\(\d{4}\)$", "", raw).strip()
                    title = raw or None

            if slug:
                film_url = f"https://letterboxd.com/film/{slug}/"
                films.append(((title or slug).strip(), film_url))

        print(f"    [{list_url.split('/')[-3]}] Page {page}: {len(items)} items, {len(films)} total")
        page += 1

        if page > 30:
            break

    return films


# ----------------------------------------------
# STEP 2 -- Scrape details from a single film page
# ----------------------------------------------

def scrape_film_details(driver, film_url: str) -> dict:
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

    for sel in ["h1.filmtitle span", "h1.filmtitle", "h1[itemprop='name']", ".headline-1"]:
        tag = soup.select_one(sel)
        if tag:
            result["film_title"] = tag.get_text(strip=True)
            break

    for sel in ["div.releaseyear a", "small.number a", ".releaseyear a", "a[href*='/films/year/']"]:
        tag = soup.select_one(sel)
        if tag:
            text = tag.get_text(strip=True)
            if re.match(r"\d{4}", text):
                result["year"] = text
                break

    genres = [a.get_text(strip=True) for a in soup.select("a[href*='/films/genre/']")]
    result["genre"] = "|".join(genres) if genres else None

    for candidate in soup.find_all(string=re.compile(r"\d+\s*mins?")):
        m = re.search(r"(\d+)\s*mins?", candidate)
        if m:
            result["runtime_mins"] = int(m.group(1))
            break

    rating_meta = soup.find("meta", attrs={"name": "twitter:data2"})
    if rating_meta and rating_meta.get("content"):
        m = re.search(r"([\d.]+)", rating_meta["content"])
        if m:
            result["avg_rating"] = float(m.group(1))

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

    try:
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            raw = script.string or ""
            raw = re.sub(r"/\*.*?\*/", "", raw, flags=re.DOTALL).strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
                review_count = (data.get("aggregateRating") or {}).get("reviewCount")
                if review_count is not None:
                    result["num_reviews"] = int(review_count)
                    break
            except Exception:
                continue
    except Exception as e:
        print(f"  num_reviews error: {e}")

    countries = [a.get_text(strip=True) for a in soup.select("a[href*='/films/country/']")]
    result["country"] = "|".join(countries) if countries else None

    languages = [a.get_text(strip=True) for a in soup.select("a[href*='/films/language/']")]
    result["language"] = "|".join(languages) if languages else None

    return result


# ----------------------------------------------
# WORKER -- each thread gets its own browser
# ----------------------------------------------

def scrape_film_worker(args):
    """Called by each thread. Opens its own browser and scrapes its chunk of films."""
    film_chunk, worker_id = args
    driver = make_driver()
    results = []
    try:
        for film_url, title in film_chunk:
            details = scrape_film_details(driver, film_url)
            if details["film_title"] is None:
                details["film_title"] = title
            results.append(details)
    finally:
        driver.quit()
    return results


# ----------------------------------------------
# STEP 3 -- Main scrape loop
# ----------------------------------------------

def scrape_all():
    # -- STEP 1: collect all film URLs (single browser, fast) --
    print("\nStarting browser for list collection...")
    driver = make_driver()
    all_list_rows = []
    all_film_urls = {}

    try:
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
    finally:
        driver.quit()

    lists_df = pd.DataFrame(all_list_rows)
    lists_df.to_csv("cinephileart_lists.csv", index=False)
    print(f"cinephileart_lists.csv saved -- {len(lists_df)} rows")

    # -- STEP 2: scrape film details in parallel --
    print(f"\nSTEP 2: Scraping {len(all_film_urls)} unique film pages using {NUM_WORKERS} browsers in parallel\n")

    # Split the film list into equal chunks, one per worker
    film_items = list(all_film_urls.items())
    chunk_size = len(film_items) // NUM_WORKERS + 1
    chunks = [film_items[i:i + chunk_size] for i in range(0, len(film_items), chunk_size)]
    args = [(chunk, i) for i, chunk in enumerate(chunks)]

    all_details = []
    with tqdm(total=len(film_items), desc="Films") as pbar:
        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = {executor.submit(scrape_film_worker, arg): arg for arg in args}
            for future in as_completed(futures):
                results = future.result()
                all_details.extend(results)
                pbar.update(len(results))

    films_df = pd.DataFrame(all_details)
    films_df.to_csv("cinephileart_films.csv", index=False)
    print(f"\ncinephileart_films.csv saved -- {len(films_df)} rows")
    print("\nDone. cinephileart_films.csv and cinephileart_lists.csv are ready to send.\n")


if __name__ == "__main__":
    scrape_all()
