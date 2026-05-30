"""
CineMatch - Letterboxd Popular Films Scraper (seth)
=========================================
Scrapes popular films from Letterboxd (this year) using Selenium + BeautifulSoup.

Output:
  popular_films.csv  -- film details table

Features:
  - Scrapes film title, year, genre, runtime
  - Extracts ratings and review counts
  - Handles pagination automatically
  - Uses randomized delay to avoid blocking

Requirements:
  pip install selenium beautifulsoup4 lxml pandas webdriver-manager

"""

import pandas as pd
import time
import random
import re
from datetime import date
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ----------------------------------------------
# CONFIG
# ----------------------------------------------

BASE_URL = "https://letterboxd.com"
START_URL = "https://letterboxd.com/films/popular/this/year/"
TODAY = str(date.today())

DELAY_MIN = 2
DELAY_MAX = 4


# ----------------------------------------------
# DRIVER (MAC SAFE)
# ----------------------------------------------

def make_driver():
    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    return driver


# ----------------------------------------------
# HELPER
# ----------------------------------------------

def get_soup(driver, url):
    try:
        driver.get(url)
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        return BeautifulSoup(driver.page_source, "lxml")
    except:
        return None


def safe_int(text):
    try:
        cleaned = re.sub(r"[^\d]", "", text)
        return int(cleaned) if cleaned else None
    except:
        return None


# ----------------------------------------------
# STEP 1: GET FILMS
# ----------------------------------------------

def get_films(driver):
    films = []
    page = 1

    while True:
        url = START_URL + f"page/{page}/"
        print(f"Scraping page {page}")

        soup = get_soup(driver, url)
        if not soup:
            break

        items = soup.select("li.poster-container") or soup.select("div.film-poster")

        if not items:
            print("No more films found.")
            break

        for item in items:
            title = None
            film_url = None

            a = item.select_one('a[href*="/film/"]')
            if a:
                film_url = BASE_URL + a.get("href")

            img = item.select_one("img")
            if img:
                raw = img.get("alt", "")
                raw = re.sub(r"^Poster for\s+", "", raw)
                raw = re.sub(r"\s*\(\d{4}\)$", "", raw).strip()
                title = raw

            if film_url:
                films.append((title, film_url))

        page += 1
        if page > 5:
            break

    return films


# ----------------------------------------------
# STEP 2: FILM DETAILS
# ----------------------------------------------

def scrape_film_details(driver, url):
    soup = get_soup(driver, url)

    data = {
        "film_title": None,
        "year": None,
        "genre": None,
        "runtime_mins": None,
        "avg_rating": None,
        "num_ratings": None,
        "num_reviews": None,
        "country": None,
        "language": None,
        "letterboxd_url": url,
        "date_scraped": TODAY
    }

    if not soup:
        return data

    # title
    # ✅ FIXED TITLE (targets actual movie title)
    tag = soup.select_one("h1.headline-1")

    if tag:
        data["film_title"] = tag.get_text(strip=True)
    else:
        # fallback just in case
        tag = soup.select_one("div.film-title-wrapper h1")
        if tag:
            data["film_title"] = tag.get_text(strip=True)

    # year
    year_tag = soup.select_one("a[href*='/films/year/']")
    if year_tag:
        data["year"] = year_tag.get_text(strip=True)

    # genre
    genres = [g.get_text(strip=True) for g in soup.select("a[href*='/films/genre/']")]
    data["genre"] = "|".join(genres) if genres else None

    # runtime
    for txt in soup.find_all(string=re.compile(r"\d+\s*mins")):
        m = re.search(r"(\d+)", txt)
        if m:
            data["runtime_mins"] = int(m.group(1))
            break

    # avg rating
    meta = soup.find("meta", {"name": "twitter:data2"})
    if meta:
        m = re.search(r"([\d.]+)", meta.get("content", ""))
        if m:
            data["avg_rating"] = float(m.group(1))

    # num ratings (FIXED)
    for txt in soup.find_all(string=re.compile(r"ratings")):
        data["num_ratings"] = safe_int(txt)
        break

    # num_reviews (FIXED)
    try:
        import json as _json, re as _re
        scripts = soup.find_all("script", type="application/ld+json")

        for script in scripts:
            raw = script.string or ""

            # remove CDATA wrapper
            raw = _re.sub(r"/\*.*?\*/", "", raw, flags=_re.DOTALL).strip()
            if not raw:
                continue

            try:
                json_data = _json.loads(raw)  # ✅ DO NOT overwrite `data`

                review_count = (json_data.get("aggregateRating") or {}).get("reviewCount")

                if review_count is not None:
                    data["num_reviews"] = int(review_count)  # ✅ FIXED
                    break

            except Exception:
                continue

    except Exception as e:
        print(f"  num_reviews error: {e}")

    # country
    countries = [c.get_text(strip=True) for c in soup.select("a[href*='/films/country/']")]
    data["country"] = "|".join(countries) if countries else None

    # language
    languages = [l.get_text(strip=True) for l in soup.select("a[href*='/films/language/']")]
    data["language"] = "|".join(languages) if languages else None

    return data


# ----------------------------------------------
# MAIN
# ----------------------------------------------

def run_scraper():
    print("\n🚀 Scraping Popular Films This Year...\n")

    driver = make_driver()

    try:
        films = get_films(driver)
        print(f"\nFound {len(films)} films. Scraping details...\n")

        all_data = []

        for title, url in films:
            details = scrape_film_details(driver, url)

            if details["film_title"] is None:
                details["film_title"] = title

            all_data.append(details)

        df = pd.DataFrame(all_data)
        df.to_csv("popular_films.csv", index=False)

        print(f"\n✅ DONE — {len(df)} films saved!")

    finally:
        driver.quit()
