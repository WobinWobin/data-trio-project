from serpapi import GoogleSearch
import pandas as pd
import time

# -------------------------------
# CONFIG
# -------------------------------
SERPAPI_KEY = "179bd7fcfd4c4a9b7de4867a3d5e9eef6c7ae75d4945f44d483303af307ea47d"

activities = []

# -------------------------------
# STEP 1: SCRAPE GOOGLE MAPS
# -------------------------------
def scrape_maps(query, query_category):
    """
    Scrapes Google Maps search results using SerpAPI and extracts activity data.

    This function sends a search request to Google Maps based on the provided query,
    retrieves a list of places, and extracts relevant information such as activity name,
    category, and address. The results are appended to the global 'activities' list.

    Parameters
    ----------
    query : str
        The search term used to find places (e.g., "arcade Makati")

    query_category : str
        The category label assigned to the query (e.g., "ARCADE_GAMING")

    Returns
    -------
    None
        The function does not return anything but appends results to the 'activities' list.
    """
    
    params = {
        "engine": "google_maps",
        "q": query,
        "ll": "@14.5547,121.0244,14z",
        "type": "search",
        "api_key": SERPAPI_KEY,
        "hl": "en"
    }

    search = GoogleSearch(params)
    results = search.get_dict()

    if "local_results" not in results:
        print(f"❌ No results for {query}")
        return

    for place in results["local_results"]:
        name = place.get("title")
        category = place.get("type") or "Unknown"
        address = place.get("address")

        if not name or not address:
            continue

        activities.append({
            "activity_name": name,
            "category": category,
            "location_area": address
        })

# -------------------------------
# QUERIES WITH CATEGORY LABELS
# -------------------------------
queries = {

    "CORE": [
        "things to do Makati","activities Makati","fun activities Makati",
        "places to visit Makati","fun places Makati","hangout places Makati",
        "date ideas Makati","things to do at night Makati"
    ],

    "ARCADE_GAMING": [
        "arcade Makati","Timezone Makati","TimeZone Glorietta",
        "Quantum arcade Makati","Tom's World Makati",
        "gaming center Makati","gaming lounge Makati",
        "internet cafe Makati","lan gaming Makati",
        "ps5 lounge Makati","console gaming Makati",
        "vr gaming Makati","virtual reality Makati",
        "simulator gaming Makati","racing simulator Makati"
    ],

    "INTERACTIVE": [
        "escape room Makati","puzzle room Makati","mystery room Makati",
        "immersive experience Makati","interactive experience Makati",
        "experience center Makati","funhouse Makati","mirror maze Makati"
    ],

    "KARAOKE": [
        "karaoke Makati","videoke Makati","ktv Makati",
        "singing rooms Makati","music lounge Makati"
    ],

    "CINEMA": [
        "cinema Makati","movie theater Makati","IMAX Makati",
        "indie cinema Makati","film screening Makati"
    ],

    "NIGHTLIFE": [
        "bars Makati","pub Makati","club Makati",
        "nightclub Makati","rooftop bar Makati","speakeasy Makati"
    ],

    "ACTION_FUN": [
        "go kart Makati","karting Makati","bowling alley Makati",
        "laser tag Makati","archery Makati"
    ],

    "CREATIVE": [
        "painting class Makati","pottery class Makati",
        "art workshop Makati","DIY workshop Makati"
    ],

    "WELLNESS": [
        "spa Makati","massage Makati","wellness center Makati",
        "meditation center Makati"
    ],

    "OUTDOOR": [
        "parks Makati","gardens Makati","walking areas Makati"
    ],

    "TRENDY": [
        "instagrammable places Makati","hidden spots Makati",
        "cool spots Makati","unique places Makati"
    ]
}

# -------------------------------
# RUN SCRAPER
# -------------------------------
print("🔄 STARTING SCRAPE...\n")

for category, query_list in queries.items():
    print(f"\n📂 CATEGORY: {category}")
    for q in query_list:
        print(f"🔍 {q}")
        scrape_maps(q, category)
        time.sleep(1)

# -------------------------------
# CLEAN DATA
# -------------------------------
df = pd.DataFrame(activities)
df = df.drop_duplicates(subset=["activity_name", "location_area"])

# -------------------------------
# PRINT RESULTS
# -------------------------------
print("\n✅ FINAL LIST:\n")

for i, row in df.iterrows():
    print(f"{row['activity_name']} | {row['category']} | {row['location_area']}")

# -------------------------------
# SAVE CSV
# -------------------------------
df.to_csv("makati_activities.csv", index=False)

print(f"\n📁 Saved to makati_activities.csv ({len(df)} places)")