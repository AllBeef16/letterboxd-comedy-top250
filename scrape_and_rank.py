# scrape_and_rank.py
# Bi-weekly automation to build "Top 250 Highest-Rated Comedy (>= 5,000 ratings)" from Letterboxd browse pages.

import csv, json, re, subprocess, sys, pathlib

# ------------- CONFIG: Put your Letterboxd Comedy browse URLs here -------------
SRC_LIST_URLS = [
    "https://letterboxd.com/films/popular/genre/comedy+-documentary/"  # e.g., https://letterboxd.com/films/genre/comedy/
    # You can add more URLs if you like, one per line.
]

OUT_CSV = pathlib.Path("top250_comedy.csv")
OUT_JSON = pathlib.Path("top250_comedy.json")

def parse_count(text):
    """Convert counts like '253k' or '12,345' to int."""
    if not text:
        return 0
    t = text.lower().strip()
    # Handle 'k' suffix
    if t.endswith('k'):
        # e.g., '2.3k' -> 2300
        try:
            return int(float(t[:-1]) * 1000)
        except:
            return 0
    # Strip non-digits (commas/spaces)
    t = re.sub(r"[^\d]", "", t)
    return int(t) if t.isdigit() else 0

def run_scraper(url):
    """
    Calls L-Dot’s scraper via `python -m listscraper`.
    This tool supports generic films pages (genre, popular feeds, etc.).
    """
    cmd = ["python", "-m", "listscraper", url]
    subprocess.run(cmd, check=True)

def collect_records():
    """
    Collects per-film rows from scraper outputs in ./scraper_outputs/.
    The scraper can emit fields like:
    film_title, release_year, average_rating, ratings_count, film_url, ...
    """
    out_dir = pathlib.Path("scraper_outputs")
    rows = []
    for f in sorted(out_dir.glob("*.csv")):
        with f.open(newline="", encoding="utf-8") as fh:
            r = csv.DictReader(fh)
            for row in r:
                title = row.get("film_title") or row.get("title") or ""
                year = row.get("release_year") or row.get("year") or ""
                avg = row.get("average_rating") or row.get("avg_rating") or ""
                count = row.get("ratings_count") or ""
                url = row.get("film_url") or row.get("letterboxd_url") or ""
                # Build record
                try:
                    avg_val = float(avg)
                except:
                    avg_val = 0.0
                rows.append({
                    "title": title.strip(),
                    "year": year.strip(),
                    "avg_rating": avg_val,
                    "ratings_count": parse_count(count),
                    "url": url.strip(),
                })
    return rows

def post_filters(rows):
    """
    Apply your curation rules:
    - ratings_count >= 5000 (Letterboxd doesn't offer this filter in UI)
    - sort by avg_rating desc, then ratings_count desc
    - keep top 250
    """
    # Threshold
    eligible = [r for r in rows if r["ratings_count"] >= 5000]
    # Sort
    eligible.sort(key=lambda r: (r["avg_rating"], r["ratings_count"]), reverse=True)
    # Keep Top 250
    return eligible[:250]

def write_outputs(rows):
    # CSV
    with OUT_CSV.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["Title", "Year", "Letterboxd URL", "Avg Rating", "Ratings Count"])
        for r in rows:
            w.writerow([r["title"], r["year"], r["url"], r["avg_rating"], r["ratings_count"]])
    # JSON
    with OUT_JSON.open("w", encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=2)

def main():
    # 1) Scrape each source URL (genre/browse pages)
    for url in SRC_LIST_URLS:
        run_scraper(url)  # ~1.2 films/sec, with pagination handled by the tool (see README)

    # 2) Aggregate all results
    rows = collect_records()

    # 3) Apply threshold + ranking
    top250 = post_filters(rows)

    # 4) Write outputs
    write_outputs(top250)

if __name__ == "__main__":
    sys.exit(main())
