# scrape_and_rank.py
# Builds "Top 250 Highest-Rated Comedy (>= 5,000 ratings)" from Letterboxd browse pages.

import csv, json, re, subprocess, sys, pathlib

# ------------- CONFIG: Put your Letterboxd Comedy browse URLs here -------------
SRC_LIST_URLS = [
    "https://letterboxd.com/films/genre/comedy/"  # e.g., https://letterboxd.com/films/genre/comedy/
]

OUT_CSV = pathlib.Path("top250_comedy.csv")
OUT_JSON = pathlib.Path("top250_comedy.json")
SCRAPER_DIR = pathlib.Path("Letterboxd-list-scraper")  # cloned by the workflow

def parse_count(text):
    """Convert counts like '253k' or '12,345' to int."""
    if not text:
        return 0
    t = text.lower().strip()
    if t.endswith('k'):
        try:
            return int(float(t[:-1]) * 1000)
        except:
            return 0
    t = re.sub(r"[^\d]", "", t)
    return int(t) if t.isdigit() else 0

def run_scraper(url):
    """
    Execute L-Dot's listscraper as a module from the cloned repo.
    We run inside the repo directory so the module resolves as intended.
    """
    cmd = ["python", "-m", "listscraper", url]
    subprocess.run(cmd, check=True, cwd=SCRAPER_DIR)

def collect_records():
    """
    Collect per-film rows from scraper outputs in ./scraper_outputs/.
    """
    out_dir = pathlib.Path("scraper_outputs")
    out_dir.mkdir(exist_ok=True)
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
    Your curation rules:
    - ratings_count >= 5000
    - sort by avg_rating desc, then ratings_count desc
    - keep top 250
    """
    eligible = [r for r in rows if r["ratings_count"] >= 5000]
    eligible.sort(key=lambda r: (r["avg_rating"], r["ratings_count"]), reverse=True)
    return eligible[:250]

def write_outputs(rows):
    with OUT_CSV.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["Title", "Year", "Letterboxd URL", "Avg Rating", "Ratings Count"])
        for r in rows:
            w.writerow([r["title"], r["year"], r["url"], r["avg_rating"], r["ratings_count"]])
    with OUT_JSON.open("w", encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=2)

def main():
    # 1) Scrape each source URL (genre/browse pages)
    for url in SRC_LIST_URLS:
        run_scraper(url)

    # 2) Aggregate results
    rows = collect_records()

    # 3) Apply threshold + ranking
    top250 = post_filters(rows)

    # 4) Write outputs
    write_outputs(top250)

if __name__ == "__main__":
    sys.exit(main())
