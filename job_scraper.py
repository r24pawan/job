#!/usr/bin/env python3
"""
Daily job aggregator:
- Prefers job API (Adzuna) if credentials provided (faster & legal).
- Falls back to RSS feeds (Indeed/AngelList/company career RSS) and simple parsing.
- Filters jobs posted within last 48 hours and matches experience/keywords/location.
- Outputs CSV locally and optionally pushes to Google Sheets or emails results.

NOTE: Replace placeholders and follow API ToS. This script is a template.
"""

import os
import csv
import json
import re
import requests
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus
import time

# ---------- CONFIG ----------
CONFIG = {
    "roles_regex": r"(Software Engineer|Full[- ]stack|Backend Developer|Java Developer)",
    "keywords": ["Java","Spring","Spring Boot","Java 8","Java 11","Microservices","Hibernate","J2EE","REST","Node.js","React","SQL","Multithreading","Servlets","Core Java","Data Structure"],
    "locations_allowed": ["India","Remote","Bengaluru","Bangalore"],
    "exp_min": 2,
    "exp_max": 4,
    "salary_min_lpa": 12,
    "salary_max_lpa": 20,
    "post_within_hours": 48,
    # APIs / feeds
    "use_adzuna": bool(os.getenv("ADZUNA_APP_ID") and os.getenv("ADZUNA_APP_KEY")),
    "adzuna_country": "in",   # 'in' for India Adzuna
    "rss_feeds": [
        # Add RSS feed URLs for job searches you control or that provide feeds
        # Example placeholders (replace with real feeds you configure):
        # "https://stackoverflow.com/jobs/feed?q=java&l=india",
        # "https://angel.co/jobs.rss?keywords=java"
    ],
    "output_csv": "job_results.csv",
    "output_limit": 200
}

# ---------- Helpers ----------
def contains_keywords(text):
    t = (text or "").lower()
    for kw in CONFIG["keywords"]:
        if kw.lower() in t:
            return True
    # fallback to role regex
    if re.search(CONFIG["roles_regex"], t, flags=re.I):
        return True
    return False

def parse_post_date_generic(posted_text):
    # Tries to parse relative "3 hours ago", "2 days ago" or ISO datetime.
    if not posted_text:
        return None
    text = posted_text.lower()
    now = datetime.now(timezone.utc)
    m = re.search(r"(\d+)\s*hour", text)
    if m:
        return now - timedelta(hours=int(m.group(1)))
    m = re.search(r"(\d+)\s*day", text)
    if m:
        return now - timedelta(days=int(m.group(1)))
    # try ISO-like
    try:
        return datetime.fromisoformat(posted_text.replace("Z","+00:00"))
    except Exception:
        return None

def within_window(dt):
    if not dt: return False
    return (datetime.now(timezone.utc) - dt).total_seconds() <= CONFIG["post_within_hours"]*3600

# ---------- Adzuna API fetch (preferred if available) ----------
def fetch_adzuna_jobs():
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    if not app_id or not app_key:
        return []
    results = []
    page = 1
    while True:
        url = (
            f"https://api.adzuna.com/v1/api/jobs/{CONFIG['adzuna_country']}/search/{page}"
            f"?app_id={app_id}&app_key={app_key}"
            f"&results_per_page=50&what={quote_plus(' OR '.join(CONFIG['keywords']))}&content-type=application/json"
        )
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            break
        data = r.json()
        for j in data.get("results", []):
            # Adzuna gives created date
            created = None
            if j.get("created"):
                try:
                    created = datetime.fromisoformat(j["created"].replace("Z","+00:00"))
                except:
                    created = None
            item = {
                "title": j.get("title"),
                "company": (j.get("company") or {}).get("display_name"),
                "location": j.get("location", {}).get("area", []),
                "location_plain": j.get("location", {}).get("display_name"),
                "remote": "remote" in (j.get("description","")+j.get("title","")).lower(),
                "skills": j.get("description"),
                "exp": None,
                "link": j.get("redirect_url") or j.get("redirect_url"),
                "posted_dt": created,
                "short_desc": j.get("description","")[:300]
            }
            results.append(item)
        # pagination guard
        if page*50 >= data.get("count",0) or page>5:
            break
        page += 1
        time.sleep(0.5)
    return results

# ---------- RSS fetch (fallback) ----------
def fetch_rss_jobs(rss_urls):
    # Minimal RSS parsing to extract title, link, pubDate, description
    import xml.etree.ElementTree as ET
    out = []
    for url in rss_urls:
        try:
            r = requests.get(url, timeout=15, headers={"User-Agent":"job-scraper/1.0"})
            root = ET.fromstring(r.content)
            # find items
            for item in root.findall(".//item")[:CONFIG["output_limit"]]:
                title = item.findtext("title") or ""
                link = item.findtext("link") or ""
                desc = (item.findtext("description") or "")[:1000]
                pub = item.findtext("pubDate") or item.findtext("published") or ""
                posted_dt = None
                try:
                    posted_dt = datetime.strptime(pub[:25], "%a, %d %b %Y %H:%M:%S").replace(tzinfo=timezone.utc)
                except:
                    posted_dt = parse_post_date_generic(pub)
                out.append({
                    "title": title, "company": "", "location_plain": "", "remote": "remote" in (title+desc).lower(),
                    "skills": desc, "exp": None, "link": link, "posted_dt": posted_dt, "short_desc": desc[:300]
                })
        except Exception as e:
            # continue on failure
            print("RSS fetch error", url, e)
    return out

# ---------- Filter & format ----------
def filter_jobs(items):
    filtered = []
    for it in items:
        # posted within last 48 hours
        if not it.get("posted_dt") or not within_window(it["posted_dt"]):
            continue
        text_blob = " ".join(filter(None, [it.get("title",""), it.get("skills",""), it.get("short_desc","")]))
        if not contains_keywords(text_blob):
            continue
        # location filter
        loc_ok = False
        loc_text = (it.get("location_plain") or " ".join(it.get("location") if isinstance(it.get("location"), list) else [it.get("location","")])).lower()
        for loc in CONFIG["locations_allowed"]:
            if loc.lower() in loc_text:
                loc_ok = True
                break
        if not loc_ok and not it.get("remote"):
            continue
        # experience filter if numeric exp is provided
        exp = it.get("exp")
        if isinstance(exp, (int,float)):
            if exp < CONFIG["exp_min"] or exp > CONFIG["exp_max"]:
                continue
        filtered.append(it)
    return filtered

# ---------- Outputs ----------
def save_csv(items, path=CONFIG["output_csv"]):
    keys = ["title","company","location_plain","remote","skills","exp","link","posted_dt","short_desc"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for it in items:
            row = {k: (it.get(k) if not isinstance(it.get(k), datetime) else it.get(k).isoformat()) for k in keys}
            w.writerow(row)

def main():
    all_items = []
    if CONFIG["use_adzuna"]:
        print("Fetching Adzuna jobs...")
        all_items.extend(fetch_adzuna_jobs())
    # RSS fallback
    if CONFIG["rss_feeds"]:
        all_items.extend(fetch_rss_jobs(CONFIG["rss_feeds"]))
    # Deduplicate by link/title
    seen = set()
    dedup = []
    for it in all_items:
        key = (it.get("link") or it.get("title","")).strip()
        if not key or key in seen: continue
        seen.add(key)
        dedup.append(it)
    filtered = filter_jobs(dedup)
    save_csv(filtered)
    print("Found", len(filtered), "jobs. Saved to", CONFIG["output_csv"])

    # Optionally: push to Google Sheets, send email, or upload artifact via GitHub Actions
    # This script ends here. Use GH Actions artifacts or action steps for emailing/publishing.

if __name__ == "__main__":
    main()
