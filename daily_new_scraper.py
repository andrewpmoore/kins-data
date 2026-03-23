import os
import json
import requests
from datetime import datetime

API_KEY = os.environ.get("NYT_API_KEY")
DATA_FILE = "headlines.json"

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {}

def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=4)

def fetch_daily_top_stories():
    url = f"https://api.nytimes.com/svc/topstories/v2/home.json?api-key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    
    data = response.json()
    top_5 = data.get("results", [])[:5]
    
    headlines = []
    for article in top_5:
        headlines.append({
            "title": article.get("title"),
            "url": article.get("url"),
            "published_date": article.get("published_date")
        })
    return headlines

def main():
    if not API_KEY:
        raise ValueError("NYT_API_KEY environment variable is not set.")

    data = load_data()
    now = datetime.now()
    year_str = str(now.year)
    month_str = f"{now.month:02d}"
    
    # Ensure nested dictionary structure exists
    if year_str not in data:
        data[year_str] = {}
    if month_str not in data[year_str]:
        data[year_str][month_str] = []

    print(f"Fetching daily stories for {year_str}-{month_str}...")
    today_headlines = fetch_daily_top_stories()
    
    # Append to the current month, preventing duplicate titles
    existing_titles = [h["title"] for h in data[year_str][month_str]]
    for h in today_headlines:
        if h["title"] not in existing_titles:
            data[year_str][month_str].append(h)

    save_data(data)
    print("Daily headlines successfully updated.")

if __name__ == "__main__":
    main()
