import os
import json
import time
import requests
from collections import defaultdict

# Set this in your terminal before running, e.g.: export NYT_API_KEY="your_key"
API_KEY = os.environ.get("NYT_API_KEY")
DATA_DIR = os.path.join("data", "news")

def fetch_historic_month(year, month):
    url = f"https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 429:
        print("Rate limit hit! Waiting 30 seconds...")
        time.sleep(30)
        return fetch_historic_month(year, month)
        
    response.raise_for_status()
    docs = response.json().get("response", {}).get("docs", [])
    
    # Filter for World/International News (handling older decades' naming conventions)
    world_news_sections = ["World", "Foreign", "International", "Foreign Desk"]
    
    world_articles = []
    for doc in docs:
        section = doc.get("section_name", "")
        desk = doc.get("news_desk", "")
        
        if section in world_news_sections or desk in world_news_sections:
            world_articles.append(doc)
            
    # Group articles by their specific day
    articles_by_day = defaultdict(list)
    
    for article in world_articles:
        pub_date = article.get("pub_date")
        if not pub_date:
            continue
            
        day_string = pub_date.split("T")[0]
        
        # 1. Extract the best available summary
        abstract = article.get("abstract") or article.get("lead_paragraph", "No summary available.")
        
        # 2. Extract an image URL if one exists
        image_url = None
        multimedia = article.get("multimedia", [])
        
        # Handle both list and dictionary (though API usually returns list)
        img_path = None
        if isinstance(multimedia, list) and len(multimedia) > 0:
            img_path = multimedia[0].get("url")
        elif isinstance(multimedia, dict):
            img_path = multimedia.get("url")

        if img_path:
            image_url = img_path if img_path.startswith("http") else f"https://www.nytimes.com/{img_path}"

        # 3. Build the rich article object
        articles_by_day[day_string].append({
            "title": article.get("headline", {}).get("main", "No Title"),
            "url": article.get("web_url"),
            "published_date": pub_date,
            "category": article.get("section_name", "World"),
            "abstract": abstract,
            "image_url": image_url
        })

    # Sort the days and take only the top 5 per day (NYT generally returns them in order of importance/print layout)
    monthly_data = {}
    for day, articles in sorted(articles_by_day.items()):
        monthly_data[day] = articles[:5]
        
    return monthly_data

def main():
    if not API_KEY:
        raise ValueError("NYT_API_KEY environment variable is not set. Please export it first.")

    target_year = input("Enter the year you want to scrape (e.g., 1970): ").strip()
    
    if not target_year.isdigit() or len(target_year) != 4:
        print("Invalid year format.")
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"Starting historic scrape for {target_year} World News...")

    for month in range(1, 13):
        month_str = f"{month:02d}"
        print(f"Fetching data for {target_year}_{month_str}...")
        
        monthly_data = fetch_historic_month(target_year, month)
        
        if not monthly_data:
            print(f"No world news data found for {target_year}_{month_str}. Skipping.")
            continue
            
        file_path = os.path.join(DATA_DIR, f"{target_year}_{month_str}.json")
        with open(file_path, "w") as f:
            json.dump(monthly_data, f, indent=4)
        
        if month < 12:
            time.sleep(12)

    print(f"\nFinished scraping {target_year}! Files saved in {DATA_DIR}/.")

if __name__ == "__main__":
    main()
