import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import os
import io
import time

# List of hubs for English Wikipedia Box Office
MOVIE_HUBS = {
    'us': 'Lists_of_box_office_number-one_films_in_North_America',
    'ca': 'Lists_of_box_office_number-one_films_in_North_America',
    'uk': 'Lists_of_box_office_number-one_films_in_the_United_Kingdom',
    'ie': 'Lists_of_box_office_number-one_films_in_the_United_Kingdom',
    'de': 'Lists_of_box_office_number-one_films_in_Germany',
    'es': 'Lists_of_box_office_number-one_films_in_Spain',
    'fr': 'Lists_of_box_office_number-one_films_in_France',
    'it': 'Lists_of_box_office_number-one_films_in_Italy',
    'jp': 'Lists_of_box_office_number-one_films_in_Japan',
    'au': 'Lists_of_box_office_number-one_films_in_Australia',
    'br': 'Lists_of_box_office_number-one_films_in_Brazil',
    'mx': 'Lists_of_box_office_number-one_films_in_Mexico',
    'in': 'Lists_of_box_office_number-one_films_in_India',
    'kr': 'Lists_of_box_office_number-one_films_in_South_Korea',
    'nl': 'Lists_of_box_office_number-one_films_in_the_Netherlands',
    'se': 'Lists_of_box_office_number-one_films_in_Sweden',
    'no': 'Lists_of_box_office_number-one_films_in_Norway',
    'dk': 'Lists_of_box_office_number-one_films_in_Denmark',
    'ch': 'Lists_of_box_office_number-one_films_in_Switzerland',
    'at': 'Lists_of_box_office_number-one_films_in_Austria',
    'be': 'Lists_of_box_office_number-one_films_in_Belgium',
    'pl': 'Lists_of_box_office_number-one_films_in_Poland',
    'tr': 'Lists_of_box_office_number-one_films_in_Turkey',
    'za': 'Lists_of_box_office_number-one_films_in_South_Africa',
    'nz': 'Lists_of_box_office_number-one_films_in_New_Zealand'
}

def clean_text(text):
    if pd.isna(text): return ""
    text = str(text)
    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r'\(.*?\)', '', text)
    return text.strip().strip('"').strip('«').strip('»').strip()

def parse_date(date_str, default_year=None):
    if pd.isna(date_str): return None
    date_str = clean_text(date_str)
    if '–' in date_str: date_str = date_str.split('–')[0].strip()
    elif '-' in date_str: date_str = date_str.split('-')[0].strip()
    if not any(char.isdigit() for char in date_str[-4:]) and default_year:
        date_str = f"{date_str} {default_year}"
    formats = [
        '%d %B %Y', '%B %d %Y', '%B %d, %Y', '%d %b %Y', 
        '%Y-%m-%d', '%Y/%m/%d', '%d.%m.%Y', '%m/%d/%Y', '%B %d'
    ]
    for fmt in formats:
        try: return datetime.strptime(date_str, fmt)
        except ValueError: continue
    return None

def get_sub_links(country_code, hub_title):
    url = f"https://en.wikipedia.org/wiki/{hub_title}"
    print(f"🔍 Finding sub-links for {country_code.upper()} from {url}...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200: return [url]
        soup = BeautifulSoup(response.content, 'html.parser')
        
        links = set()
        if 'Category:' in hub_title:
            target_div = soup.find('div', {'id': 'mw-pages'})
            if target_div:
                for a in target_div.find_all('a', href=True):
                    href = a['href']
                    title = a.get('title', '')
                    if not href.startswith('/wiki/'): continue
                    if 'box office' in title.lower() and re.search(r'\b(19|20)\d{2}\b', title):
                        links.add(f"https://en.wikipedia.org{href}")
        else:
            for a in soup.find_all('a', href=True):
                href = a['href']
                title = a.get('title', '')
                if not href.startswith('/wiki/'): continue
                if 'box office' in title.lower() and re.search(r'\b(19|20)\d{2}\b', title):
                    links.add(f"https://en.wikipedia.org{href}")
        
        return list(links) if links else [url]
    except Exception as e:
        print(f"⚠️ Error finding links for {country_code}: {e}")
        return [url]

def scrape_country(country_code, hub_title):
    links = get_sub_links(country_code, hub_title)
    all_data = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    links.sort(reverse=True)
    processed_urls = set()
    
    for url in links:
        if url in processed_urls: continue
        processed_urls.add(url)
        print(f"🌍 Scraping {url}...")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(response.content, 'html.parser')
            tables = soup.find_all('table', {'class': 'wikitable'})
            url_year_match = re.search(r'(19|20)\d{2}', url)
            default_year = url_year_match.group(0) if url_year_match else None
            
            for table in tables:
                try: df = pd.read_html(io.StringIO(str(table)))[0]
                except: continue
                if isinstance(df.columns, pd.MultiIndex):
                    best_level = 0
                    for level in range(df.columns.nlevels):
                        cols = [str(c).lower() for c in df.columns.get_level_values(level)]
                        if any(any(k in c for k in ['film', 'movie', 'title', 'date', 'weekend', 'week']) for c in cols):
                            best_level = level
                            break
                    df.columns = df.columns.get_level_values(best_level)
                df.columns = [re.sub(r'\[.*?\]', '', str(col)).strip() for col in df.columns]
                
                date_col, film_col = None, None
                for col in df.columns:
                    col_lower = str(col).lower()
                    if any(k in col_lower for k in ['date', 'weekend', 'week']):
                        if not date_col: date_col = col
                    elif any(k in col_lower for k in ['film', 'movie', 'title']):
                        if not film_col: film_col = col
                
                if date_col and film_col:
                    temp_df = df.rename(columns={date_col: 'raw_date', film_col: 'film'})
                    temp_df = temp_df.ffill()
                    temp_df['film'] = temp_df['film'].apply(clean_text)
                    temp_df['dt'] = temp_df['raw_date'].apply(lambda x: parse_date(x, default_year))
                    temp_df = temp_df.dropna(subset=['dt', 'film'])
                    temp_df = temp_df[temp_df['film'] != '']
                    
                    rows = []
                    for _, row in temp_df.iterrows():
                        rows.append({'date': row['dt'].strftime('%Y-%m-%d'), 'rank': 1, 'film': row['film']})
                    if rows: all_data.append(pd.DataFrame(rows))
            time.sleep(0.1)
        except Exception as e: print(f"⚠️ Error scraping {url}: {e}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['date', 'film'])
        final_df = final_df.sort_values(by='date')
        os.makedirs('data_movies', exist_ok=True)
        filename = f'data_movies/movies_{country_code}.csv'
        final_df.to_csv(filename, index=False)
        print(f"✅ DONE: {country_code.upper()} -> {len(final_df)} entries saved to {filename}")
    else: print(f"❌ No data found for {country_code}")

def main():
    print("🎬 Starting Global Box Office Scraper...")
    for code, hub in MOVIE_HUBS.items():
        scrape_country(code, hub)

if __name__ == "__main__":
    main()
