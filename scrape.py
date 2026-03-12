import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import os
import io
import time

# List of hubs for English Wikipedia
COUNTRY_HUBS = {
    'us': 'Lists_of_Billboard_number-one_singles',
    'uk': 'Lists_of_UK_Singles_Chart_number_ones',
    'de': 'List_of_number-one_hits_(Germany)',
    'es': 'List_of_number-one_hits_(Spain)',
    'fr': 'List_of_number-one_singles_in_France',
    'it': 'List_of_number-one_hits_(Italy)',
    'jp': 'List_of_Oricon_number-one_singles',
    'ca': 'List_of_number-one_singles_in_Canada',
    'au': 'List_of_number-one_singles_in_Australia',
    'br': 'Category:Lists_of_number-one_songs_in_Brazil',
    'mx': 'Category:Lists_of_number-one_songs_in_Mexico',
    'in': 'Category:Lists_of_number-one_songs_in_India',
    'kr': 'Category:Lists_of_number-one_songs_in_South_Korea',
    'nl': 'List_of_number-one_singles_in_the_Dutch_Top_40',
    'se': 'List_of_number-one_singles_and_albums_in_Sweden',
    'no': 'List_of_number-one_songs_in_Norway',
    'dk': 'List_of_number-one_hits_(Denmark)',
    'ie': 'List_of_songs_that_reached_number_one_on_the_Irish_Singles_Chart',
    'ch': 'List_of_number-one_singles_in_Switzerland',
    'at': 'List_of_number-one_singles_(Austria)',
    'be': 'List_of_number-one_hits_(Belgium)',
    'pl': 'List_of_number-one_singles_in_Poland',
    'tr': 'Category:Lists_of_number-one_songs_in_Turkey',
    'za': 'Category:Lists_of_number-one_songs_in_South_Africa',
    'nz': 'Category:Lists_of_number-one_songs_in_New_Zealand'
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
    if not any(char.isdigit() for char in date_str[-4:]) and default_year:
        date_str = f"{date_str} {default_year}"
    formats = [
        '%d %B %Y', '%B %d %Y', '%B %d, %Y', '%d %b %Y', 
        '%Y-%m-%d', '%Y/%m/%d', '%d.%m.%Y', '%m/%d/%Y'
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
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
        
        # If it's a category page, we look for pages within it
        if 'Category:' in hub_title:
            target_div = soup.find('div', {'id': 'mw-pages'})
            if target_div:
                for a in target_div.find_all('a', href=True):
                    href = a['href']
                    title = a.get('title', '')
                    if not href.startswith('/wiki/'): continue
                    if 'list' in title.lower() and re.search(r'\b(19|20)\d{2}\b', title):
                        links.add(f"https://en.wikipedia.org{href}")
        else:
            # Normal Hub page
            decade_links = set()
            year_links = set()
            for a in soup.find_all('a', href=True):
                href = a['href']
                title = a.get('title', '')
                if not href.startswith('/wiki/'): continue
                if any(x in title.lower() for x in ['album', 'list of years', 'music in', 'category:']): continue
                if 'number' in title.lower() or 'chart' in title.lower() or 'hits' in title.lower() or 'singles' in title.lower():
                    if re.search(r'\b(19|20)\d{2}s\b', title):
                        decade_links.add(f"https://en.wikipedia.org{href}")
                    elif re.search(r'\b(19|20)\d{2}\b', title):
                        year_links.add(f"https://en.wikipedia.org{href}")
            
            if decade_links:
                covered_decades = []
                for d_url in decade_links:
                    m = re.search(r'\b((19|20)\d)0s\b', d_url)
                    if m: covered_decades.append(m.group(1))
                filtered_years = []
                for y_url in year_links:
                    m = re.search(r'\b((19|20)\d)\d\b', y_url)
                    if m and m.group(1) not in covered_decades:
                        filtered_years.append(y_url)
                    elif not m:
                        filtered_years.append(y_url)
                links = decade_links.union(set(filtered_years))
            else:
                links = year_links
        
        return list(links) if links else [url]
    except Exception as e:
        print(f"⚠️ Error finding links for {country_code}: {e}")
        return [url]

def scrape_country(country_code, hub_title):
    links = get_sub_links(country_code, hub_title)
    all_data = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    links.sort()
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
                try:
                    df = pd.read_html(io.StringIO(str(table)))[0]
                except: continue
                if isinstance(df.columns, pd.MultiIndex):
                    best_level = 0
                    for level in range(df.columns.nlevels):
                        cols = [str(c).lower() for c in df.columns.get_level_values(level)]
                        if any(any(k in c for k in ['artist', 'song', 'single', 'date', 'title', 'week', 'performer']) for c in cols):
                            best_level = level
                            break
                    df.columns = df.columns.get_level_values(best_level)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [' '.join(col).strip() for col in df.columns.values]
                df.columns = [re.sub(r'\[.*?\]', '', str(col)).strip() for col in df.columns]
                
                date_col, artist_col, song_col, weeks_col = None, None, None, None
                for col in df.columns:
                    col_lower = str(col).lower()
                    if ('date' in col_lower or 'week' in col_lower or 'reached' in col_lower) and 'weeks at' not in col_lower and 'weeks on' not in col_lower:
                        if not date_col: date_col = col
                    elif any(k in col_lower for k in ['artist', 'performer', 'group', 'band']):
                        if not artist_col: artist_col = col
                    elif any(k in col_lower for k in ['single', 'song', 'title', 'track']):
                        if not song_col: song_col = col
                    elif any(k in col_lower for k in ['weeks at', 'weeks on top', 'weeks at number']):
                        if not weeks_col: weeks_col = col
                    elif col_lower == 'weeks':
                        if not weeks_col: weeks_col = col
                
                if date_col and artist_col and song_col:
                    temp_df = df.rename(columns={date_col: 'raw_date', artist_col: 'artist', song_col: 'song'})
                    if weeks_col: temp_df = temp_df.rename(columns={weeks_col: 'weeks_at_one'})
                    else: temp_df['weeks_at_one'] = 1
                    temp_df = temp_df.ffill()
                    temp_df['song'] = temp_df['song'].apply(clean_text)
                    temp_df['artist'] = temp_df['artist'].apply(clean_text)
                    temp_df['dt'] = temp_df['raw_date'].apply(lambda x: parse_date(x, default_year))
                    temp_df = temp_df.dropna(subset=['dt', 'song'])
                    temp_df = temp_df[temp_df['song'] != '']
                    
                    expanded_rows = []
                    for _, row in temp_df.iterrows():
                        try:
                            w_val = str(row['weeks_at_one'])
                            num_weeks = 1
                            if w_val and any(char.isdigit() for char in w_val):
                                w_match = re.search(r'\d+', w_val)
                                if w_match: num_weeks = int(w_match.group(0))
                        except: num_weeks = 1
                        start_date = row['dt']
                        if num_weeks > 52: num_weeks = 52
                        for i in range(num_weeks):
                            current_date = start_date + timedelta(weeks=i)
                            expanded_rows.append({
                                'date': current_date.strftime('%Y-%m-%d'),
                                'rank': 1,
                                'song': row['song'],
                                'artist': row['artist']
                            })
                    if expanded_rows:
                        all_data.append(pd.DataFrame(expanded_rows))
            time.sleep(0.1)
        except Exception as e:
            print(f"⚠️ Error scraping {url}: {e}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['date', 'song'])
        final_df = final_df.sort_values(by='date')
        os.makedirs('data', exist_ok=True)
        filename = f'data/music_{country_code}.csv'
        final_df.to_csv(filename, index=False)
        print(f"✅ DONE: {country_code.upper()} -> {len(final_df)} entries saved to {filename}")
    else:
        print(f"❌ No data found for {country_code}")

def main():
    print("🚀 Starting Global Music Chart Scraper...")
    priority = ['uk', 'us']
    for code in priority:
        if code in COUNTRY_HUBS:
            scrape_country(code, COUNTRY_HUBS[code])
    for code, hub in COUNTRY_HUBS.items():
        if code not in priority:
            scrape_country(code, hub)

if __name__ == "__main__":
    main()
