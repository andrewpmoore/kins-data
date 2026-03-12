import csv
import json
import os
import requests
import re
import math
import urllib.parse
from datetime import datetime, timedelta

# Configuration
OUTPUT_DIR = 'api'
TMDB_TOKEN = os.environ.get('TMDB_TOKEN')

# Country to Wikipedia Language Mapping
COUNTRIES = {
    'us': 'en', 'uk': 'en', 'de': 'de', 'es': 'es', 'fr': 'fr',
    'it': 'it', 'jp': 'ja', 'ca': 'en', 'au': 'en', 'br': 'pt',
    'mx': 'es', 'in': 'en', 'kr': 'ko', 'nl': 'nl', 'se': 'sv',
    'no': 'no', 'dk': 'da', 'ie': 'en', 'ch': 'de', 'at': 'de',
    'be': 'nl', 'pl': 'pl', 'tr': 'tr', 'za': 'en', 'nz': 'en'
}

# Regional Demonyms for the English feed sorting
DEMONYMS = {
    'uk': ['british', 'english', 'scottish', 'welsh', 'northern irish'],
    'au': ['australian'],
    'ca': ['canadian'],
    'ie': ['irish'],
    'nz': ['new zealand', 'kiwi'],
    'za': ['south african'],
    'in': ['indian'],
    'us': ['american']
}

# --- TEXT HELPERS ---
def clean_html(raw_html):
    cleanr = re.compile('<.*?>')
    return re.sub(cleanr, '', raw_html)

# --- MATH & SYMBOL CALCULATORS ---
def get_symbols(month, day, year):
    month, day, year = int(month), int(day), int(year)
    zodiacs = [
        (1, 20, "Capricorn"), (2, 19, "Aquarius"), (3, 20, "Pisces"), (4, 20, "Aries"),
        (5, 21, "Taurus"), (6, 21, "Gemini"), (7, 22, "Cancer"), (8, 23, "Leo"),
        (9, 23, "Virgo"), (10, 23, "Libra"), (11, 22, "Scorpio"), (12, 22, "Sagittarius"),
        (12, 31, "Capricorn")
    ]
    zodiac = next(z for m, d, z in zodiacs if month < m or (month == m and day <= d))
    if month == 1 and day <= 19: zodiac = "Capricorn"
    animals = ["Monkey", "Rooster", "Dog", "Pig", "Rat", "Ox", "Tiger", "Rabbit", "Dragon", "Snake", "Horse", "Sheep"]
    chinese_zodiac = animals[year % 12]
    monthly_data = {
        1: ("Garnet", "Carnation"), 2: ("Amethyst", "Violet"), 3: ("Aquamarine", "Daffodil"),
        4: ("Diamond", "Daisy"), 5: ("Emerald", "Lily of the Valley"), 6: ("Pearl", "Rose"),
        7: ("Ruby", "Larkspur"), 8: ("Peridot", "Gladiolus"), 9: ("Sapphire", "Aster"),
        10: ("Opal", "Marigold"), 11: ("Topaz", "Chrysanthemum"), 12: ("Turquoise", "Narcissus")
    }
    stone, flower = monthly_data.get(month, ("Unknown", "Unknown"))
    return {"zodiac": zodiac, "chinese_zodiac": chinese_zodiac, "birthstone": stone, "birth_flower": flower}

def get_moon_phase(year, month, day):
    year, month, day = int(year), int(month), int(day)
    if month < 3:
        year -= 1; month += 12
    month += 1
    c = 365.25 * year
    e = 30.6 * month
    jd = c + e + day - 694039.09 
    phase = jd / 29.5305882
    phase -= math.floor(phase)
    if phase < 0.03 or phase > 0.97: return "New Moon"
    elif phase < 0.22: return "Waxing Crescent"
    elif phase < 0.28: return "First Quarter"
    elif phase < 0.47: return "Waxing Gibbous"
    elif phase < 0.53: return "Full Moon"
    elif phase < 0.72: return "Waning Gibbous"
    elif phase < 0.78: return "Last Quarter"
    else: return "Waning Crescent"

# --- API FETCHERS WITH IMAGES & SMART SORTING ---
wiki_network_cache = {}
wiki_history_cache = {}
pageviews_cache = {}

def get_pageviews(title, lang):
    if title in pageviews_cache: return pageviews_cache[title]
    end = datetime.utcnow()
    start = end - timedelta(days=30)
    start_str = start.strftime('%Y%m%d00')
    end_str = end.strftime('%Y%m%d00')
    
    safe_title = urllib.parse.quote(title)
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{lang}.wikipedia/all-access/all-agents/{safe_title}/daily/{start_str}/{end_str}"
    
    try:
        res = requests.get(url, headers={'User-Agent': 'BirthdayTimeCapsuleApp/1.0'}, timeout=5)
        data = res.json()
        views = sum(item.get('views', 0) for item in data.get('items', []))
        pageviews_cache[title] = views
        return views
    except:
        pageviews_cache[title] = 0
        return 0

def fetch_historical_fact(lang_code, month, day):
    cache_key = f"{lang_code}-{month}-{day}"
    if cache_key in wiki_history_cache: return wiki_history_cache[cache_key]
        
    url = f"https://api.wikimedia.org/feed/v1/wikipedia/{lang_code}/onthisday/events/{month}/{day}"
    headers = {'User-Agent': 'BirthdayTimeCapsuleApp/1.0'}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        events = res.json().get('events', [])
        if events:
            event = events[-1]
            year = event.get('year', 'History')
            text = clean_html(event.get('text', ''))
            image_url = None
            pages = event.get('pages', [])
            if pages and 'thumbnail' in pages[0]:
                image_url = pages[0]['thumbnail'].get('source')
            
            fact_data = {"text": f"In {year}: {text}", "image_url": image_url}
            wiki_history_cache[cache_key] = fact_data
            return fact_data
    except: pass
    wiki_history_cache[cache_key] = None
    return None

def fetch_smart_births(folder, lang_code, month, day):
    net_key = f"{lang_code}-{month}-{day}"
    if net_key not in wiki_network_cache:
        url = f"https://api.wikimedia.org/feed/v1/wikipedia/{lang_code}/onthisday/births/{month}/{day}"
        headers = {'User-Agent': 'BirthdayTimeCapsuleApp/1.0 (bot)'}
        try:
            res = requests.get(url, headers=headers, timeout=10)
            wiki_network_cache[net_key] = res.json().get('births', [])
        except: wiki_network_cache[net_key] = []
            
    births = wiki_network_cache[net_key]
    candidates = []
    
    for person in births:
        try:
            year = int(person.get('year', 0))
            if year >= 1950:
                pages = person.get('pages', [])
                if pages and 'thumbnail' in pages[0]:
                    page = pages[0]
                    title = page.get('normalizedtitle', '')
                    img = page['thumbnail'].get('source')
                    profession = page.get('description', '')
                    
                    if not profession:
                        parts = person.get('text', '').split(',', 1)
                        profession = parts[1].strip() if len(parts) > 1 else "Public Figure"
                    profession = profession[:1].upper() + profession[1:] if profession else "Public Figure"
                    name = clean_html(person.get('text', '').split(',')[0])
                    raw_bio = (page.get('extract', '') + " " + profession).lower()
                    
                    candidates.append({
                        'text': f"{name} ({year})",
                        'title': title, 'image_url': img,
                        'profession': profession, 'raw_bio': raw_bio,
                        'fame_score': len(page.get('extract', ''))
                    })
        except: continue
        
    candidates.sort(key=lambda x: x['fame_score'], reverse=True)
    top_candidates = candidates[:15]
    
    for c in top_candidates:
        c['fame_score'] = get_pageviews(c['title'], lang_code)
        
    top_candidates.sort(key=lambda x: x['fame_score'], reverse=True)
    regional_matches, global_matches = [], []
    
    if folder in DEMONYMS:
        keywords = DEMONYMS[folder]
        for c in top_candidates:
            if any(k in c['raw_bio'] for k in keywords):
                regional_matches.append(c)
            else:
                global_matches.append(c)
    else:
        regional_matches = top_candidates
        
    results = (regional_matches + global_matches)[:3]
    return [{'text': r['text'], 'profession': r['profession'], 'image_url': r['image_url']} for r in results] if results else None

def fetch_regional_movie(country_code):
    if not TMDB_TOKEN: return None
    headers = {"Authorization": f"Bearer {TMDB_TOKEN}"}
    url = (f"https://api.themoviedb.org/3/discover/movie"
           f"?region={country_code.upper()}&sort_by=popularity.desc&with_release_type=2|3")
    try:
        res = requests.get(url, headers=headers, timeout=10)
        results = res.json().get('results', [])
        if results:
            top_movie = results[0]
            poster_path = top_movie.get('poster_path')
            img_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None
            return {'title': top_movie['title'], 'image_url': img_url}
    except: pass
    return None

def fetch_apple_music_song(country_code):
    url = f"https://itunes.apple.com/{country_code}/rss/topsongs/limit=1/json"
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        entry = data.get('feed', {}).get('entry')
        target = entry[0] if isinstance(entry, list) else entry
        if target:
            images = target.get('im:image', [])
            img_url = images[-1].get('label') if images else None
            if img_url: img_url = img_url.replace('170x170bb', '600x600bb')
            return {'title': target['im:name']['label'], 'artist': target['im:artist']['label'], 'image_url': img_url}
    except: pass
    return None

def load_historical_csv(filepath):
    """Parses historical CSV files safely, assuming standard column names."""
    history = {}
    if not os.path.exists(filepath):
        return history
        
    with open(filepath, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rank = str(row.get('rank', row.get('position', row.get('Rank', '1'))))
            if rank not in ('1', '01'): continue
            
            date_str = row.get('date', row.get('Date', row.get('week', '')))
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            except:
                continue
                
            title = row.get('song', row.get('title', row.get('Title', 'Unknown')))
            artist = row.get('artist', row.get('singer', row.get('Artist', 'Unknown')))
            
            for i in range(7):
                curr = dt - timedelta(days=i)
                y, m, d = curr.strftime('%Y'), curr.strftime('%m'), curr.strftime('%d')
                day_key = f"{m}-{d}"
                
                history.setdefault(y, {})[day_key] = {
                    'title': title, 
                    'artist': artist, 
                    'image_url': None
                }
    return history

# --- MAIN GENERATOR ---
def process_data():
    database = {}
    today = datetime.utcnow()
    
    # 1. PROCESS US DATA FIRST (Serves as the Global Fallback)
    print("📚 Scanning for historical US/Global data...")
    us_history = load_historical_csv('data/music.csv')
    
    for y, days in us_history.items():
        for day_key, data_obj in days.items():
            database.setdefault('us', {}).setdefault(y, {}).setdefault(day_key, {})
            database['us'][y][day_key]['music'] = data_obj
            database['us'][y][day_key]['global_music'] = data_obj

    # 2. DYNAMICALLY PROCESS ALL OTHER REGIONAL CSVs
    print("📚 Scanning for other regional historical CSVs...")
    for folder in COUNTRIES.keys():
        if folder == 'us': continue # Handled above
        
        # Look for files formatted like data/music_uk.csv, data/music_de.csv, etc.
        csv_path = f'data/music_{folder}.csv'
        if os.path.exists(csv_path):
            print(f"   -> Found data for {folder.upper()}!")
            regional_history = load_historical_csv(csv_path)
            
            for y, days in regional_history.items():
                for day_key, data_obj in days.items():
                    database.setdefault(folder, {}).setdefault(y, {}).setdefault(day_key, {})
                    database[folder][y][day_key]['music'] = data_obj
                    
                    # Always inject the US global fallback if it exists for this day
                    if y in us_history and day_key in us_history[y]:
                        database[folder][y][day_key]['global_music'] = us_history[y][day_key]

    # 3. PRE-FETCH WEEKLY ENTERTAINMENT
    print("🌍 Fetching Live Global Fallbacks with Images...")
    global_music = fetch_apple_music_song('us')
    global_movie = fetch_regional_movie('us')
    
    regional_music_cache = {}
    regional_movie_cache = {}
    
    print(f"🚀 Fetching live music & movies for {len(COUNTRIES)} regions...")
    for folder, code in COUNTRIES.items():
        regional_music_cache[folder] = fetch_apple_music_song(folder)
        regional_movie_cache[folder] = fetch_regional_movie(folder)

    # 4. BUILD THE LAST 14 DAYS (Live 2026 Data)
    print("📅 Generating daily historical fun facts, births, and cosmos...")
    for i in range(14):
        dt = today - timedelta(days=i)
        y, m, d = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')
        day_key = f"{m}-{d}"
        
        global_fact = fetch_historical_fact('en', m, d)
        global_births = fetch_smart_births('us', 'en', m, d) 
        
        for folder, lang_code in COUNTRIES.items():
            database.setdefault(folder, {}).setdefault(y, {}).setdefault(day_key, {})
            day_data = database[folder][y][day_key]
            
            # --- Personal / Cosmic ---
            day_data['symbols'] = get_symbols(m, d, y)
            day_data['moon_phase'] = get_moon_phase(y, m, d)
            
            # --- Regional APIs ---
            reg_fact = fetch_historical_fact(lang_code, m, d)
            reg_births = fetch_smart_births(folder, lang_code, m, d)
            
            if regional_music_cache[folder]: day_data['music'] = regional_music_cache[folder]
            if regional_movie_cache[folder]: day_data['movie'] = regional_movie_cache[folder]
            if reg_fact: day_data['history_fact'] = reg_fact
            if reg_births: day_data['shared_birthdays'] = reg_births
            
            # --- Global Fallbacks ---
            if global_music: day_data['global_music'] = global_music
            if global_movie: day_data['global_movie'] = global_movie
            if global_fact: day_data['global_history_fact'] = global_fact
            if global_births: day_data['global_shared_birthdays'] = global_births

    # 5. EXPORT JSON FILES
    print("📁 Exporting JSON files...")
    for country, years in database.items():
        path = os.path.join(OUTPUT_DIR, country)
        os.makedirs(path, exist_ok=True)
        for year, days in years.items():
            with open(os.path.join(path, f"{year}.json"), 'w', encoding='utf-8') as f:
                json.dump(days, f, indent=2, sort_keys=True)
    
    print("✅ Build Finished. Your dynamic time capsule is ready!")

if __name__ == '__main__':
    process_data()
