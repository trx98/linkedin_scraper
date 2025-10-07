import os
import pandas as pd
import requests
import csv
import re
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client, Client

# Configuration
LINKEDIN_URL = "https://www.linkedin.com/company/extrastaff-recruitment"
SCRAPINGDOG_API_KEY = os.environ.get("SCRAPINGDOG_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
BUCKET_NAME = "csv-files"

FOLLOWERS_CSV = "linkedin_followers.csv"
POSTS_CSV = "linkedin_posts.csv"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

class LinkedInFollowerExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        })

    def extract_followers(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        patterns = [
            r'(\d+(?:,\d+)*)\s+followers',
            r'followerCount["\']?\s*:\s*["\']?(\d+(?:,\d+)*)'
        ]
        for p in patterns:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                return int(m.group(1).replace(',', ''))
        return None

    def get_followers(self, url):
        try:
            r = self.session.get(url, timeout=15)
            if r.status_code != 200:
                return None
            return self.extract_followers(r.text)
        except Exception as e:
            logging.error(f"Follower fetch error: {e}")
            return None

def save_follower_data(followers):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    row = {'timestamp': ts, 'linkedin_url': LINKEDIN_URL, 'followers': followers}
    
    exists = os.path.isfile(FOLLOWERS_CSV)
    with open(FOLLOWERS_CSV, 'a', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if not exists:
            w.writeheader()
        w.writerow(row)
    
    logging.info(f"Saved followers: {followers}")
    return followers

def fetch_linkedin_followers():
    logging.info("Fetching LinkedIn followers...")
    extractor = LinkedInFollowerExtractor()
    followers = extractor.get_followers(LINKEDIN_URL)
    if followers:
        return save_follower_data(followers)
    return None

def fetch_linkedin_posts():
    logging.info("Fetching LinkedIn posts...")
    try:
        url = "https://api.scrapingdog.com/linkedin"
        params = {
            "api_key": SCRAPINGDOG_API_KEY,
            "type": "company",
            "linkId": "extrastaff-recruitment"
        }
        r = requests.get(url, params=params, timeout=30)
        data = r.json()
        if not data or not isinstance(data, list):
            return None
        posts = data[0].get("updates", [])
        df = pd.DataFrame(posts)
        df.to_csv(POSTS_CSV, index=False)
        logging.info(f"Saved {len(posts)} posts")
        return len(posts)
    except Exception as e:
        logging.error(f"Post fetch error: {e}")
        return None

def upload_csv_to_supabase(file_path):
    file_name = os.path.basename(file_path)
    try:
        with open(file_path, "rb") as f:
            file_bytes = f.read()
        try:
            supabase.storage.from_(BUCKET_NAME).remove([file_name])
        except:
            pass
        supabase.storage.from_(BUCKET_NAME).upload(file_name, file_bytes)
        logging.info(f"Uploaded {file_name} to Supabase")
        return True
    except Exception as e:
        logging.error(f"Upload failed: {e}")
        return False

def upload_all_csvs():
    logging.info("Uploading files to Supabase...")
    results = {}
    if os.path.exists(FOLLOWERS_CSV):
        results['followers'] = upload_csv_to_supabase(FOLLOWERS_CSV)
    if os.path.exists(POSTS_CSV):
        results['posts'] = upload_csv_to_supabase(POSTS_CSV)
    return results

def main():
    logging.info("🚀 Starting LinkedIn scraper...")
    followers = fetch_linkedin_followers()
    posts = fetch_linkedin_posts()
    upload_results = upload_all_csvs()
    logging.info("✅ Scraping completed!")
    return {"followers": followers, "posts": posts, "upload": upload_results}

if __name__ == "__main__":
    main()
