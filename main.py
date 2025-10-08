import os
import time
import csv
import logging
import schedule
import requests
import pandas as pd
import re
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ----------------------------
# CONFIG
# ----------------------------
SCRAPINGDOG_API_KEY = os.environ.get("SCRAPINGDOG_API_KEY")
LINKEDIN_URL = "https://www.linkedin.com/company/extrastaff-recruitment"
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
BUCKET_NAME = "csv-files"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_fetcher.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ----------------------------
# Supabase client
# ----------------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def upload_csv_to_supabase(file_path):
    """
    Upload CSV to Supabase, safely overwriting existing files
    (fixes 409 Duplicate error).
    """
    file_name = os.path.basename(file_path)
    with open(file_path, "rb") as f:
        file_bytes = f.read()
    try:
        # ✅ Use upsert=True to overwrite existing files
        supabase.storage.from_(BUCKET_NAME).upload(file_name, file_bytes, {"upsert": True})
        logging.info(f"✅ Uploaded '{file_name}' to Supabase bucket '{BUCKET_NAME}' (safe overwrite enabled)")
    except Exception as e:
        logging.error(f"❌ Supabase upload failed for {file_name}: {e}")

# ----------------------------
# LinkedIn follower extractor
# ----------------------------
class LinkedInFollowerExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept-Language': 'en-US,en;q=0.9',
        })

    def extract_followers(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        text_content = soup.get_text()
        match = re.search(r'(\d+(?:,\d+)*)\s+followers', text_content, re.IGNORECASE)
        return int(match.group(1).replace(',', '')) if match else None

    def get_followers(self, linkedin_url):
        try:
            response = self.session.get(linkedin_url, timeout=15)
            if response.status_code != 200:
                logging.error(f"HTTP {response.status_code}")
                return None
            if 'login' in response.url.lower():
                logging.error("Blocked by LinkedIn login/authwall")
                return None
            return self.extract_followers(response.text)
        except Exception as e:
            logging.error(f"Error fetching followers: {e}")
            return None

# ----------------------------
# Save follower data
# ----------------------------
def save_follower_data(followers):
    """Append new follower count to CSV and upload to Supabase"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data = {'timestamp': timestamp, 'linkedin_url': LINKEDIN_URL, 'followers': int(followers)}

    file_path = 'linkedin_followers.csv'
    file_exists = os.path.isfile(file_path)

    with open(file_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['timestamp', 'linkedin_url', 'followers'])
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

    logging.info(f"📈 Follower data appended: {followers}")

    # Upload updated CSV to Supabase
    upload_csv_to_supabase(file_path)

def fetch_linkedin_followers():
    extractor = LinkedInFollowerExtractor()
    followers = extractor.get_followers(LINKEDIN_URL)
    if followers:
        save_follower_data(followers)
    else:
        logging.error("Failed to fetch followers")

# ----------------------------
# Fetch LinkedIn posts
# ----------------------------
def fetch_linkedin_posts():
    try:
        url = "https://api.scrapingdog.com/linkedin"
        params = {"api_key": SCRAPINGDOG_API_KEY, "type": "company", "linkId": "extrastaff-recruitment"}
        response = requests.get(url, params=params, timeout=45)
        data = response.json()
        if not data or not isinstance(data, list):
            logging.error("Invalid API response for posts")
            return
        posts = data[0].get('updates', [])
        if posts:
            file_path = "lnkdn.csv"

            # ✅ If file exists, append new posts that are not duplicates
            if os.path.exists(file_path):
                existing = pd.read_csv(file_path)
                new_posts = pd.DataFrame(posts)
                combined = pd.concat([existing, new_posts]).drop_duplicates(subset=['updateUrl'], keep='last')
            else:
                combined = pd.DataFrame(posts)

            combined.to_csv(file_path, index=False)
            logging.info(f"📰 Saved {len(combined)} total posts (with deduplication)")
            upload_csv_to_supabase(file_path)
        else:
            logging.info("No new posts found")
    except Exception as e:
        logging.error(f"Error fetching posts: {e}")

# ----------------------------
# Scheduler
# ----------------------------
schedule.every(5).minutes.do(fetch_linkedin_followers)
schedule.every(4).hours.do(fetch_linkedin_posts)

logging.info("🚀 Starting LinkedIn data pipeline...")

# Initial run
fetch_linkedin_followers()
fetch_linkedin_posts()

while True:
    schedule.run_pending()
    time.sleep(1)
