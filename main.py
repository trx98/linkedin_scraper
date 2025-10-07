import os
import requests
import csv
import re
import json
import logging
from datetime import datetime
from bs4 import BeautifulSoup

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

# Initialize Supabase client (will be created only if needed)
supabase = None

def get_supabase_client():
    """Lazy initialization of Supabase client to avoid import issues"""
    global supabase
    if supabase is None:
        try:
            from supabase import create_client
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            logging.info("✅ Supabase client initialized")
        except Exception as e:
            logging.error(f"❌ Failed to initialize Supabase client: {e}")
            return None
    return supabase

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
                logging.error(f"HTTP {r.status_code} while fetching {url}")
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
    
    logging.info(f"✅ Saved followers: {followers}")
    return followers

def save_posts_data(posts):
    if not posts:
        logging.warning("No posts to save")
        return 0
        
    # Get all unique fieldnames from all posts
    fieldnames = set()
    for post in posts:
        fieldnames.update(post.keys())
    fieldnames = list(fieldnames)
    
    # Save posts to CSV
    with open(POSTS_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for post in posts:
            # Fill in missing fields with empty values
            row = {field: post.get(field, '') for field in fieldnames}
            writer.writerow(row)
    
    logging.info(f"✅ Saved {len(posts)} posts to CSV")
    return len(posts)

def fetch_linkedin_followers():
    logging.info("🔍 Fetching LinkedIn followers...")
    extractor = LinkedInFollowerExtractor()
    followers = extractor.get_followers(LINKEDIN_URL)
    if followers:
        return save_follower_data(followers)
    else:
        logging.error("❌ Failed to get follower count")
        return None

def fetch_linkedin_posts():
    logging.info("📝 Fetching LinkedIn posts...")
    try:
        url = "https://api.scrapingdog.com/linkedin"
        params = {
            "api_key": SCRAPINGDOG_API_KEY,
            "type": "company",
            "linkId": "extrastaff-recruitment"
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        if not data or not isinstance(data, list):
            logging.error("❌ Invalid API response format")
            return None
            
        posts = data[0].get("updates", [])
        if not posts:
            logging.warning("⚠️ No posts found in response")
            return 0
            
        return save_posts_data(posts)
        
    except Exception as e:
        logging.error(f"❌ Post fetch error: {e}")
        return None

def upload_csv_to_supabase(file_path):
    client = get_supabase_client()
    if client is None:
        logging.error("❌ Cannot upload - Supabase client not available")
        return False
        
    file_name = os.path.basename(file_path)
    try:
        with open(file_path, "rb") as f:
            file_bytes = f.read()
        
        # Try to remove existing file first
        try:
            client.storage.from_(BUCKET_NAME).remove([file_name])
            logging.info(f"🗑️  Removed existing {file_name} from Supabase")
        except Exception as e:
            logging.info(f"ℹ️  No existing file to remove: {file_name}")
        
        # Upload new file
        client.storage.from_(BUCKET_NAME).upload(file_name, file_bytes)
        logging.info(f"📤 Uploaded {file_name} to Supabase")
        return True
        
    except Exception as e:
        logging.error(f"❌ Upload failed for {file_name}: {e}")
        return False

def upload_all_csvs():
    logging.info("☁️ Uploading files to Supabase...")
    results = {}
    
    if os.path.exists(FOLLOWERS_CSV):
        results['followers'] = upload_csv_to_supabase(FOLLOWERS_CSV)
    else:
        logging.warning("⚠️ Followers CSV not found")
        
    if os.path.exists(POSTS_CSV):
        results['posts'] = upload_csv_to_supabase(POSTS_CSV)
    else:
        logging.warning("⚠️ Posts CSV not found")
        
    return results

def main():
    logging.info("🚀 Starting LinkedIn scraper...")
    
    try:
        # Test Supabase connection first
        client = get_supabase_client()
        if client is None:
            logging.warning("⚠️ Supabase not available, will only save local CSV files")
        
        # Run all scraping functions
        followers = fetch_linkedin_followers()
        posts = fetch_linkedin_posts()
        
        # Only try to upload if Supabase is available
        if client is not None:
            upload_results = upload_all_csvs()
        else:
            upload_results = {"status": "supabase_unavailable"}
        
        # Log summary
        logging.info("=" * 50)
        logging.info("📊 SCRAPING SUMMARY:")
        logging.info(f"   👥 Followers: {followers}")
        logging.info(f"   📄 Posts: {posts}")
        logging.info(f"   📤 Upload Results: {upload_results}")
        logging.info("✅ Scraping completed successfully!")
        logging.info("=" * 50)
        
        return {
            "status": "success",
            "followers": followers,
            "posts": posts,
            "upload_results": upload_results
        }
        
    except Exception as e:
        logging.error(f"💥 Scraping failed: {e}")
        return {
            "status": "error",
            "error": str(e)
        }

if __name__ == "__main__":
    main()
