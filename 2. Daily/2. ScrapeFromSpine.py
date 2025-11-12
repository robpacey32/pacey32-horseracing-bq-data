# ===============================================================
# 🏇 SPORTING LIFE SCRAPER — FROM RACE SPINE
# ---------------------------------------------------------------
# Purpose:
#   Pull the latest pending races from BigQuery RaceSpine_Latest
#   and run Selenium scraper (pre/post race).
#
#   Saves scraped data directly to BigQuery.
#
# Author: Rob Pacey
# Last Updated: 2025-11-12
# ===============================================================

import os
import re
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from google.cloud import bigquery
from google.oauth2 import service_account

# ===============================================================
# ⚙️ CONFIGURATION
# ===============================================================
PROJECT_ID = "horseracing-pacey32-github"
DATASET_ID = "horseracescrape"
VIEW_NAME = "RaceSpine_Latest"
KEY_PATH = "key.json"

SCRAPE_MODE = os.getenv("SCRAPE_MODE", "prerace")  # prerace or results
MAX_RACES = int(os.getenv("MAX_RACES", "100"))

# ===============================================================
# 🔗 LOAD RACES FROM BIGQUERY
# ===============================================================
def load_pending_races():
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    today_str = datetime.today().strftime("%Y-%m-%d")
    query = f"""
        SELECT Date, Location, Time, prerace_URL, postrace_URL, Status
        FROM `{PROJECT_ID}.{DATASET_ID}.{VIEW_NAME}`
        WHERE Status = 'Pending'
          AND prerace_URL IS NOT NULL
        ORDER BY Location, Time
        LIMIT 10
    """
    df = client.query(query).to_dataframe()

    if df.empty:
        print(f"⚠️ No pending races found for {today_str}")
    else:
        print(f"✅ Loaded {len(df)} pending races for {today_str}")

    return df

# ===============================================================
# 🧩 SETUP SELENIUM
# ===============================================================
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

# ===============================================================
# 🐎 SCRAPE PRE-RACE
# ===============================================================
def scrape_prerace(driver, url):
    data = []
    driver.get(url)
    time.sleep(2)
    try:
        race_name = driver.find_element(By.CSS_SELECTOR, "h1[data-test-id='racecard-race-name']").text.strip()
        race_location = driver.find_element(By.CSS_SELECTOR, "p[class*='StyledMainTitle']").text.strip()
    except NoSuchElementException:
        return pd.DataFrame()

    horses = driver.find_elements(By.CSS_SELECTOR, "div[class*='Runner__StyledRunnerContainer']")
    for h in horses:
        try:
            horse_name = h.find_element(By.CSS_SELECTOR, "a[data-test-id='horse-name-link']").text
            odds = h.find_element(By.CSS_SELECTOR, "span[class*='BetLink']").text
            data.append({
                "RaceName": race_name,
                "RaceLocation": race_location,
                "HorseName": horse_name,
                "Odds": odds,
                "ScrapeTimestamp": datetime.utcnow(),
                "SourceURL": url
            })
        except Exception:
            continue
    return pd.DataFrame(data)

# ===============================================================
# 🏁 SCRAPE POST-RACE
# ===============================================================
def scrape_results(driver, url):
    data = []
    driver.get(url)
    time.sleep(2)
    try:
        race_name = driver.find_element(By.CSS_SELECTOR, "h1[data-test-id='racecard-race-name']").text.strip()
        race_location = driver.find_element(By.CSS_SELECTOR, "p[class*='StyledMainTitle']").text.strip()
    except NoSuchElementException:
        return pd.DataFrame()

    runners = driver.find_elements(By.CSS_SELECTOR, "div[class*='ResultRunner__StyledResultRunnerWrapper']")
    for r in runners:
        try:
            pos = r.find_element(By.CSS_SELECTOR, "div[data-test-id='position-no']").text
            horse = r.find_element(By.CSS_SELECTOR, "div[class*='StyledHorseName']").text
            sp = r.find_element(By.CSS_SELECTOR, "span[class*='BetLink']").text
            data.append({
                "RaceName": race_name,
                "RaceLocation": race_location,
                "Pos": pos,
                "HorseName": horse,
                "SP": sp,
                "ScrapeTimestamp": datetime.utcnow(),
                "SourceURL": url
            })
        except Exception:
            continue
    return pd.DataFrame(data)

# ===============================================================
# ☁️ UPLOAD TO BIGQUERY
# ===============================================================
def upload_to_bigquery(df, mode):
    """Upload scraped results to BigQuery table."""
    if df.empty:
        print("⚠️ No data to upload — skipping.")
        return

    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    table_name = "Scrape_PreRace" if mode == "prerace" else "Scrape_PostRace"
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"✅ Uploaded {len(df)} rows to {table_id}")

# ===============================================================
# 🚀 MAIN EXECUTION
# ===============================================================
def main():
    df_races = load_pending_races()
    if df_races.empty:
        return

    driver = setup_driver()
    all_data = []

    for i, row in df_races.head(MAX_RACES).iterrows():
        url = row["postrace_URL"] if SCRAPE_MODE == "results" else row["prerace_URL"]
        if not url or pd.isna(url):
            continue

        print(f"[{i+1}/{len(df_races)}] Scraping {row['Location']} {row['Time']} ({SCRAPE_MODE})...")

        df_scraped = scrape_results(driver, url) if SCRAPE_MODE == "results" else scrape_prerace(driver, url)
        if not df_scraped.empty:
            df_scraped["Date"] = row["Date"]
            df_scraped["RaceTime"] = row["Time"]
            all_data.append(df_scraped)

    driver.quit()

    if all_data:
        df_all = pd.concat(all_data, ignore_index=True)
        upload_to_bigquery(df_all, SCRAPE_MODE)
    else:
        print("⚠️ No races scraped.")

if __name__ == "__main__":
    main()
