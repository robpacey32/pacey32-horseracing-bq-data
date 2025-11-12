# ===============================================================
# 🏇 SPORTING LIFE SCRAPER — FROM RACE SPINE
# ---------------------------------------------------------------
# Purpose:
#   Pull latest pending races from BigQuery RaceSpine_Latest,
#   scrape both pre- and post-race data if URLs exist,
#   upload results to BigQuery tables,
#   and append new statuses (with load_timestamp) to RaceSpine.
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
MAX_RACES = int(os.getenv("MAX_RACES", "100"))

# ===============================================================
# 🔗 LOAD RACES FROM BIGQUERY
# ===============================================================
def load_pending_races():
    """Pull latest pending races from RaceSpine_Latest view."""
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    today_str = datetime.today().strftime("%Y-%m-%d")
    query = f"""
        SELECT Date, Location, Time, prerace_URL, postrace_URL, Status
        FROM `{PROJECT_ID}.{DATASET_ID}.{VIEW_NAME}`
        WHERE Status = 'Pending'
        ORDER BY Location, Time
        LIMIT {MAX_RACES}
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
    """Configure and start headless Chrome driver."""
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
    """Scrape pre-race info (horse names + odds)."""
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
    """Scrape post-race results (positions, horses, SP)."""
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
def upload_to_bigquery(df, table_suffix):
    """Upload scraped results to BigQuery table."""
    if df.empty:
        print(f"⚠️ No {table_suffix} data to upload — skipping.")
        return

    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.Scrape_{table_suffix}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"✅ Uploaded {len(df)} rows to {table_id}")

# ===============================================================
# 🧩 APPEND STATUS UPDATES TO RACESPLINE
# ===============================================================
def append_status_updates(status_rows):
    """Append new status rows to RaceSpine with updated load_timestamp."""
    if not status_rows:
        print("⚠️ No status updates to append.")
        return

    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    df_status = pd.DataFrame(status_rows)
    df_status["load_timestamp"] = datetime.utcnow()

    table_id = f"{PROJECT_ID}.{DATASET_ID}.RaceSpine"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)

    client.load_table_from_dataframe(df_status, table_id, job_config=job_config).result()
    print(f"✅ Appended {len(df_status)} new status rows to {table_id}")

# ===============================================================
# 🚀 MAIN EXECUTION
# ===============================================================
def main():
    df_races = load_pending_races()
    if df_races.empty:
        return

    driver = setup_driver()
    prerace_data = []
    postrace_data = []
    updated_rows = []

    for i, row in df_races.iterrows():
        prerace_url = row["prerace_URL"]
        postrace_url = row["postrace_URL"]
        location = row["Location"]
        time_ = row["Time"]
        date_ = row["Date"]
        status = "Pending"

        print(f"[{i+1}/{len(df_races)}] Scraping {location} {time_}...")

        prerace_df = pd.DataFrame()
        postrace_df = pd.DataFrame()

        # --- Run pre-race ---
        if pd.notna(prerace_url) and prerace_url.strip():
            prerace_df = scrape_prerace(driver, prerace_url)
            if not prerace_df.empty:
                prerace_df["Date"] = date_
                prerace_df["RaceTime"] = time_
                prerace_data.append(prerace_df)

        # --- Run post-race ---
        if pd.notna(postrace_url) and postrace_url.strip():
            postrace_df = scrape_results(driver, postrace_url)
            if not postrace_df.empty:
                postrace_df["Date"] = date_
                postrace_df["RaceTime"] = time_
                postrace_data.append(postrace_df)

        # --- Determine status ---
        if not prerace_df.empty and not postrace_df.empty:
            status = "Complete"
        elif not prerace_df.empty or not postrace_df.empty:
            status = "In Progress"

        updated_rows.append({
            "Date": date_,
            "Location": location,
            "Time": time_,
            "Status": status
        })

        print(f"   → {status}")

    driver.quit()

    # --- Upload data ---
    if prerace_data:
        upload_to_bigquery(pd.concat(prerace_data, ignore_index=True), "PreRace")
    if postrace_data:
        upload_to_bigquery(pd.concat(postrace_data, ignore_index=True), "PostRace")

    # --- Append status snapshot ---
    append_status_updates(updated_rows)

    # --- Summary ---
    print(f"\n🏁 Finished scraping {len(df_races)} races.")
    print(f"✅ {sum(1 for r in updated_rows if r['Status']=='Complete')} complete")
    print(f"🕓 {sum(1 for r in updated_rows if r['Status']=='In Progress')} in progress")

if __name__ == "__main__":
    main()
