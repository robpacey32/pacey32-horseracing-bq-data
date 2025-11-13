# ===============================================================
# 🏇 SCRAPE ABANDONED RACES — PRERACE ONLY
# ---------------------------------------------------------------
# Pulls races where Status contains 'Abandoned' but NOT already
# marked "PreRace Completed -".
#
# Saves prerace data to Scrape_PreRace
# Updates RaceSpine status to:
#   PreRace Completed - {previous status}
#
# Author: Rob Pacey
# Last Updated: 2025-11-13
# ===============================================================

import os
import re
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from google.cloud import bigquery
from google.oauth2 import service_account
from webdriver_manager.chrome import ChromeDriverManager


# ===============================================================
# ⚙️ CONFIG
# ===============================================================
PROJECT_ID = "horseracing-pacey32-github"
DATASET_ID = "horseracescrape"
VIEW_NAME = "RaceSpine_Latest"
KEY_PATH = "key.json"


# ===============================================================
# 🔗 LOAD ABANDONED RACES FROM BIGQUERY
# ===============================================================
def load_abandoned_races():
    """Load races with abandoned status still needing prerace scrape."""
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    query = f"""
        SELECT Date, Location, Time, prerace_URL, Status
        FROM `{PROJECT_ID}.{DATASET_ID}.{VIEW_NAME}`
        WHERE LOWER(Status) LIKE '%abandoned%'
          AND NOT STARTS_WITH(Status, 'PreRace Completed')
        ORDER BY Date DESC, Location, Time
        LIMIT 10
    """

    df = client.query(query).to_dataframe()

    if df.empty:
        print("⚠️ No abandoned races needing prerace scrape.")
    else:
        print(f"✅ Loaded {len(df)} abandoned races to process.")
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
def scrape_prerace(driver, prerace_url, max_retries=3):
    data = []

    def safe(elem, css=None):
        try:
            if css:
                elem = elem.find_element(By.CSS_SELECTOR, css)
            return elem.text.strip()
        except:
            return "N/A"

    # -----------------------------------------------------------
    # NEW: Clean "Class | Distance | Runners | Surface" parser
    # -----------------------------------------------------------
    def parse_meta_block(meta_text):
        """Parse the classic 4-piece metadata block cleanly."""
        race_class = race_dist = race_runners = race_surface = "N/A"

        if not meta_text or meta_text == "N/A":
            return race_class, race_dist, race_runners, race_surface

        # Everything will be split by pipe
        parts = [p.strip() for p in meta_text.split("|")]

        if len(parts) >= 1 and "Class" in parts[0]:
            race_class = parts[0].replace("Class", "").strip()

        if len(parts) >= 2:
            race_dist = parts[1]

        if len(parts) >= 3:
            # Extract e.g. "14" from "14 Runners"
            race_runners = re.sub(r"\D", "", parts[2])

        if len(parts) >= 4:
            race_surface = parts[3]

        return race_class, race_dist, race_runners, race_surface

    # ===========================================================
    # MAIN SCRAPE LOOP
    # ===========================================================
    for attempt in range(max_retries):
        try:
            driver.get(prerace_url)
            time.sleep(2)

            # --- Race Info ---
            race_name = safe(driver, "h1[data-test-id='racecard-race-name']")
            race_date_raw = safe(driver, "p[class*='CourseListingHeader__StyledMainSubTitle']")

            try:
                d = datetime.strptime(race_date_raw, "%A %d %B %Y")
                race_date = d.strftime("%d/%m/%Y")
                race_dow = d.strftime("%A")
            except:
                race_date, race_dow = "N/A", "N/A"

            course_line = safe(driver, "p[class*='CourseListingHeader__StyledMainTitle']")
            if " " in course_line:
                race_time, race_location = course_line.split(" ", 1)
            else:
                race_time, race_location = "N/A", course_line

            # ---------------------------------------------------
            # FIXED META BLOCK
            # ---------------------------------------------------
            try:
                meta_elem = driver.find_element(By.CSS_SELECTOR, "ul.RacingRacecardSummary__StyledAdditionalInfoList")
                meta_text = meta_elem.text.replace("\n", " | ")
            except:
                meta_text = "N/A"

            r_class, r_dist, r_runners, r_surface = parse_meta_block(meta_text)

            # ---------------------------------------------------
            # WINNING + OFF TIME (unchanged)
            # ---------------------------------------------------
            win_time = "N/A"
            off_time = "N/A"

            rows = driver.find_elements(By.CSS_SELECTOR, "li.RacingRacecardSummary__StyledAdditionalInfo-sc-ff7de2c2-3")
            for elem in rows:
                txt = elem.text

                if m := re.search(r"Winning time:\s*(.*)", txt):
                    win_time = m.group(1).strip()

                if m := re.search(r"Off time:\s*([0-9:]+)", txt):
                    off_time = m.group(1)

            # ---------------------------------------------------
            # Horse Rows
            # ---------------------------------------------------
            horses = driver.find_elements(By.CSS_SELECTOR, "div[class*='Runner__StyledRunnerContainer']")
            for h in horses:
                data.append({
                    "HorseNumber": safe(h, "div[data-test-id='saddle-cloth-no']"),
                    "StallNumber": safe(h, "div[data-test-id='stall-no']").strip("()"),
                    "HorseName": safe(h, "a[data-test-id='horse-name-link']"),
                    "Headgear": safe(h, "sup[data-test-id='headgear']"),
                    "LastRun": safe(h, "sup[data-test-id='last-ran']"),
                    "Commentary": safe(h, "div[data-test-id='commentary']"),
                    "RaceDate": race_date,
                    "RaceDayOfWeek": race_dow,
                    "RaceLocation": race_location,
                    "RaceName": race_name,
                    "RaceTime": race_time,
                    "RaceClass": r_class,
                    "RaceDistance": r_dist,
                    "RaceRunners": r_runners,
                    "RaceSurface": r_surface,
                    "RaceGoing": "N/A",   # Not always available in abandoned
                    "WinningTime": win_time,
                    "OffTime": off_time,
                    "SourceURL": prerace_url
                })
            break

        except TimeoutException:
            time.sleep(2)

    return pd.DataFrame(data)


# ===============================================================
# ☁️ UPLOAD TO BIGQUERY
# ===============================================================
def upload_prerace(df):
    if df.empty:
        print("⚠️ No prerace scraped data to upload.")
        return

    df["load_timestamp"] = datetime.utcnow()

    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.Scrape_PreRace_TEST"
    config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    client.load_table_from_dataframe(df, table_id, config).result()

    print(f"✅ Uploaded {len(df)} prerace rows → {table_id}")


# ===============================================================
# 🧩 APPEND STATUS UPDATES
# ===============================================================
def append_status_updates(rows):
    if not rows:
        print("⚠️ No status updates.")
        return

    df = pd.DataFrame(rows)
    df["load_timestamp"] = datetime.utcnow()

    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.RaceSpine"
    config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    client.load_table_from_dataframe(df, table_id, config).result()

    print(f"🔄 Updated {len(df)} race statuses.")


# ===============================================================
# 🚀 MAIN
# ===============================================================
def main():
    df = load_abandoned_races()
    if df.empty:
        return

    driver = setup_driver()
    prerace_all = []
    status_updates = []

    for i, row in df.iterrows():
        print(f"[{i+1}/{len(df)}] Scraping {row.Location} {row.Time}...")

        pre_df = scrape_prerace(driver, row.prerace_URL)
        if not pre_df.empty:
            prerace_all.append(pre_df)

        prev = row["Status"]
        new_status = f"PreRace Completed - {prev}"

        status_updates.append({
            "Date": row["Date"],
            "Location": row["Location"],
            "Time": row["Time"],
            "Status": new_status
        })

    driver.quit()

    if prerace_all:
        upload_prerace(pd.concat(prerace_all, ignore_index=True))

    append_status_updates(status_updates)

    print("🏁 Finished abandoned-only prerace scrape.")


if __name__ == "__main__":
    main()
