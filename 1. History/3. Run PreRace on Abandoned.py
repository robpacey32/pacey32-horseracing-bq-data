# ===============================================================
# 🏇 SCRAPE ABANDONED RACES — PRERACE ONLY
# ---------------------------------------------------------------
# Pulls races where Status contains 'Abandoned' but NOT already
# marked "PreRace Completed -".
#
# Saves prerace data to Scrape_PreRace_TEST
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
          AND NOT STARTS_WITH(Status, 'Prerace Completed')
        ORDER BY Date DESC, Location, Time
        LIMIT 50
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
# 🐎 SCRAPE PRE-RACE — USING YOUR ORIGINAL WORKING LOGIC
# ===============================================================
def scrape_prerace(driver, prerace_url, max_retries=3):
    """Scrape pre-race info and horse-level data from Sporting Life."""
    data = []

    def safe_text(elem, css=None, attr="text", default="N/A"):
        """Safely extract text or attributes."""
        try:
            if css:
                elem = elem.find_element(By.CSS_SELECTOR, css)
            return elem.text.strip() if attr == "text" else elem.get_attribute(attr)
        except Exception:
            return default

    for attempt in range(max_retries):
        try:
            driver.get(prerace_url)
            time.sleep(2)

            # --- Race info ---
            race_name = safe_text(driver, "h1[data-test-id='racecard-race-name']")
            race_date_txt = safe_text(driver, "p[class*='CourseListingHeader__StyledMainSubTitle']")
            try:
                date_obj = datetime.strptime(race_date_txt, "%A %d %B %Y")
                race_date = date_obj.strftime("%d/%m/%Y")
                race_day_of_week = date_obj.strftime("%A")
            except Exception:
                race_date, race_day_of_week = "N/A", "N/A"

            race_location_txt = safe_text(driver, "p[class*='CourseListingHeader__StyledMainTitle']")
            if " " in race_location_txt:
                race_time, race_location = race_location_txt.split(" ", 1)
            else:
                race_time, race_location = "N/A", race_location_txt

            race_class = race_distance = race_going = race_runners = race_surface = winning_time = off_time = "N/A"

            # 🔍 This is your original clean parsing using '|' splits
            for elem in driver.find_elements(
                By.CSS_SELECTOR,
                "li.RacingRacecardSummary__StyledAdditionalInfo-sc-ff7de2c2-3"
            ):
                for part in [p.strip() for p in elem.text.split("|")]:
                    if match := re.search(r"Class\s+(\d+)", part):
                        race_class = match.group(1)
                    elif match := re.search(r"Winning time:\s*([0-9m\s\.]+)", part):
                        winning_time = match.group(1).strip()
                    elif match := re.search(r"Off time:\s*([0-9:]+)", part):
                        off_time = match.group(1)
                    elif re.search(r"\d+\s*(m|f|y)", part):
                        race_distance = part
                    elif any(word in part for word in ["Heavy", "Soft", "Good", "Firm", "Standard", "Yielding", "Fast", "Slow"]):
                        race_going = part
                    elif match := re.search(r"(\d+)\s*Runners?", part):
                        race_runners = match.group(1)
                    elif re.search(r"(Turf|Allweather|All Weather|AW|Polytrack|Fibresand|Tapeta|Dirt)", part, re.I):
                        race_surface = part

            # --- Horse info ---
            horse_containers = driver.find_elements(
                By.CSS_SELECTOR,
                "div[class*='Runner__StyledRunnerContainer']"
            )

            for parent in horse_containers:
                try:
                    horse_number = safe_text(parent, "div[data-test-id='saddle-cloth-no']")
                    stall_no = safe_text(parent, "div[data-test-id='stall-no']", default="N/A").strip("()")
                    horse_name = safe_text(parent, "a[data-test-id='horse-name-link']")
                    headgear = safe_text(parent, "sup[data-test-id='headgear']", default="")
                    last_run = safe_text(parent, "sup[data-test-id='last-ran']", default="")
                    commentary = safe_text(parent, "div[data-test-id='commentary']", default="N/A")

                    sub_info_elem = parent.find_element(By.CSS_SELECTOR, "div[data-test-id='horse-sub-info']")
                    sub_info_text = sub_info_elem.text
                    age = re.search(r"Age: (\d+)", sub_info_text)
                    weight = re.search(r"Weight: ([\d-]+)", sub_info_text)

                    jockey_name = safe_text(
                        sub_info_elem,
                        "a[href*='/jockey/'] span",
                        default="N/A"
                    ).replace("J:", "").strip()

                    trainer_name = safe_text(
                        sub_info_elem,
                        "a[href*='/trainer/'] span",
                        default="N/A"
                    ).replace("T:", "").strip()

                    odds = safe_text(parent, "span[class*='BetLink']")
                    history_stats = " | ".join(
                        [s.text.strip()
                         for s in parent.find_elements(
                             By.CSS_SELECTOR,
                             "span[data-test-id^='race-history-stat-']"
                         )]
                    )

                    data.append({
                        "HorseNumber": horse_number,
                        "StallNumber": stall_no,
                        "HorseName": horse_name,
                        "Headgear": headgear,
                        "LastRun": last_run,
                        "Commentary": commentary,
                        "Age": age.group(1) if age else "",
                        "Weight": weight.group(1) if weight else "",
                        "Jockey": jockey_name,
                        "Trainer": trainer_name,
                        "Odds": odds,
                        "RaceHistoryStats": history_stats,
                        "RaceDate": race_date,
                        "RaceDayOfWeek": race_day_of_week,
                        "RaceLocation": race_location,
                        "RaceName": race_name,
                        "RaceTime": race_time,
                        "RaceClass": race_class,
                        "RaceDistance": race_distance,
                        "RaceGoing": race_going,
                        "RaceRunners": race_runners,
                        "RaceSurface": race_surface,
                        "WinningTime": winning_time,
                        "OffTime": off_time,
                        "SourceURL": prerace_url
                    })
                except Exception:
                    continue

            break  # success → exit retry loop

        except TimeoutException:
            time.sleep(3)

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

    table_id = f"{PROJECT_ID}.{DATASET_ID}.Scrape_PreRace"
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

        prev_status = row["Status"]
        new_status = f"PreRace Completed - {prev_status}"

        status_updates.append({
            "Date": row["Date"],
            "Location": row["Location"],
            "Time": row["Time"],
            "prerace_URL": row["prerace_URL"],
            "postrace_URL": row.get("postrace_URL", ""),
            "Status": new_status
        })

    driver.quit()

    if prerace_all:
        upload_prerace(pd.concat(prerace_all, ignore_index=True))

    append_status_updates(status_updates)

    print("🏁 Finished abandoned-only prerace scrape.")


if __name__ == "__main__":
    main()
