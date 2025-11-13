# ===============================================================
# 🏇 SPORTING LIFE SCRAPER — FROM RACE SPINE (FULL VERSION)
# ---------------------------------------------------------------
# Runs both pre- and post-race scrapes for all "Pending" races
# from RaceSpine_Latest view in BigQuery.
#
# Uploads pre/post results to BigQuery tables and appends updated
# race statuses to RaceSpine with a new load_timestamp.
#
# Author: Rob Pacey
# Last Updated: 2025-11-12
# ===============================================================

import os
import re
import time
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
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
MAX_RACES = int(os.getenv("MAX_RACES", "500"))

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
        ORDER BY date desc, Location, Time
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
# 🐎 SCRAPE PRE-RACE — FULL DETAIL
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
        except:
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
            except:
                race_date, race_day_of_week = "N/A", "N/A"

            race_location_txt = safe_text(driver, "p[class*='CourseListingHeader__StyledMainTitle']")
            race_time, race_location = (
                race_location_txt.split(" ", 1) if " " in race_location_txt else ("N/A", race_location_txt)
            )

            race_class = race_distance = race_going = race_runners = race_surface = winning_time = off_time = "N/A"

            for elem in driver.find_elements(By.CSS_SELECTOR, "li.RacingRacecardSummary__StyledAdditionalInfo-sc-ff7de2c2-3"):
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
            horse_containers = driver.find_elements(By.CSS_SELECTOR, "div[class*='Runner__StyledRunnerContainer']")
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

                    jockey_name = safe_text(sub_info_elem, "a[href*='/jockey/'] span", default="N/A").replace("J:", "").strip()
                    trainer_name = safe_text(sub_info_elem, "a[href*='/trainer/'] span", default="N/A").replace("T:", "").strip()

                    odds = safe_text(parent, "span[class*='BetLink']")
                    history_stats = " | ".join(
                        [s.text.strip() for s in parent.find_elements(By.CSS_SELECTOR, "span[data-test-id^='race-history-stat-']")]
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
            break
        except TimeoutException:
            time.sleep(3)
    return pd.DataFrame(data)

# ===============================================================
# 🏁 FULL POST-RACE SCRAPER (MATCHES LOCAL VERSION)
# ===============================================================
def scrape_results(driver, result_url, max_retries=3):
    data = []

    def safe_text(elem, css=None, attr=None, default="N/A"):
        try:
            if css:
                elem = elem.find_element(By.CSS_SELECTOR, css)
            return elem.get_attribute(attr) if attr else elem.text.strip()
        except:
            return default

    for attempt in range(max_retries):
        try:
            driver.get(result_url)
            time.sleep(2)

            # ---------------------------
            # RACE INFO
            # ---------------------------
            try:
                race_name = safe_text(driver, "h1[data-test-id='racecard-race-name']")
                race_date_text = safe_text(driver, "p[class*='CourseListingHeader__StyledMainSubTitle']")

                try:
                    race_date_obj = datetime.strptime(race_date_text, "%A %d %B %Y")
                    race_date = race_date_obj.strftime("%d/%m/%Y")
                    race_day_of_week = race_date_obj.strftime("%A")
                except:
                    race_date = race_day_of_week = "N/A"

                race_time_text = safe_text(driver, "p[class*='CourseListingHeader__StyledMainTitle']")
                parts = race_time_text.split()
                race_time = parts[0] if parts else "N/A"
                race_location = " ".join(parts[1:]) if len(parts) > 1 else "N/A"

                # Defaults
                winning_time = race_distance = race_going = race_runners = race_surface = "N/A"

                for li in driver.find_elements(
                    By.CSS_SELECTOR,
                    "li.RacingRacecardSummary__StyledAdditionalInfo-sc-ff7de2c2-3"
                ):
                    t = li.text.strip()
                    if m := re.search(r"Winning time:\s*([0-9m\s\.]+)", t):
                        winning_time = m.group(1).strip()
                    if m := re.search(r"(\d+)\s*Runners?", t):
                        race_runners = m.group(1)
                    if m := re.search(r"(\d+\s*(?:m|f|y))", t):
                        race_distance = m.group(1)
                    if m := re.search(r"(Heavy|Soft|Good|Firm|Standard|Slow)", t):
                        race_going = m.group(1)
                    if m := re.search(r"(Turf|Polytrack|Tapeta|AW|Dirt)", t, re.I):
                        race_surface = m.group(1)

            except:
                race_name = race_date = race_day_of_week = race_location = race_time = \
                race_distance = race_going = race_runners = race_surface = winning_time = "N/A"

            # ----------------------------------------
            # PRIZE MONEY PANEL (extract once per race)
            # ----------------------------------------
            prize_dict = {}
            try:
                prize_rows = driver.find_elements(
                    By.CSS_SELECTOR,
                    "#prizemoney span.PrizeMoney__Prize-sc-1dca786a-0"
                )
                for row in prize_rows:
                    label = row.find_element(
                        By.CSS_SELECTOR,
                        "span.PrizeMoney__PrizeLabel-sc-1dca786a-1"
                    ).text.strip().replace(":", "")
                    amount = row.find_element(
                        By.CSS_SELECTOR,
                        "span.PrizeMoney__PrizeNumber-sc-1dca786a-2"
                    ).text.strip()
                    prize_dict[label] = amount
            except:
                prize_dict = {}

            # ---------------------------
            # HORSE-LEVEL DATA
            # ---------------------------
            horse_elements = driver.find_elements(
                By.CSS_SELECTOR,
                "div[class*='ResultRunner__StyledResultRunnerWrapper']"
            )

            for horse_elem in horse_elements:
                try:
                    pos = safe_text(horse_elem, "div[data-test-id='position-no']")
                    silk_url = safe_text(horse_elem, "div[class*='StyledSilkContainer'] img", attr="src")
                    horse_number = safe_text(horse_elem, "div[data-test-id='saddle-cloth-no']")
                    stall_number = safe_text(horse_elem, "div[data-test-id='stall-no']")
                    horse_name = safe_text(horse_elem, "div[class*='StyledHorseName'] a")
                    ride_desc = safe_text(horse_elem, "div[data-test-id='ride-description']", default="N/A")

                    # Trainer & jockey
                    trainer = jockey = "N/A"
                    for span in horse_elem.find_elements(By.CSS_SELECTOR, "span[class*='StyledPersonName']"):
                        try:
                            label = span.find_element(By.XPATH, "./..").text
                            if label.startswith("T:"):
                                trainer = span.text.strip()
                            elif label.startswith("J:"):
                                jockey = span.text.strip()
                        except:
                            continue

                    sp = safe_text(horse_elem, "span[class*='BetLink']")

                    # --------------------------------------------
                    # FIXED: Assign prize money from prize_dict
                    # --------------------------------------------
                    prize_money = prize_dict.get(pos, "0")

                    data.append({
                        "Pos": pos,
                        "SilkURL": silk_url,
                        "HorseNumber": horse_number,
                        "StallNumber": stall_number,
                        "HorseName": horse_name,
                        "Result": pos,
                        "SP": sp,
                        "Trainer": trainer,
                        "Jockey": jockey,
                        "PrizeMoney": prize_money,
                        "RideDescription": ride_desc,
                        "RaceDate": race_date,
                        "RaceDayOfWeek": race_day_of_week,
                        "RaceLocation": race_location,
                        "RaceName": race_name,
                        "RaceTime": race_time,
                        "WinningTime": winning_time,
                        "RaceDistance": race_distance,
                        "RaceGoing": race_going,
                        "RaceRunners": race_runners,
                        "RaceSurface": race_surface,
                        "SourceURL": result_url
                    })

                except Exception as e:
                    print("Horse parse error:", e)

            break

        except TimeoutException:
            time.sleep(3)

    return pd.DataFrame(data)

# ===============================================================
# ☁️ UPLOAD TO BIGQUERY
# ===============================================================
def upload_to_bigquery(df, table_suffix):
    if df.empty:
        print(f"⚠️ No {table_suffix} data to upload — skipping.")
        return

    # Add timestamp
    df["load_timestamp"] = datetime.utcnow()
    
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.Scrape_{table_suffix}"
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print(f"✅ Uploaded {len(df)} rows to {table_id}")

# ===============================================================
# 🧩 APPEND STATUS UPDATES TO RACESPLINE
# ===============================================================
def append_status_updates(status_rows):
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
    prerace_data, postrace_data, updated_rows = [], [], []

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

        if pd.notna(prerace_url) and prerace_url.strip():
            prerace_df = scrape_prerace(driver, prerace_url)
            if not prerace_df.empty:
                prerace_data.append(prerace_df)

        if pd.notna(postrace_url) and postrace_url.strip():
            postrace_df = scrape_results(driver, postrace_url)
            if not postrace_df.empty:
                postrace_data.append(postrace_df)

        if not prerace_df.empty and not postrace_df.empty:
            status = "Complete"
        elif not prerace_df.empty or not postrace_df.empty:
            status = "In Progress"

        updated_rows.append({
            "Date": row["Date"],
            "Location": row["Location"],
            "Time": row["Time"],
            "prerace_URL": prerace_URL,
            "postrace_URL": postrace_url,
            "Status": status
        })
        print(f"   → {status}")

    driver.quit()

    if prerace_data:
        upload_to_bigquery(pd.concat(prerace_data, ignore_index=True), "PreRace")
    if postrace_data:
        upload_to_bigquery(pd.concat(postrace_data, ignore_index=True), "PostRace")

    append_status_updates(updated_rows)

    print(f"\n🏁 Finished scraping {len(df_races)} races.")
    print(f"✅ {sum(1 for r in updated_rows if r['Status']=='Complete')} complete")
    print(f"🕓 {sum(1 for r in updated_rows if r['Status']=='In Progress')} in progress")

if __name__ == "__main__":
    main()
