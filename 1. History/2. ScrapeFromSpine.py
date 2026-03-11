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
# Last Updated: 2026-03-11
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

    query = f"""
        SELECT Date, Location, Time, prerace_URL, postrace_URL, Status
        FROM `{PROJECT_ID}.{DATASET_ID}.{VIEW_NAME}`
        WHERE Status = 'Pending'
        ORDER BY Date DESC, Location, Time
        LIMIT {MAX_RACES}
    """

    df = client.query(query).to_dataframe(create_bqstorage_client=False)

    if df.empty:
        print("⚠️ No pending races found.")
    else:
        print(f"✅ Loaded {len(df)} pending races.")

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
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


# ===============================================================
# 🧰 SELENIUM HELPERS
# ===============================================================
def first_text(driver_or_elem, selectors, default="N/A"):
    """Try multiple CSS selectors and return the first matching text."""
    for selector in selectors:
        try:
            elem = driver_or_elem.find_element(By.CSS_SELECTOR, selector)
            text = elem.text.strip()
            if text:
                return text
        except Exception:
            continue
    return default


def first_attr(driver_or_elem, selectors, attr, default="N/A"):
    """Try multiple CSS selectors and return the first matching attribute."""
    for selector in selectors:
        try:
            elem = driver_or_elem.find_element(By.CSS_SELECTOR, selector)
            value = elem.get_attribute(attr)
            if value:
                return value.strip()
        except Exception:
            continue
    return default


def all_elements(driver_or_elem, selectors):
    """Return the first non-empty list of elements from a list of selectors."""
    for selector in selectors:
        try:
            elems = driver_or_elem.find_elements(By.CSS_SELECTOR, selector)
            if elems:
                return elems
        except Exception:
            continue
    return []


# ===============================================================
# 🐎 SCRAPE PRE-RACE
# ===============================================================
def scrape_prerace(driver, prerace_url, max_retries=3):
    """Scrape pre-race info and horse-level data from Sporting Life."""
    data = []

    for attempt in range(max_retries):
        try:
            driver.get(prerace_url)
            time.sleep(3)

            # --- Race info ---
            race_name = first_text(driver, [
                "h1[data-testid='racecard-race-name']",
                "h1[data-test-id='racecard-race-name']",
                "h1"
            ])

            race_date_txt = first_text(driver, [
                "p[class*='CourseListingHeader__StyledMainSubTitle']"
            ])

            try:
                date_obj = datetime.strptime(race_date_txt, "%A %d %B %Y")
                race_date = date_obj.strftime("%d/%m/%Y")
                race_day_of_week = date_obj.strftime("%A")
            except Exception:
                race_date, race_day_of_week = "N/A", "N/A"

            race_location_txt = first_text(driver, [
                "p[class*='CourseListingHeader__StyledMainTitle']"
            ])

            if " " in race_location_txt:
                race_time, race_location = race_location_txt.split(" ", 1)
            else:
                race_time, race_location = "N/A", race_location_txt

            race_class = "N/A"
            race_distance = "N/A"
            race_going = "N/A"
            race_runners = "N/A"
            race_surface = "N/A"
            winning_time = "N/A"
            off_time = "N/A"

            summary_items = all_elements(driver, [
                "li.RacingRacecardSummary__StyledAdditionalInfo-sc-ff7de2c2-3",
                "li[class*='RacingRacecardSummary__StyledAdditionalInfo']",
                "li[class*='AdditionalInfo']"
            ])

            for elem in summary_items:
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
            horse_containers = all_elements(driver, [
                "div[class*='Runner__StyledRunnerContainer']",
                "div[class*='RunnerContainer']"
            ])

            print(f"   Pre-race horse containers found: {len(horse_containers)}")

            for parent in horse_containers:
                try:
                    horse_number = first_text(parent, [
                        "div[data-testid='saddle-cloth-no']",
                        "div[data-test-id='saddle-cloth-no']"
                    ])

                    stall_no = first_text(parent, [
                        "div[data-testid='stall-no']",
                        "div[data-test-id='stall-no']"
                    ], default="N/A").strip("()")

                    horse_name = first_text(parent, [
                        "a[data-testid='horse-name-link']",
                        "a[data-test-id='horse-name-link']",
                        "a[href*='/horse/']"
                    ])

                    headgear = first_text(parent, [
                        "sup[data-testid='headgear']",
                        "sup[data-test-id='headgear']"
                    ], default="")

                    last_run = first_text(parent, [
                        "sup[data-testid='last-ran']",
                        "sup[data-test-id='last-ran']"
                    ], default="")

                    commentary = first_text(parent, [
                        "div[data-testid='commentary']",
                        "div[data-test-id='commentary']"
                    ], default="N/A")

                    sub_info_elem = None
                    for selector in [
                        "div[data-testid='horse-sub-info']",
                        "div[data-test-id='horse-sub-info']"
                    ]:
                        try:
                            sub_info_elem = parent.find_element(By.CSS_SELECTOR, selector)
                            break
                        except Exception:
                            continue

                    sub_info_text = sub_info_elem.text if sub_info_elem else ""
                    age = re.search(r"Age: (\d+)", sub_info_text)
                    weight = re.search(r"Weight: ([\d-]+)", sub_info_text)

                    jockey_name = "N/A"
                    trainer_name = "N/A"

                    if sub_info_elem:
                        jockey_name = first_text(sub_info_elem, [
                            "a[href*='/jockey/'] span",
                            "a[href*='/jockey/']"
                        ], default="N/A").replace("J:", "").strip()

                        trainer_name = first_text(sub_info_elem, [
                            "a[href*='/trainer/'] span",
                            "a[href*='/trainer/']"
                        ], default="N/A").replace("T:", "").strip()

                    odds = first_text(parent, [
                        "span[class*='BetLink']",
                        "a[class*='BetLink']"
                    ])

                    history_elems = all_elements(parent, [
                        "span[data-testid^='race-history-stat-']",
                        "span[data-test-id^='race-history-stat-']"
                    ])
                    history_stats = " | ".join([s.text.strip() for s in history_elems if s.text.strip()])

                    if horse_name != "N/A":
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
                except Exception as e:
                    print(f"   Pre-race horse parse error: {e}")
                    continue

            break

        except TimeoutException:
            print(f"   Timeout loading pre-race page (attempt {attempt + 1}/{max_retries})")
            time.sleep(3)
        except Exception as e:
            print(f"   Pre-race scrape error (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(3)

    return pd.DataFrame(data)


# ===============================================================
# 🏁 SCRAPE POST-RACE RESULTS
# ===============================================================
def scrape_results(driver, result_url, max_retries=3):
    data = []

    for attempt in range(max_retries):
        try:
            driver.get(result_url)
            time.sleep(3)

            # --- Race info ---
            race_name = first_text(driver, [
                "h1[data-testid='racecard-race-name']",
                "h1[data-test-id='racecard-race-name']",
                "h1"
            ])

            race_date_text = first_text(driver, [
                "p[class*='CourseListingHeader__StyledMainSubTitle']"
            ])

            try:
                race_date_obj = datetime.strptime(race_date_text, "%A %d %B %Y")
                race_date = race_date_obj.strftime("%d/%m/%Y")
                race_day_of_week = race_date_obj.strftime("%A")
            except Exception:
                race_date = "N/A"
                race_day_of_week = "N/A"

            race_time_text = first_text(driver, [
                "p[class*='CourseListingHeader__StyledMainTitle']"
            ])

            parts = race_time_text.split()
            race_time = parts[0] if parts else "N/A"
            race_location = " ".join(parts[1:]) if len(parts) > 1 else "N/A"

            winning_time = "N/A"
            race_distance = "N/A"
            race_going = "N/A"
            race_runners = "N/A"
            race_surface = "N/A"

            summary_items = all_elements(driver, [
                "li.RacingRacecardSummary__StyledAdditionalInfo-sc-ff7de2c2-3",
                "li[class*='RacingRacecardSummary__StyledAdditionalInfo']",
                "li[class*='AdditionalInfo']"
            ])

            for li in summary_items:
                t = li.text.strip()
                if m := re.search(r"Winning time:\s*([0-9m\s\.]+)", t):
                    winning_time = m.group(1).strip()
                if m := re.search(r"(\d+)\s*Runners?", t):
                    race_runners = m.group(1)
                if m := re.search(r"(\d+\s*(?:m|f|y))", t):
                    race_distance = m.group(1)
                if m := re.search(r"(Heavy|Soft|Good|Firm|Standard|Slow|Yielding|Fast)", t):
                    race_going = m.group(1)
                if m := re.search(r"(Turf|Polytrack|Tapeta|AW|Dirt|All Weather|Allweather)", t, re.I):
                    race_surface = m.group(1)

            # --- Prize money ---
            prize_dict = {}
            try:
                prize_rows = all_elements(driver, [
                    "#prizemoney span.PrizeMoney__Prize-sc-1dca786a-0",
                    "#prizemoney span[class*='PrizeMoney__Prize']"
                ])
                for row in prize_rows:
                    try:
                        label = row.find_element(
                            By.CSS_SELECTOR,
                            "span[class*='PrizeLabel']"
                        ).text.strip().replace(":", "")
                        amount = row.find_element(
                            By.CSS_SELECTOR,
                            "span[class*='PrizeNumber']"
                        ).text.strip()
                        prize_dict[label] = amount
                    except Exception:
                        continue
            except Exception:
                prize_dict = {}

            # --- Horse-level results ---
            horse_elements = all_elements(driver, [
                "div[class*='ResultRunner__StyledResultRunnerWrapper']",
                "div[class*='ResultRunnerWrapper']"
            ])

            print(f"   Post-race horse containers found: {len(horse_elements)}")

            for horse_elem in horse_elements:
                try:
                    pos = first_text(horse_elem, [
                        "div[data-testid='position-no']",
                        "div[data-test-id='position-no']"
                    ])

                    silk_url = first_attr(horse_elem, [
                        "div[class*='StyledSilkContainer'] img",
                        "img"
                    ], "src", default="N/A")

                    horse_number = first_text(horse_elem, [
                        "div[data-testid='saddle-cloth-no']",
                        "div[data-test-id='saddle-cloth-no']"
                    ])

                    stall_number = first_text(horse_elem, [
                        "div[data-testid='stall-no']",
                        "div[data-test-id='stall-no']"
                    ])

                    horse_name = first_text(horse_elem, [
                        "div[class*='StyledHorseName'] a",
                        "a[href*='/horse/']"
                    ])

                    ride_desc = first_text(horse_elem, [
                        "div[data-testid='ride-description']",
                        "div[data-test-id='ride-description']"
                    ], default="N/A")

                    trainer = "N/A"
                    jockey = "N/A"

                    person_spans = all_elements(horse_elem, [
                        "span[class*='StyledPersonName']"
                    ])

                    for span in person_spans:
                        try:
                            parent_text = span.find_element(By.XPATH, "./..").text
                            if parent_text.startswith("T:"):
                                trainer = span.text.strip()
                            elif parent_text.startswith("J:"):
                                jockey = span.text.strip()
                        except Exception:
                            continue

                    sp = first_text(horse_elem, [
                        "span[class*='BetLink']",
                        "a[class*='BetLink']"
                    ])

                    prize_money = prize_dict.get(pos, "0")

                    if horse_name != "N/A":
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
                    print(f"   Post-race horse parse error: {e}")

            break

        except TimeoutException:
            print(f"   Timeout loading results page (attempt {attempt + 1}/{max_retries})")
            time.sleep(3)
        except Exception as e:
            print(f"   Results scrape error (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(3)

    return pd.DataFrame(data)


# ===============================================================
# ☁️ UPLOAD TO BIGQUERY
# ===============================================================
def upload_to_bigquery(df, table_suffix):
    if df.empty:
        print(f"⚠️ No {table_suffix} data to upload — skipping.")
        return

    df["load_timestamp"] = datetime.utcnow()

    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.Scrape_{table_suffix}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

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

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

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

        print(f"[{i+1}/{len(df_races)}] Scraping {location} {time_}...")

        prerace_df = pd.DataFrame()
        postrace_df = pd.DataFrame()
        status = "Pending"

        if pd.notna(prerace_url) and str(prerace_url).strip():
            prerace_df = scrape_prerace(driver, prerace_url)
            if not prerace_df.empty:
                prerace_data.append(prerace_df)

        if pd.notna(postrace_url) and str(postrace_url).strip():
            postrace_df = scrape_results(driver, postrace_url)
            if not postrace_df.empty:
                postrace_data.append(postrace_df)

        # Status logic
        if not postrace_df.empty:
            status = "Complete"
        elif not prerace_df.empty:
            status = "Pending"
        else:
            status = "Pending"

        updated_rows.append({
            "Date": row["Date"],
            "Location": row["Location"],
            "Time": row["Time"],
            "prerace_URL": prerace_url,
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
    print(f"✅ {sum(1 for r in updated_rows if r['Status'] == 'Complete')} complete")
    print(f"🕓 {sum(1 for r in updated_rows if r['Status'] == 'Pending')} pending")


if __name__ == "__main__":
    main()
