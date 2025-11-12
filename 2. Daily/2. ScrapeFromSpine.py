# ===============================================================
# 🏁 FULL POST-RACE SCRAPER (MATCHES LOCAL VERSION)
# ===============================================================
def scrape_results(driver, result_url, max_retries=3):
    """Scrape full post-race results from Sporting Life."""
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

            # ----------------------------------------------------------
            # 🏁 RACE-LEVEL DATA
            # ----------------------------------------------------------
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

                # Additional info list
                for li in driver.find_elements(
                    By.CSS_SELECTOR,
                    "li.RacingRacecardSummary__StyledAdditionalInfo-sc-ff7de2c2-3"
                ):
                    text = li.text.strip()

                    # Winning time
                    match = re.search(r"Winning time:\s*([0-9m\s\.]+)", text)
                    if match:
                        winning_time = match.group(1).strip()

                    # Distance
                    match = re.search(r"(\d+\s*(?:m|f|y)(?:\s*\d*\s*(?:f|y))?)", text)
                    if match:
                        race_distance = match.group(1)

                    # Going
                    match = re.search(
                        r"(Heavy|Soft|Good to Soft|Good to Firm|Good|Firm|Standard|Standard / Slow|Yielding|Fast|Slow)",
                        text,
                    )
                    if match:
                        race_going = match.group(1)

                    # Runners
                    match = re.search(r"(\d+)\s*Runners?", text)
                    if match:
                        race_runners = match.group(1)

                    # Surface
                    match = re.search(r"(Turf|All Weather|AW|Allweather|Polytrack|Fibresand|Tapeta|Dirt)", text, re.I)
                    if match:
                        race_surface = match.group(1)

            except Exception as e:
                print("Error extracting race info:", e)
                race_name = race_date = race_day_of_week = race_location = race_time = \
                race_distance = race_going = race_runners = race_surface = winning_time = "N/A"

            # ----------------------------------------------------------
            # 🐎 HORSE-LEVEL DATA
            # ----------------------------------------------------------
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

                    sp = safe_text(horse_elem, "span[class*='BetLink__BetLinkStyle']")

                    # Prize money
                    try:
                        prize_elem = horse_elem.find_element(
                            By.CSS_SELECTOR,
                            "span[class*='PrizeNumber']"
                        )
                        prize_money = prize_elem.text.strip()
                    except:
                        prize_money = "0"

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

            break  # success → exit retry loop

        except TimeoutException:
            print(f"[Retry {attempt+1}/{max_retries}] Timeout scraping results")
            time.sleep(3)

    return pd.DataFrame(data)
