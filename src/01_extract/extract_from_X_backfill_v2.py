from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time
import os
from dotenv import load_dotenv
from upload_to_gcs import upload_to_gcs

load_dotenv()

BASE_URL = "https://nitter.net"
PHT = ZoneInfo("Asia/Manila")


def init_driver():
    options = webdriver.FirefoxOptions()
    options.add_argument("-headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.set_preference("privacy.trackingprotection.enabled", True)
    options.set_preference("network.http.referer.XOriginPolicy", 2)
    options.set_preference("network.http.referer.XOriginTrimmingPolicy", 2)
    options.set_preference("dom.popup_allowed_events", "")
    options.set_preference("dom.disable_open_during_load", True)
    options.set_preference(
        "general.useragent.override",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0"
    )

    driver = webdriver.Firefox(options=options)
    return driver


def parse_tweets(driver):
    tweets = []
    items = driver.find_elements(By.CSS_SELECTOR, ".timeline-item")

    for item in items:
        try:
            content = item.find_element(By.CSS_SELECTOR, ".tweet-content").text.strip()
            date_elem = item.find_element(By.CSS_SELECTOR, ".tweet-date a")
            date_str = date_elem.get_attribute("title")
            tweetlinkid = date_elem.get_attribute("href").replace('nitter.net', 'x.com')

            # Extract only date part
            date_str = date_str.split(" · ")[0]
            created_at = datetime.strptime(date_str, "%b %d, %Y").replace(tzinfo=PHT)

            if "MMDA ALERT" in content:
                tweets.append({
                    "content": content,
                    "tweetlinkid": tweetlinkid,
                    "created_at": created_at
                })

        except Exception:
            continue

    # Get LAST "show-more" link
    cursor = None
    more_links = driver.find_elements(By.CSS_SELECTOR, "div.show-more a")
    if more_links:
        cursor = more_links[-1].get_attribute("href")

    return tweets, cursor


def scrape_for_date(target_date):
    """Scrape tweets for a single target_date only."""
    target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()

    driver = init_driver()
    wait = WebDriverWait(driver, 20)
    url = f"{BASE_URL}/MMDA"
    all_data = []
    page = 1

    while True:
        print(f"Scraping page {page}: {url}")
        driver.get(url)

        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".timeline-item")))
        except:
            print("Page didn't load tweets.")
            break

        tweets, cursor = parse_tweets(driver)

        if not tweets:
            print("No tweets found, stopping.")
            break

        stop = False
        for t in tweets:
            tweet_date = t["created_at"].date()
            if tweet_date == target_date_obj:
                all_data.append(t)
            elif tweet_date < target_date_obj:
                stop = True
                break

        print(f"Collected so far: {len(all_data)}")

        if stop:
            print("Reached older than target date. Stopping.")
            break

        if not cursor:
            print("No more pages.")
            break

        # Normalize cursor URL
        if cursor.startswith("http"):
            url = cursor
        elif "MMDA" in cursor:
            url = f"{BASE_URL}{cursor}"
        else:
            url = f"{BASE_URL}/MMDA{cursor}"

        page += 1
        time.sleep(5)

    driver.quit()
    return pd.DataFrame(all_data)


def main():
    # Backfill settings
    START_DATE = "2026-03-15"
    END_DATE = "2026-03-24"  # newest date first

    output_path = "./.data/scrape"
    os.makedirs(output_path, exist_ok=True)

    # Loop from newest to oldest
    current_date = datetime.strptime(END_DATE, "%Y-%m-%d")
    start_date_obj = datetime.strptime(START_DATE, "%Y-%m-%d")

    while current_date >= start_date_obj:
        date_str = current_date.strftime("%Y-%m-%d")
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")

        filename = f"scrape_data_{year}{month}{day}.csv"
        print(f"\nScraping for {date_str} ...")

        df = scrape_for_date(date_str)
        df.to_csv(f"{output_path}/{filename}", index=False)

        if df.empty:
            print(f"No tweets found for {date_str}.")
        else:
            # Upload to GCS
            bucket_name = os.environ.get("BUCKET_NAME")
            bucket_folder_name = os.environ.get("RAW_FOLDER_NAME")
            gcs_object_name = f"{bucket_folder_name}/scrape/{year}/{month}/{filename}"
            upload_to_gcs(bucket_name, gcs_object_name, f"{output_path}/{filename}")
            print(f"{gcs_object_name} uploaded to GCS.")

        # Move one day back
        current_date -= timedelta(days=1)


if __name__ == "__main__":
    main()