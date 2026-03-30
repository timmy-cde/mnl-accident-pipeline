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
            tweetlinkid = date_elem.get_attribute("href")
            tweetlinkid = tweetlinkid.replace('nitter.net', 'x.com')

            # Extract only date part
            date_str = date_str.split(" · ")[0]
            created_at = datetime.strptime(date_str, "%b %d, %Y")
            created_at = created_at.replace(tzinfo=PHT)

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


def scrape_historical(start_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=PHT)

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
            if t["created_at"] < start_date:
                stop = True
                break
            all_data.append(t)

        print(f"Collected so far: {len(all_data)}")

        if stop:
            print("Reached target start date. Stopping.")
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

    df = pd.DataFrame(all_data)
    return df


def scrape_by_date(target_date_str):
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()

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

            if tweet_date == target_date:
                all_data.append(t)

            elif tweet_date < target_date:
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
    now = datetime.now(PHT)
    yesterday = now - timedelta(days=1)

    year = yesterday.strftime("%Y")
    month = yesterday.strftime("%m")
    day = yesterday.strftime("%d")

    TARGET_DATE = yesterday.strftime("%Y-%m-%d")

    # TARGET_DATE = "2026-03-25"

    dt = datetime.strptime(TARGET_DATE, "%Y-%m-%d")

    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    filename = f"scrape_data_{year}{month}{day}.csv"
    output_path = "./.data/scrape"

    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)

    df = scrape_by_date(TARGET_DATE)
    df.to_csv(f"{output_path}/{filename}", index=False)

    # safer empty check
    if df.empty:
        print(f"No scraped data: {TARGET_DATE}")
        return

    # Upload to GCS
    bucket_name = os.environ.get("BUCKET_NAME")
    bucket_folder_name = os.environ.get("RAW_FOLDER_NAME")

    gcs_object_name = f"{bucket_folder_name}/scrape/{year}/{month}/{filename}"

    upload_to_gcs(bucket_name, gcs_object_name, f"{output_path}/{filename}")
    print(f"{gcs_object_name} was uploaded to gcs.")

if __name__ == "__main__":
    main()