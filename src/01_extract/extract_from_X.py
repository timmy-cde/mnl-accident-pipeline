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
    """Initialize Firefox WebDriver with appropriate options for scraping."""
    options = webdriver.FirefoxOptions()
    options.add_argument("-headless")
    options.add_argument("--width=1920")
    options.add_argument("--height=3000")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.set_preference("privacy.trackingprotection.enabled", True)
    options.set_preference("network.http.referer.XOriginPolicy", 2)
    options.set_preference("network.http.referer.XOriginTrimmingPolicy", 2)
    options.set_preference("dom.popup_allowed_events", "")
    options.set_preference("dom.disable_open_during_load", True)
    options.set_preference("dom.webdriver.enabled", False)
    options.set_preference("media.peerconnection.enabled", False)
    options.set_preference("dom.webnotifications.enabled", False)
    options.set_preference(
        "general.useragent.override",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0"
    )

    driver = webdriver.Firefox(options=options)
    return driver


def parse_tweets(driver):
    """Extract MMDA ALERT tweets from the current page."""
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

    # Get LAST "show-more" link for pagination
    cursor = None
    more_links = driver.find_elements(By.CSS_SELECTOR, "div.show-more a")
    if more_links:
        cursor = more_links[-1].get_attribute("href")

    return tweets, cursor


def scrape_for_date(target_date_str):
    """
    Scrape tweets for a specific date.
    
    Args:
        target_date_str: Date in format "YYYY-MM-DD"
        
    Returns:
        DataFrame with scraped tweets for that date
    """
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").replace(tzinfo=PHT)

    driver = init_driver()
    wait = WebDriverWait(driver, 30)

    url = f"{BASE_URL}/MMDA"
    all_data = []
    page = 1

    while True:
        print(f"Scraping page {page}: {url}")
        driver.get(url)
        time.sleep(3)

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
            if tweet_date == target_date.date():
                all_data.append(t)
            elif tweet_date < target_date.date():
                stop = True
                break

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
    return pd.DataFrame(all_data)


def save_and_upload_data(df, date_str):
    """
    Save DataFrame to CSV and upload to GCS.
    
    Args:
        df: DataFrame with scraped data
        date_str: Date string in format "YYYY-MM-DD"
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    filename = f"scrape_data_{year}{month}{day}.csv"
    output_path = "./.data/scrape"

    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)

    df.to_csv(f"{output_path}/{filename}", index=False)

    if df.empty:
        print(f"No tweets found for {date_str}.")
        return

    # Upload to GCS
    bucket_name = os.environ.get("BUCKET_NAME")
    bucket_folder_name = os.environ.get("RAW_FOLDER_NAME")
    gcs_object_name = f"{bucket_folder_name}/scrape/{year}/{month}/{filename}"
    
    upload_to_gcs(bucket_name, gcs_object_name, f"{output_path}/{filename}")
    print(f"{gcs_object_name} uploaded to GCS.")


def run_daily_scrape():
    """Scrape data for yesterday (daily run mode)."""
    now = datetime.now(PHT)
    yesterday = now - timedelta(days=1)
    target_date = yesterday.strftime("%Y-%m-%d")

    print(f"Running daily scrape for {target_date}...")
    df = scrape_for_date(target_date)
    save_and_upload_data(df, target_date)


def run_backfill_scrape(start_date_str, end_date_str):
    """Scrape data for a date range (backfill mode)."""

    if not start_date_str or not end_date_str:
        print("Error: START_DATE and END_DATE environment variables required for backfill mode.")
        return

    # Loop from newest to oldest
    current_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")

    print(f"Running backfill scrape from {start_date_str} to {end_date_str}...")

    while current_date >= start_date_obj:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"\nScraping for {date_str} ...")

        df = scrape_for_date(date_str)
        save_and_upload_data(df, date_str)

        # Move one day back
        current_date -= timedelta(days=1)

def main():
    """
    Main entry point. Runs in daily mode by default, or backfill mode if
    START_DATE and END_DATE environment variables are set.
    """
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")

    if start_date and end_date:
        run_backfill_scrape(start_date, end_date)
    else:
        run_daily_scrape()


if __name__ == "__main__":
    main()
