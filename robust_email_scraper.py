#!/usr/bin/env python3
# advanced_email_scraper_parallel.py

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
import re
import csv
import time
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------- CONFIG ----------------
input_file = "websites.csv"           # Parent page URLs
output_file = "scraped_emails.csv"    # Output CSV
delay_between_pages = 1               # Delay between visits
headless = True                        # Headless Chrome
max_workers = 4                        # Parallel threads

# ---------------- SELENIUM SETUP ----------------
def create_driver():
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")  # reduce logs
    return webdriver.Chrome(options=options)

# ---------------- HELPERS ----------------
def extract_emails(text):
    """Extract emails from text using regex"""
    return re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)

def get_child_links(driver, url, parent_domain):
    """Get all child/profile links from a page"""
    try:
        driver.get(url)
        time.sleep(delay_between_pages)
        links = set()
        for a in driver.find_elements(By.TAG_NAME, "a"):
            href = a.get_attribute("href")
            if href and parent_domain in href and href != url:
                links.add(href)
        return list(links)
    except WebDriverException as e:
        print(f"Error getting links from {url}: {e}")
        return []

def scrape_page(driver, url):
    """Visit a page and return emails found"""
    try:
        driver.get(url)
        time.sleep(delay_between_pages)
        page_source = driver.page_source
        emails = extract_emails(page_source)
        return emails
    except WebDriverException:
        print(f"Failed to scrape {url}")
        return []

def scrape_worker(url):
    """Worker function for parallel scraping"""
    driver = create_driver()
    emails = scrape_page(driver, url)
    driver.quit()
    return emails

# ---------------- MAIN ----------------
all_emails = set()
visited_links = set()

# Load parent URLs
parent_urls = []
with open(input_file, newline="", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            parent_urls.append(line)

# Scrape parent pages and collect child links
child_links_all = []
for parent_url in parent_urls:
    if not parent_url.startswith("http"):
        parent_url = "http://" + parent_url
    print(f"\nScraping parent page: {parent_url}")
    visited_links.add(parent_url)

    driver = create_driver()
    # Extract emails from parent page
    emails = scrape_page(driver, parent_url)
    if emails:
        all_emails.update(emails)

    # Get child/profile links
    parent_domain = parent_url.split("//")[1].split("/")[0]
    child_links = get_child_links(driver, parent_url, parent_domain)
    print(f"Found {len(child_links)} child/profile links.")
    child_links_all.extend(child_links)
    driver.quit()

# Remove duplicates and already visited
child_links_all = [link for link in set(child_links_all) if link not in visited_links]

# ---------------- PARALLEL SCRAPING ----------------
print(f"\nStarting parallel scraping of {len(child_links_all)} child links using {max_workers} threads...")
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_url = {executor.submit(scrape_worker, url): url for url in child_links_all}
    for future in as_completed(future_to_url):
        url = future_to_url[future]
        try:
            emails = future.result()
            if emails:
                all_emails.update(emails)
                print(f"Scraped {len(emails)} emails from {url}")
        except Exception as e:
            print(f"Error scraping {url}: {e}")

# ---------------- SAVE RESULTS ----------------
with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Email"])
    for email in sorted(all_emails):
        writer.writerow([email])

print(f"\nScraping finished! Found {len(all_emails)} unique emails.")
print(f"Saved to {output_file}")
