"""
Data Scraping F1 drivers standing over the past 50 years into drivers
"""

import json
import time
import os
import logging
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
data_dir = os.path.join(project_root, "data")
os.makedirs(data_dir, exist_ok=True)
output_file = os.path.join(data_dir, "drivers.json")

def scrape_f1_drivers():
    
    current_year = datetime.now().year
    start_year = current_year - 50

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    all_results = []

    try:
        for year in range(start_year, current_year + 1):
            for attempt in range(3):
                try:
                    logger.info(f"Starting F1 Scraper ({year}) - Attempt {attempt + 1}")
                    driver.get(f"https://www.formula1.com/en/results/{year}/drivers")

                    try:
                        iframe = WebDriverWait(driver, 2).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[id^='sp_message_iframe']"))
                        )
                        driver.switch_to.frame(iframe)
                        accept_button = driver.find_element(By.CSS_SELECTOR, 'button[title="Accept all"]')
                        accept_button.click()
                        driver.switch_to.default_content()
                    except (TimeoutException, NoSuchElementException):
                        driver.switch_to.default_content()
                    
                    wait = WebDriverWait(driver, 5)
                    results = wait.until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table[class*='Table-module_table'] tbody tr"))
                    )

                    time.sleep(1)

                    year_data = []
                    for result in results:
                        def get_col_text(idx):
                            try:
                                return result.find_element(By.CSS_SELECTOR, f"td:nth-child({idx})").text.strip()
                            except NoSuchElementException:
                                return ""
                        
                        driver_name = get_col_text(2).replace('\n', ' ')
                        
                        if driver_name:
                            item = {
                                "Year": year,
                                "Position": get_col_text(1),
                                "Driver": driver_name,
                                "Nationality": get_col_text(3),
                                "Team": get_col_text(4),
                                "Points": get_col_text(5)
                            }
                            year_data.append(item)
                    
                    all_results.extend(year_data)
                    break 

                except StaleElementReferenceException:
                    logger.warning(f"Stale Element detected for {year}. Retrying...")
                    time.sleep(2)
                    continue
                except TimeoutException:
                    logger.warning(f"No data found for {year}")
                    break

    finally:
        driver.quit()

    df = pd.DataFrame(all_results)
    df.to_json(output_file, orient='records', indent=2)
    logger.info(f"Scraped data saved to: {output_file}")

if __name__ == "__main__":
    scrape_f1_drivers()