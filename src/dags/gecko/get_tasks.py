from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
import json, os, re
from time import sleep

def selenium_driver():
    chrome_options = Options()
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--remote-debugging-port=9222")
    # Start session
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def get_category():
    driver = selenium_driver()
    # Crawler
    data = {}
    for page in range(1, 6, 1):
        driver.get(f"https://www.coingecko.com/en/categories?page={page}")
        print("> Crawling page - ", page)
        # 1 page level
        lst = []
        for item in range(1, 101, 1):
            category_item = {}
            try: 
                category = driver.find_element(By.CSS_SELECTOR, f"tbody tr:nth-child({item}) td:nth-child(2) div").text
            except NoSuchElementException:
                category = None

            try:    
                url = driver.find_element(By.CSS_SELECTOR, f"tbody tr:nth-child({item}) td:nth-child(2) a").get_attribute("href")
            except NoSuchElementException:
                url = None

            try:
                market_cap = driver.find_element(By.CSS_SELECTOR, f"tbody tr:nth-child({item}) td:nth-child(7) span").text
            except NoSuchElementException:
                market_cap = None
                
            try:
                last_7_days = driver.find_element(By.CSS_SELECTOR, f"tbody tr:nth-child({item}) td:nth-child(6) span").text
            except NoSuchElementException:
                last_7_days = None

            try:
                num_coins = driver.find_element(By.CSS_SELECTOR, f"tbody tr:nth-child({item}) td:nth-child(9)").text
            except NoSuchElementException:
                num_coins = None

            try:    
                volume = driver.find_element(By.CSS_SELECTOR, f"tbody tr:nth-child({item}) td:nth-child(8) span").text
            except NoSuchElementException:
                volume = None

            # Testing
            try:    
                caret = driver.find_element(By.CSS_SELECTOR, f"tbody tr:nth-child({item}) td:nth-child(6) span i").get_attribute("class")
            except NoSuchElementException:
                caret = None

            # store into dict
            category_item['category'] = category
            category_item['market_cap'] = market_cap
            category_item['num_coins'] = num_coins
            category_item['caret'] = caret
            category_item['last_7_days'] = last_7_days
            category_item['volume_last_day'] = volume
            category_item['url'] = url

            lst.append(category_item)
        data[f"page-{page}"] = lst
    # Write data
    with open("/opt/airflow/data/gecko/category.json", 'w') as file:
        json.dump(data, file, indent=2)

def get_category_details():
    driver = selenium_driver()

    # extract only urls
    urls = []
    with open("/opt/airflow/data/gecko/category.json", 'r') as file:
        data = json.load(file)
        for page, items in data.items():
            for item in items:
                if item['url'] is not None:
                    urls.append(item['url'])
    print(f"> There are {len(urls)} urls to crawl !!")

    # Url level
    data_by_url = {}
    url_counter = 0

    for index, url in enumerate(urls):
        driver.get(url)
        print(f"Crawling url {index+1} {url} ...")

        # Take num pages
        try:
            results = driver.find_element(By.XPATH, "/html/body/div[3]/main/div/div[6]/div/div[1]").text
            num_results = int(re.findall("(\d+) results", results)[0])
            num_pages = num_results // 100 + 1
        except NoSuchElementException:
            continue

        # Crawl different pages in the url
        data_for_1_url = []
        for page in range(1, num_pages + 1, 1):
            driver.get(f"{url}?page={page}")
            print(f"> Crawling page {page}")
            # Loops through items in the same page
            for i in range(1, 101, 1):
                temp = {}
                try:
                    name = driver.find_element(By.CSS_SELECTOR, f"tbody tr:nth-child({i}) td:nth-child(3) div div").text
                except NoSuchElementException:
                    name = None 
                try: 
                    symbol = driver.find_element(By.CSS_SELECTOR, f"tbody tr:nth-child({i}) td:nth-child(3) div div div").text
                except NoSuchElementException:
                    symbol = None

                # Check the values is empty or not
                if name is not None or symbol is not None:
                    # store each item
                    temp['name'] = name 
                    temp['symbol'] = symbol
                    temp['url'] = url
                    data_for_1_url.append(temp)
                else:
                    break

        category_name = url.split("/")[-1]
        data_by_url[category_name] = data_for_1_url

        # Condition to stop (optional)
        # url_counter += 1
        # if url_counter == 3:
        #     break

    with open("/opt/airflow/data/gecko/category_details.json", "w") as file:
        json.dump(data_by_url, file, indent=2)

if __name__ == "__main__":
    get_category_details()




        