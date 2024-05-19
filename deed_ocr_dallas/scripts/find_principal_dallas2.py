import re
import requests
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import traceback
import cv2
import pytesseract
import time
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import cProfile
from threading import local

    # Thread local storage to hold database connections
thread_local = local()

def wait_for_table_load(driver, timeout=30):
    """ Wait for the table to load by checking for the presence of rows within the table. """
    try:
        first_row_present = EC.presence_of_element_located((By.CSS_SELECTOR, "#main-content > div > div > div.search-results__results-wrap > div.a11y-table > table > tbody > tr:nth-child(1)"))
        WebDriverWait(driver, timeout).until(first_row_present)
        print("First row of table loaded successfully.")
        return True
    except TimeoutException:
        print("Timed out waiting for the first row of the table to load")
        return False

def extract_image_urls(url, max_retries=3, retry_delay=5):
    retries = 0
    driver = get_headless_driver()  # Ensure using headless driver
    while retries < max_retries:
        try:
            driver.get(url)
            # Add a sleep delay to respect the server's rate limit
            time.sleep(1)  # Adjust the delay as appropriate for the server's tolerance

            try:
                # Wait for the image element to be present using the JavaScript path
                image_element = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#main-content > section > div.css-wnovuq > section > svg > g > image'))
                )

                # Extract the image URL from the 'xlink:href' attribute
                image_url = image_element.get_attribute('xlink:href')

                if image_url:
                    base_url = image_url[:-6]  # Remove the "_1.png" suffix
                    return [f"{base_url}_{i}.png" for i in range(1, 3)]  # Generate URLs for the first two pages
                else:
                    print(f"No image URL found for {url}")
                    return []
            except (TimeoutException, NoSuchElementException) as e:
                print(f"Error occurred while extracting image URLs for {url}: {str(e)}")
                retries += 1
                print(f"Retrying ({retries}/{max_retries}) in {retry_delay} seconds...")
                time.sleep(retry_delay)
        except (TimeoutException, WebDriverException) as e:
            print(f"Error occurred while extracting image URLs for {url}: {str(e)}")
            retries += 1
            print(f"Retrying ({retries}/{max_retries}) in {retry_delay} seconds...")
            time.sleep(retry_delay)
        finally:
            driver.quit()
    print(f"Max retries reached for {url}. Skipping...")
    return []

def download_image(url):
    response = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Referer": "https://dallas.tx.publicsearch.us/search/property",
        "Cookie": "authToken=da37f420-93d0-47fa-8353-7265132f535d; authToken.sig=JLCqSHZdGalrtnzqw4WzFmR8Dj4"
    }, verify=False)
    return response.content

def ocr_image(image_data):
    image = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)
    
    # Scale the image to a desired resolution (e.g., 300 DPI)
    scale_factor = 150 / 70  # Assuming the original resolution is 70 DPI
    image = cv2.resize(image, None, fx=scale_factor, fy=scale_factor, interpolation=cv2.INTER_CUBIC)
    
    ocr_text = pytesseract.image_to_string(image, lang='eng', config='--psm 6')
    pattern = r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?'
    matches = re.findall(pattern, ocr_text)
    max_dollar_value = max(float(match.replace('$', '').replace(',', '')) for match in matches) if matches else None

    # Free up memory by deleting the image after processing
    del image
    return max_dollar_value

def extract_row_data(row):
    try:
        doc_type = row.find_element(By.CSS_SELECTOR, 'td.col-5 span').text.strip()
        recorded_date = row.find_element(By.CSS_SELECTOR, 'td.col-6 span').text.strip()
        document_number = row.find_element(By.CSS_SELECTOR, 'td.col-7 span').text.strip()
        town = row.find_element(By.CSS_SELECTOR, 'td.col-9 span').text.strip()
        subdivision_html = row.find_element(By.CSS_SELECTOR, 'td.col-10 span').text
        subdivision = re.search(r"Name: ([^,]+),", subdivision_html).group(1).strip() if re.search(r"Name: ([^,]+),", subdivision_html) else None

        lot_block_text = row.find_element(By.CSS_SELECTOR, 'td.col-10 span').text.strip()
        lot_block_match = re.search(r'Lot:\s*(\w+)\s*Block:\s*(\w+)', lot_block_text)
        lot_number = lot_block_match.group(1) if lot_block_match else None
        block_number = lot_block_match.group(2) if lot_block_match else None

        # Debug prints to confirm extraction
        print(f"Extracted: Document Number: {document_number}, Date: {recorded_date}, Town: {town}, Subdivision: {subdivision}, Lot: {lot_number}, Block: {block_number}")

        return {
            'doc_type': doc_type,
            'recorded_date': recorded_date,
            'document_number': document_number,
            'town': town,
            'subdivision': subdivision,
            'lot_number': lot_number,
            'block_number': block_number
        }
    except Exception as e:
        print(f"Error extracting data from row: {str(e)}")
        return None

def safe_find_element(row, selector, retries=3):
    for _ in range(retries):
        try:
            return row.find_element(By.CSS_SELECTOR, selector)
        except selenium.common.exceptions.StaleElementReferenceException:
            time.sleep(1)  # short delay before retrying
    raise Exception(f"Element with selector {selector} could not be reliably located.")

def process_data_on_page(driver):
    rows = driver.find_elements(By.CSS_SELECTOR, "#main-content > div > div > div.search-results__results-wrap > div.a11y-table > table > tbody > tr")
    print(f"Processing {len(rows)} rows on current page.")

    # This could potentially be parallelized with a ThreadPoolExecutor if desired
#    for row in rows:
#        process_row(row)
    with ThreadPoolExecutor(max_workers=7) as executor:
        executor.map(process_row, rows)

def process_row(row):
    # Each thread creates its own database connection
    conn = get_db_connection()  # Reuse the connection stored in thread_local
    c = conn.cursor()
    start_time = time.time()  # Start timing

    try:
        # Extract the ID from the checkbox and construct the document URL
#        checkbox_input = row.find_element(By.CSS_SELECTOR, 'input[data-testid="searchResultCheckbox"]')
        checkbox_input = safe_find_element(row, 'input[data-testid="searchResultCheckbox"]')
        checkbox_id = checkbox_input.get_attribute('id')
        doc_id = checkbox_id.replace('table-checkbox-', '')
        doc_url = f"https://dallas.tx.publicsearch.us/doc/{doc_id}"

        # Extract new data from row
        row_data = extract_row_data(row)
        if not row_data:
            print("Failed to extract data from row.")
            return

        # Skip processing if this URL is already processed
#        if row_exists(doc_url, conn):
#            print(f"Skipping already processed row: {doc_url}")
#            return
        
        # Check existing data including subdivision info
        c.execute("SELECT document_number, recorded_date, town, subdivision FROM deeds WHERE doc_url = ?", (doc_url,))
        existing = c.fetchone()

        if existing:
            # Check if updates are necessary
            update_needed = False
            update_fields = {}
            for idx, field in enumerate(['document_number', 'recorded_date', 'town', 'subdivision']):
                if existing[idx] is None and row_data[field]:
                    update_fields[field] = row_data[field]
                    update_needed = True
            if update_needed:
                update_query = "UPDATE deeds SET " + ', '.join([f"{k} = ?" for k in update_fields.keys()]) + " WHERE doc_url = ?"
                c.execute(update_query, list(update_fields.values()) + [doc_url])
                conn.commit()
                print(f"Updated row: {doc_url} with fields {list(update_fields.keys())}")
            # Complete data exists, skipping further processing
            print(f"Skipping fully processed row: {doc_url}")
            return
        else:
            # New row processing
            print(f"Processing new row: {doc_url}")
            process_new_row(row, doc_url, conn, c)  # Handle processing of new rows separately

    except Exception as e:
        # Safely handle the scenario where 'doc_url' might not have been initialized
        error_url = doc_url if 'doc_url' in locals() else "URL not initialized"
        print(f"Error occurred while processing row at {error_url}: {str(e)}")
        traceback.print_exc()
        return None
    finally:
        # Ensure database connection is closed in case of error
        end_time = time.time()  # End timing
        print(f"Processed row in {end_time - start_time:.2f} seconds.")
        c.close()

def process_new_row(row, doc_url, conn, c):
    # New row processing
    
    image_urls = extract_image_urls(doc_url)
    print(f"Extracted image URLs: {image_urls}")
    
    image_data_list = [download_image(url) for url in image_urls]  # Sequential download
    #        with ThreadPoolExecutor(max_workers=1) as executor:
    #            image_data_list = list(executor.map(download_image, image_urls))
    print(f"Downloaded image data: {[len(data) for data in image_data_list]}")
    
    dollar_values = [ocr_image(data) for data in image_data_list]  # Sequential OCR
    #        with ThreadPoolExecutor(max_workers=1) as executor:
    #            dollar_values = list(executor.map(ocr_image, image_data_list))
    print(f"OCR results: {dollar_values}")
    
    # Filter out None values from dollar_values
    dollar_values = [value for value in dollar_values if value is not None]
    max_dollar_value = max(dollar_values) if dollar_values else None
    
    row_data = extract_row_data(row)
    
    row_data.update({
        'doc_url': doc_url,
        'image_urls': str(image_urls),
        'all_dollar_values': str(dollar_values),  # Store all dollar values
        'max_dollar_value': max_dollar_value
    })
    
    # Insert new data into the database
    placeholders = ', '.join(['?' for _ in row_data])
    columns = ', '.join(row_data.keys())
    values = list(row_data.values())

    c.execute(f"INSERT INTO deeds ({columns}) VALUES ({placeholders})", values)
    conn.commit()
    print(f"Inserted new row: {doc_url}")

def update_subdivision_info(conn, doc_url, row):
    # Extract new subdivision info from the row and update the database
    subdivision_info = extract_subdivision_from_row(row)  # Define this function based on your HTML structure
    c = conn.cursor()
    c.execute("UPDATE deeds SET subdivision = ? WHERE doc_url = ?", (new_subdivision, doc_url))
    conn.commit()

def extract_subdivision_from_row(row):
    # Assuming 'row' is a Selenium WebElement and you know the selector for the subdivision info
    try:
        subdivision_html = row.find_element(By.CSS_SELECTOR, 'td.col-10 span').text
        # Example parsing might be required if the text contains more than just the subdivision
        match = re.search(r"Name: ([\w\s]+) Lot:", subdivision_html)
        if match:
            return match.group(1).strip()
        return None
    except NoSuchElementException:
        return None

def get_db_connection(db_path='deed_data.db'):
    if not hasattr(thread_local, 'connection'):
        thread_local.connection = sqlite3.connect(db_path, check_same_thread=False)
    return thread_local.connection

def initialize_db(db_path='deed_data.db'):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS deeds (
            doc_url TEXT UNIQUE,
            image_urls TEXT,
            document_number TEXT,
            recorded_date TEXT,
            lot_number TEXT,
            block_number TEXT,
            all_dollar_values TEXT,
            max_dollar_value REAL
            town TEXT DEFAULT NULL,           -- Ensure new columns are added with default values
            subdivision TEXT DEFAULT NULL,     -- This allows handling of NULL values appropriately
            doc_type TEXT  -- Adding the missing column
        )
    ''')

    # Check for the existence of new columns and add them if necessary
    # This is a simplified approach and would need to be adjusted based on the specific database management system you're using.
    c.execute("PRAGMA table_info(deeds)")
    columns = [info[1] for info in c.fetchall()]  # Fetches the column names from the table_info pragma call

    if 'town' not in columns:
        c.execute('ALTER TABLE deeds ADD COLUMN town TEXT DEFAULT NULL')
    if 'subdivision' not in columns:
        c.execute('ALTER TABLE deeds ADD COLUMN subdivision TEXT DEFAULT NULL')
    if 'doc_type' not in columns:
        c.execute('ALTER TABLE deeds ADD COLUMN doc_type TEXT DEFAULT NULL')

    # Consider adding columns if they do not exist with ALTER TABLE statements if schema updates are needed.
    conn.commit()
    c.close()

def update_db_schema(conn):
    cursor = conn.cursor()
    # List of new or potentially missing columns in your database schema
    new_columns = [
        ("document_number", "TEXT"),
        ("recorded_date", "TEXT"),
        ("town", "TEXT"),
        ("subdivision", "TEXT"),
        ("doc_type", "TEXT")
    ]
    
    for column, col_type in new_columns:
        try:
            # Attempt to add each new column
            cursor.execute(f"ALTER TABLE deeds ADD COLUMN {column} {col_type}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print(f"Column '{column}' already exists. No changes made.")
            else:
                raise  # Re-raise the exception if it's not a duplicate column error
    conn.commit()
    cursor.close()

def row_exists(doc_url, conn):
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM deeds WHERE doc_url = ?', (doc_url,))
    exists = cursor.fetchone() is not None
    cursor.close()
    return exists

def fetch_all_pages(base_url):
    limit = 250
    offset = 0
    max_retries = 3  # Maximum number of retries for loading a page
    driver = get_headless_driver()
    while True:
        url = f"{base_url}&limit={limit}&offset={offset}"
        print(f"Fetching URL: {url}")

        success = False
        for attempt in range(max_retries):
            driver.get(url)
            if wait_for_table_load(driver):
                print("Table loaded successfully.")
                success = True
                break
            else:
                print(f"Failed to load the table, attempt {attempt + 1} of {max_retries}.")
                time.sleep(5)  # Wait before retrying, could be increased based on the server's response time

        if not success:
            print("Failed to load the table after multiple attempts, stopping pagination.")
            break

        # Process the data on the page here
        process_data_on_page(driver)

        # Check if this is the last page
        results_summary = driver.find_element(By.CSS_SELECTOR, 'p[data-testid="resultsSummary"]').text
        if '+' in results_summary:
            total_results = int(results_summary.split()[2].replace(',', '').replace('+', ''))
        else:
            total_results = int(results_summary.split()[-2].replace(',', ''))

        if offset + limit >= total_results:
            print("Reached the last page.")
            break
        offset += limit

    driver.quit()

# Setup headless ChromeDriver
def get_headless_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=chrome_options)

def main():
    overall_start = time.time()

    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    initialize_db()  # Call this once at the start of your script or application

    # Update the database schema if necessary
    conn = get_db_connection()  # Retrieve the current database connection
    update_db_schema(conn)  # Update the schema with any new or missing columns
    conn.close()  # Close the connection after updating schema

    base_url = "https://dallas.tx.publicsearch.us/results?_docTypes=DT&_recordedYears=2020-Present&department=RP&recordedDateRange=18000101%2C20240426&searchOcrText=false&searchType=quickSearch&searchValue=deed%20of%20trust&sort=desc&sortBy=recordedDate"

    fetch_all_pages(base_url)  # Fetch and process all pages

    overall_end = time.time()
    print(f"Total execution time: {overall_end - overall_start:.2f} seconds.")
    
    pass
    
if __name__ == "__main__":
#    main()
    cProfile.run('main()')

