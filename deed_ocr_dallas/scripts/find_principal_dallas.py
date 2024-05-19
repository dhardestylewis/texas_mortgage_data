import os
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from PIL import Image
import cv2
import pytesseract
from datetime import datetime
import pandas as pd

def extract_image_urls(url):
    response = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Referer": "https://dallas.tx.publicsearch.us/search/property",
        "Cookie": "authToken=da37f420-93d0-47fa-8353-7265132f535d; authToken.sig=JLCqSHZdGalrtnzqw4WzFmR8Dj4"
    }, verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    image_url = soup.select_one('image[xlink:href]')['xlink:href']
    base_url = image_url[:-6]  # Remove the "_1.png" suffix
    return [f"{base_url}_{i}.png" for i in range(1, 3)]  # Generate URLs for the first two pages

def download_image(url):
    response = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Referer": "https://dallas.tx.publicsearch.us/search/property",
        "Cookie": "authToken=da37f420-93d0-47fa-8353-7265132f535d; authToken.sig=JLCqSHZdGalrtnzqw4WzFmR8Dj4"
    }, verify=False)
    return response.content

def ocr_image(image_data):
    image = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)
    ocr_text = pytesseract.image_to_string(image, lang='eng', config='--psm 6')
    pattern = r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?'
    matches = re.findall(pattern, ocr_text)
    max_dollar_value = max(float(match.replace('$', '').replace(',', '')) for match in matches) if matches else None
    return max_dollar_value

def extract_data(url):
    response = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Referer": "https://dallas.tx.publicsearch.us/search/property",
        "Cookie": "authToken=da37f420-93d0-47fa-8353-7265132f535d; authToken.sig=JLCqSHZdGalrtnzqw4WzFmR8Dj4"
    }, verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')

    document_number = soup.select_one('span[data-testid="docPreviewSummaryItemValue"]:nth-of-type(1)').text.strip()
    instrument_date = datetime.strptime(soup.select_one('span[data-testid="docPreviewSummaryItemValue"]:nth-of-type(5)').text.strip(), '%m/%d/%Y')
    legal_description = soup.select_one('a.doc-preview-group__summary-group-text-full').text.strip()

    lot_block_text = soup.select_one('span.doc-preview-group__summary-group-label').text.strip()
    lot_block_parts = lot_block_text.split('/')
    lot = lot_block_parts[0]
    block = lot_block_parts[1] if len(lot_block_parts) > 1 else None
    block_parts = block.split('/') if block else []
    block_b = block_parts[0] if block_parts else None
    block_c = block_parts[1] if len(block_parts) > 1 else None
    block_d = lot_block_parts[3] if len(lot_block_parts) > 3 else None

    return {
        'document_number': document_number,
        'instrument_date': instrument_date,
        'legal_description': legal_description,
        'lot': lot,
        'block_b': block_b,
        'block_c': block_c,
        'block_d': block_d
    }

def process_row(row_url):
    doc_url = row_url.replace('/results', '/doc')
    image_urls = extract_image_urls(doc_url)
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        image_data_list = list(executor.map(download_image, image_urls))
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        dollar_values = list(executor.map(ocr_image, image_data_list))
    
    max_dollar_value = max(dollar_values)
    data = extract_data(doc_url)
    data['max_dollar_value'] = max_dollar_value
    return data

base_url = 'https://dallas.tx.publicsearch.us/results?_docTypes=DT&_recordedYears=2020-Present&department=RP&limit=250&offset=0&recordedDateRange=18000101%2C20240426&searchOcrText=false&searchType=quickSearch&searchValue=deed%20of%20trust&sort=desc&sortBy=recordedDate'
response = requests.get(base_url, headers={
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Referer": "https://dallas.tx.publicsearch.us/search/property",
    "Cookie": "authToken=da37f420-93d0-47fa-8353-7265132f535d; authToken.sig=JLCqSHZdGalrtnzqw4WzFmR8Dj4"
}, verify=False)
soup = BeautifulSoup(response.text, 'html.parser')
row_urls = [f"https://dallas.tx.publicsearch.us{row['data-testid']}" for row in soup.select('tr[data-testid="searchResult"]')]

with ThreadPoolExecutor(max_workers=20) as executor:
    data_list = list(executor.map(process_row, row_urls))

df = pd.DataFrame(data_list)
df.to_csv('deed_of_trust_data.csv', index=False)
