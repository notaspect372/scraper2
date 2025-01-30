
import requests
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import pandas as pd
import time
import os


OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_property_urls(soup):
    # Find all 'a' tags with the specific class used for property links
    return [a['href'] for a in soup.find_all('a', class_='btn btn-primary btn-item')]

def build_next_page_url(base_url, page_num):
    if '?' in base_url:
        # If there are query parameters, insert the pagination before them
        base, query_params = base_url.split('?', 1)
        return f"{base}/page/{page_num}/?{query_params}"
    else:
        # If no query parameters, just append pagination to the URL
        return f"{base_url}/page/{page_num}/"


def extract_characteristics(soup):
    characteristics = {}
    detail_wrap = soup.find('div', class_='detail-wrap')
    if detail_wrap:
        for li in detail_wrap.find_all('li'):
            label = li.find('strong').get_text(strip=True).rstrip(':')
            value = li.find('span').get_text(strip=True)
            characteristics[label] = value
    return characteristics

def get_location(geolocator, address, retries=3):
    for i in range(retries):
        try:
            return geolocator.geocode(address, timeout=10)
        except GeocoderTimedOut:
            if i < retries - 1:
                time.sleep(2)
                continue
            else:
                return None



def scrape_property_details(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        name = soup.find('meta', property='og:title')['content'] if soup.find('meta', property='og:title') else None
        price = soup.find('li', class_='item-price').text.strip() if soup.find('li', class_='item-price') else None

        address_details = {}
        address_wrap = soup.find('div', id='property-address-wrap')
        if address_wrap:
            address_elements = address_wrap.find_all('li')
            for element in address_elements:
                strong_tag = element.find('strong')
                span_tag = element.find('span')
                if strong_tag and span_tag:
                    key = strong_tag.text.strip().lower().replace(" ", "_")  # Convert to lowercase with underscores
                    value = span_tag.text.strip()
                    address_details[key] = value

        sub_address = address_details.get('city', None)

        description = None
        description_wrap = soup.find('div', class_='property-description-wrap property-section-wrap')
        if description_wrap:
            description_content = description_wrap.find('div', class_='block-content-wrap')
            if description_content:
                description = description_content.text.strip()
        
        # Extract characteristics
        characteristics = extract_characteristics(soup)  # Ensure this is defined before use
        
        transaction_type = characteristics.get("Property Status", "N/A")  # Now characteristics will be defined
        property_type = characteristics.get('Property Type', None)

        latitude, longitude = None, None
        if sub_address:
            geolocator = Nominatim(user_agent="property_scraper")
            location = get_location(geolocator, sub_address)
            if location:
                latitude, longitude = location.latitude, location.longitude


        amenities = []
        amenities_wrap = soup.find('div', id='property-features-wrap')
        if amenities_wrap:
            amenities_list = amenities_wrap.find_all('li')
            for amenity in amenities_list:
                amenity_text = amenity.get_text(strip=True)
                if amenity_text:
                    amenities.append(amenity_text)

        return {
            'url': url,
            'name': name,
            'description': description,
            'price': price,
            'address': address_details,
            'transaction_type': transaction_type,
            'area': characteristics.get("Property Size", "N/A"),
            "amenities":amenities,
            'property_type': property_type,
            'latitude': latitude,
            'longitude': longitude,
            'characteristics': characteristics
        }
    except requests.exceptions.RequestException as e:
        print(f"Error scraping {url}: {e}")
        return None

def scrape_properties(base_url):
    all_property_urls = []
    page_num = 1
    consecutive_404_errors = 0

    while consecutive_404_errors < 3:
        url = build_next_page_url(base_url, page_num)
        try:
            response = requests.get(url)
            if response.status_code == 404:
                consecutive_404_errors += 1
                print(f"Encountered 404 error on {url}, count: {consecutive_404_errors}")
                continue
            else:
                consecutive_404_errors = 0

            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            property_urls = get_property_urls(soup)
            if not property_urls:
                print(f"No property URLs found on {url}. Stopping pagination.")
                break
            print(f'Scraped {len(property_urls)} properties from {url}')
            all_property_urls.extend(property_urls)
            page_num += 1

        except requests.exceptions.RequestException as e:
            print(f"Error requesting page {url}: {e}")
            consecutive_404_errors += 1
            continue

    properties_data = []
    for property_url in all_property_urls:
        details = scrape_property_details(property_url)
        print(details)
        if details:
            properties_data.append(details)
            print(f"Scraped details for {property_url}")

    return properties_data

# List of URLs to scrape
urls = [
    "https://real-estate-tanzania.beforward.jp/status/for-sale/",

]


for base_url in urls:
    properties_data = scrape_properties(base_url)
    df = pd.DataFrame(properties_data)
    url_filename = base_url.split('/')[2].replace(".", "_")  # Creating a valid filename from URL
    file_path = os.path.join(OUTPUT_DIR, f"{url_filename}.xlsx")
    df.to_excel(file_path, index=False)

    print(f"Data saved to {file_path}")
