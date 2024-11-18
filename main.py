import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlparse
import os
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderInsufficientPrivileges
import re
from geopy.exc import GeocoderUnavailable


# Function to scrape all property URLs from a given page
def scrape_property_urls(page_url):
    response = requests.get(page_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all article elements with class "property-item"
    property_articles = soup.find_all('article', class_='property-item clearfix')
    
    property_urls = []
    for article in property_articles:
        # Find the first <a> tag inside <h4> tag
        url = article.find('h4').find('a')['href']
        property_urls.append(url)
    
    return property_urls

# Function to get the total number of pages
def get_total_pages(starting_url):
    response = requests.get(starting_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the pagination div and get the last page number
    pagination = soup.find('div', class_='pagination')
    last_page_url = pagination.find_all('a')[-1]['href']
    
    # Extract the page number from the last page URL
    total_pages = int(last_page_url.split('/page/')[1].split('/')[0])
    return total_pages

# Function to get latitude and longitude from an address
def get_lat_long(address, retries=3):
    try:
        geolocator = Nominatim(user_agent="my_unique_app_123")
        location = geolocator.geocode(address, timeout=10)
        
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except (GeocoderTimedOut, GeocoderUnavailable, ConnectionError) as e:
        if retries > 0:
            time.sleep(1)
            return get_lat_long(address, retries - 1)
        else:
            print(f"Error: {e}")
            return None, None

# Function to scrape property details from a given property URL
def scrape_property_details(property_url):
    response = requests.get(property_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Scraping details
    name = soup.find('meta', attrs={'property': 'og:title'})['content']

    description_element = soup.find('div', class_='content clearfix')
    description = description_element.get_text(" ", strip=True) if description_element else None


    address_element = soup.find('nav', class_='property-breadcrumbs')
    address = address_element.find_all('a')[-1].get_text(strip=True) if address_element else None

    
    # Scrape price
    price_element = soup.find('h5', class_='price')
    price = price_element.get_text(strip=True) if price_element else None
    
    # Scrape transaction type (For Sale or Rent)
    transaction_type = soup.find('span', class_='status-label').get_text(strip=True)
    
    # Extract details from the name
    property_type = None
    price_element = soup.find('h5', class_='price')  # Find the h5 tag with class "price"
    if price_element:
        # Find the <small> tag inside the <h5> and extract text after '-'
        small_element = price_element.find('small')
        if small_element:
            property_type = small_element.get_text(strip=True).split('-')[-1].strip()
    # Scrape characteristics
    characteristics = []
    property_meta = soup.find('div', class_='property-meta clearfix')
    if property_meta:
        for characteristic in property_meta.find_all('span'):
            characteristics.append(characteristic.get_text(strip=True))
    characteristics = ', '.join(characteristics)

    # Get latitude and longitude from the address
    latitude, longitude = get_lat_long(address)

    area_match = re.search(r'(\d+)\s*(?:m²|m2)', characteristics)
    area = area_match.group(1) + ' m²' if area_match else None

    return {
        'URL': property_url,
        'Name': name,
        'Description': description,
        'Address': address,
        'Price': price,
        'Area': area,
        'Property Type': property_type,
        'Transaction Type': transaction_type,
        'Characteristics': characteristics,
        'Latitude': latitude,
        'Longitude': longitude,
    }

# Function to derive the base URL from the starting URL
def derive_base_url(starting_url):
    """
    Derives the base URL for pagination.
    If the URL contains '?', pagination is added before the '?'.
    Otherwise, pagination is appended at the end.
    """
    if '?' in starting_url:
        # Insert '/page/{}/' before the '?'
        base_url = starting_url.split('?')[0] + '/page/{}/?' + starting_url.split('?')[1]
    else:
        # Append '/page/{}/' to the end
        base_url = starting_url.rstrip('/') + '/page/{}/'
    return base_url

# Main function to scrape all property URLs from all pages and then their details
def main(urls):
    # Ensure artifacts directory exists
    os.makedirs('artifacts', exist_ok=True)
    
    for starting_url in urls:
        # Derive the base URL from the starting URL
        base_url = derive_base_url(starting_url)
        
        # Get the total number of pages
        total_pages = get_total_pages(starting_url)
        
        all_property_urls = []
        
        # Iterate over all pages and scrape property URLs
        for page_num in range(1, total_pages + 1):
            page_url = base_url.format(page_num)
            print(page_url)
            property_urls = scrape_property_urls(page_url)
            all_property_urls.extend(property_urls)
            print(f'Scraped {len(property_urls)} URLs from page {page_num}')
        
        print(f'Total {len(all_property_urls)} property URLs scraped from {starting_url}')
        
        # Scrape details from each property URL
        property_details = []
        for property_url in all_property_urls:
            details = scrape_property_details(property_url)
            print(details)
            property_details.append(details)
        
        # Convert data to DataFrame
        df = pd.DataFrame(property_details)
        
        # Save DataFrame to Excel file in artifacts directory
        url_parts = urlparse(starting_url)
        filename = url_parts.netloc + url_parts.path.replace('/', '_') + '.xlsx'
        file_path = os.path.join('artifacts', filename)
        df.to_excel(file_path, index=False)
        print(f'Saved data to {file_path}')


if __name__ == "__main__":
    # Example of multiple starting URLs
    urls = [
        'https://www.immoconseilmada.com/property-search/?keyword=&location=any&status=any&type=any&bedrooms=any&bathrooms=any&min-price=any&max-price=any&min-area=&max-area=',
        # Add more URLs as needed
    ]
    
    # Call main function with the list of URLs
    main(urls)
