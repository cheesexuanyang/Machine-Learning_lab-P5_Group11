import pandas as pd
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load the dataset
file_path = "C://Users//Chee Xuan Yang//Downloads//ResaleflatpricesbasedonregistrationdatefromJan2017onwards.csv"
dataset = pd.read_csv(file_path)

# Your OneMap API token (replace with your actual token)
API_TOKEN = os.getenv("ONEMAP_API_TOKEN")

# Function to call OneMap Search API to get Latitude and Longitude
def get_lat_long(address, token):
    base_url = "https://www.onemap.gov.sg/api/common/elastic/search"
    params = {
        "searchVal": address,
        "returnGeom": "Y",
        "getAddrDetails": "Y",
        "pageNum": 1,
        "token": token
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data['found'] > 0:
            result = data['results'][0]  # Take the first result
            # Round latitude and longitude to 7 decimal places
            lat = round(float(result['LATITUDE']), 7)
            long = round(float(result['LONGITUDE']), 7)
            return lat, long
    return None, None

# Create a new column for full addresses
dataset['Full Address'] = dataset['block'].astype(str) + " " + dataset['street_name']

# Add columns for latitude and longitude
dataset['Latitude'] = None
dataset['Longitude'] = None

# Batch processing function
def process_batch(batch, token):
    results = []
    for index, row in batch.iterrows():
        address = row['Full Address']
        latitude, longitude = get_lat_long(address, token)
        print(f"Processed row {index}: {address} -> Latitude: {latitude}, Longitude: {longitude}")
        results.append((index, latitude, longitude))
    return results

# Define batch size and worker count
BATCH_SIZE = 100  # Number of rows per batch
MAX_WORKERS = 15   # Number of workers for parallel processing

# Divide the dataset into batches
batches = [dataset.iloc[i:i + BATCH_SIZE] for i in range(0, len(dataset), BATCH_SIZE)]

# Use ThreadPoolExecutor to process batches in parallel
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(process_batch, batch, API_TOKEN): batch for batch in batches}
    for future in as_completed(futures):
        try:
            results = future.result()
            for index, latitude, longitude in results:
                dataset.at[index, 'Latitude'] = latitude
                dataset.at[index, 'Longitude'] = longitude
        except Exception as e:
            print(f"Error processing batch: {e}")

# Save the updated dataset to a new file
output_file = "C://Users//Chee Xuan Yang//Downloads//ResaleFlatPrices_WithLatLong.csv"
dataset.to_csv(output_file, index=False)
print(f"Updated dataset saved to {output_file}")
