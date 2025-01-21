import pandas as pd
import requests

# Load the dataset
file_path = "C://Users//Chee Xuan Yang//Downloads//ResaleflatpricesbasedonregistrationdatefromJan2017onwards.csv"  # Update with your file path
dataset = pd.read_csv(file_path)

# Function to call OneMap API
def get_lat_long(address):
    base_url = "https://www.onemap.gov.sg/api/common/elastic/search"
    params = {
        "searchVal": address,
        "returnGeom": "Y",
        "getAddrDetails": "Y",
        "pageNum": 1
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data['found'] > 0:
            result = data['results'][0]  # Take the first result
            return result['LATITUDE'], result['LONGITUDE']
    return None, None

# Create a new column for full addresses
dataset['Full Address'] = dataset['street_name'] + " " + dataset['block']

# Add columns for latitude and longitude
dataset['Latitude'] = None
dataset['Longitude'] = None

# Limit processing to the first 10 rows
limited_dataset = dataset.head(10)

# Fetch latitude and longitude for each row
for index, row in limited_dataset.iterrows():
    address = row['Full Address']
    latitude, longitude = get_lat_long(address)
    dataset.at[index, 'Latitude'] = latitude
    dataset.at[index, 'Longitude'] = longitude
    print(f"Processed {index + 1}/{len(limited_dataset)}: {address} -> {latitude}, {longitude}")

# Save the updated dataset to a new file
output_file = "C://Users//Chee Xuan Yang//Downloads//ResaleFlatPrices_WithLatLong_Limited.csv"
dataset.to_csv(output_file, index=False)
print(f"Updated dataset saved to {output_file}")
