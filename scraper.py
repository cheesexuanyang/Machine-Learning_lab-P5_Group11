import pandas as pd
import requests

# Load the dataset
file_path = "C://Users//Chee Xuan Yang//Downloads//ResaleflatpricesbasedonregistrationdatefromJan2017onwards.csv" # Update with your file path
dataset = pd.read_csv(file_path)

# Your OneMap API token (replace with your actual token)
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI4YmFiNzEzZTE0MDRlM2MxMDYwNDJkMDhkYWU4ZTU4OCIsImlzcyI6Imh0dHA6Ly9pbnRlcm5hbC1hbGItb20tcHJkZXppdC1pdC1uZXctMTYzMzc5OTU0Mi5hcC1zb3V0aGVhc3QtMS5lbGIuYW1hem9uYXdzLmNvbS9hcGkvdjIvdXNlci9wYXNzd29yZCIsImlhdCI6MTczNzQ1ODU5NywiZXhwIjoxNzM3NzE3Nzk3LCJuYmYiOjE3Mzc0NTg1OTcsImp0aSI6IldBOVFDTnpTSzlQdjVicmEiLCJ1c2VyX2lkIjo1NjE2LCJmb3JldmVyIjpmYWxzZX0.4t3zmU8m6Wl4NXT10bIWnk2S4ImDHNMq55JB_GOkr1I"

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

# Function to call OneMap Reverse Geocoding API for nearby features
def get_nearby_features(lat, long, token):
    base_url = "https://www.onemap.gov.sg/api/public/revgeocode"
    params = {
        "location": f"{lat},{long}",
        "buffer": 500,
        "addressType": "All",
        "otherFeatures": "Y"
    }
    # Include token in the request headers
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    # Debug: Print the URL and headers
    print(f"Formatted URL: {base_url}")
    print(f"Headers: {headers}")
    
    # Make the API call
    response = requests.get(base_url, params=params, headers=headers)
    print(f"Response Status Code: {response.status_code}")
    if response.status_code == 200:
        print(f"Response JSON: {response.json()}")
        data = response.json()
        return data.get('GeocodeInfo', [])
    else:
        print(f"Error: API call failed with status code {response.status_code}")
        print(f"Error Message: {response.text}")
    return []

# Create a new column for full addresses
dataset['Full Address'] = dataset['block'].astype(str) + " " + dataset['street_name']

# Add columns for latitude, longitude, and nearby features
dataset['Latitude'] = None
dataset['Longitude'] = None
dataset['Nearby Features'] = None

# Limit processing to the first 10 rows for demonstration
limited_dataset = dataset.head(10)

# Fetch latitude, longitude, and nearby features for each row
for index, row in limited_dataset.iterrows():
    address = row['Full Address']
    latitude, longitude = get_lat_long(address, API_TOKEN)
    dataset.at[index, 'Latitude'] = latitude
    dataset.at[index, 'Longitude'] = longitude
    
    if latitude and longitude:
        # Round again before calling reverse geocoding (to ensure consistency)
        latitude = round(latitude, 7)
        longitude = round(longitude, 7)
        nearby_features = get_nearby_features(latitude, longitude, API_TOKEN)
        dataset.at[index, 'Nearby Features'] = nearby_features
    
    print(f"Processed {index + 1}/{len(limited_dataset)}: {address} -> {latitude}, {longitude}")

# Save the updated dataset to a new file
output_file = "C://Users//Chee Xuan Yang//Downloads//ResaleFlatPrices_WithLatLong_and_NearbyFeatures.csv"
dataset.to_csv(output_file, index=False)
print(f"Updated dataset saved to {output_file}")
