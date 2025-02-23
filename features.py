import requests
import pandas as pd
import time
import logging
import threading
from geopy.distance import geodesic
from tqdm import tqdm
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
RADIUS = 500  # Radius in meters
OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
MAX_RETRIES = 3
BACKOFF_FACTOR = 10  # seconds
REQUEST_DELAY = 2  # fixed delay between API calls (in seconds)

# Cache to store API responses based on (lat, lon, date)
amenities_cache = {}

# Global lock to serialize API requests
api_lock = threading.Lock()

def calc_dist(pt1, pt2):
    """Calculate the distance between two points in meters."""
    return geodesic(pt1, pt2).meters

def get_amenities(lat, lon, radius=RADIUS, date=None):
    """Query Overpass API for amenities within a given radius, with retry, caching, and throttling."""
    key = (lat, lon, date)
    if key in amenities_cache:
        return amenities_cache[key]

    # Build query
    if date:
        query = f"""
        [out:json][date:"{date}"];
        (
            node["highway"="bus_stop"](around:{radius},{lat},{lon});
            way["railway"="station"](around:{radius},{lat},{lon});
            node["railway"="station"](around:{radius},{lat},{lon});
            way["shop"="mall"](around:{radius},{lat},{lon});
            way["amenity"="shopping_mall"](around:{radius},{lat},{lon});
            way["amenity"="school"](around:{radius},{lat},{lon});
        );
        out body geom;
        """
    else:
        query = f"""
        [out:json];
        (
            node["highway"="bus_stop"](around:{radius},{lat},{lon});
            way["railway"="station"](around:{radius},{lat},{lon});
            node["railway"="station"](around:{radius},{lat},{lon});
            way["shop"="mall"](around:{radius},{lat},{lon});
            way["amenity"="shopping_mall"](around:{radius},{lat},{lon});
            way["amenity"="school"](around:{radius},{lat},{lon});
        );
        out body geom;
        """

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with api_lock:
                response = requests.get(OVERPASS_API_URL, params={"data": query}, timeout=60)
                response.raise_for_status()
                # Wait a fixed delay to throttle subsequent requests
                time.sleep(REQUEST_DELAY)
            data = response.json()
            amenities_cache[key] = data
            return data
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt} failed for location ({lat}, {lon}) with date {date}: {e}")
            time.sleep(BACKOFF_FACTOR * attempt)
    logging.error(f"All {MAX_RETRIES} attempts failed for location ({lat}, {lon}) with date {date}.")
    return None

def process_amenities(data, lat, lon):
    """Process amenities data to calculate distances and counts."""
    stops = []
    stations = []
    malls = []
    schools = []
    
    for element in data.get("elements", []):
        tags = element.get("tags", {})
        
        # Get coordinates
        if element["type"] == "node":
            point_lat = element["lat"]
            point_lon = element["lon"]
        elif "geometry" in element:
            coords = element["geometry"]
            point_lat = sum(p["lat"] for p in coords) / len(coords)
            point_lon = sum(p["lon"] for p in coords) / len(coords)
        else:
            continue
        
        # Calculate distance
        dist = calc_dist((lat, lon), (point_lat, point_lon))
        
        # Categorize amenities
        if tags.get("highway") == "bus_stop":
            stops.append({"name": tags.get("name", "Unnamed"), "dist": dist})
        elif tags.get("railway") == "station":
            stations.append({"name": tags.get("name", "Unnamed"), "dist": dist})
        elif tags.get("shop") == "mall" or tags.get("amenity") == "shopping_mall":
            malls.append({"name": tags.get("name", "Unnamed"), "dist": dist, "addr": tags.get("addr:street", "No address")})
        elif tags.get("amenity") == "school":
            schools.append({"name": tags.get("name", "Unnamed"), "dist": dist, "addr": tags.get("addr:street", "No address")})
    
    # Sort by distance
    stops.sort(key=lambda x: x["dist"])
    stations.sort(key=lambda x: x["dist"])
    malls.sort(key=lambda x: x["dist"])
    schools.sort(key=lambda x: x["dist"])
    
    return {
        "stops": stops,
        "stations": stations,
        "malls": malls,
        "schools": schools,
    }

def get_amenity_distances_and_counts(lat, lon, date=None):
    """Get distances and counts for all amenities, optionally for a historical date."""
    data = get_amenities(lat, lon, date=date)
    if not data:
        return None
    
    amenities = process_amenities(data, lat, lon)
    
    return {
        "nearest_bus_stop_distance": amenities["stops"][0]["dist"] if amenities["stops"] else None,
        "nearest_mrt_distance": amenities["stations"][0]["dist"] if amenities["stations"] else None,
        "nearest_mall_distance": amenities["malls"][0]["dist"] if amenities["malls"] else None,
        "nearest_school_distance": amenities["schools"][0]["dist"] if amenities["schools"] else None,
        "bus_stop_count": len(amenities["stops"]),
        "mrt_count": len(amenities["stations"]),
        "mall_count": len(amenities["malls"]),
        "school_count": len(amenities["schools"]),
    }

def process_row(row, new_columns):
    """Process a single row to fetch amenity features."""
    lat = row["Latitude"]
    lon = row["Longitude"]
    
    # Extract the date from the "month" column if it exists
    date = None
    if "month" in row and pd.notna(row["month"]):
        try:
            date = datetime.strptime(row["month"], "%Y-%m").strftime("%Y-%m-01T00:00:00Z")
        except ValueError:
            logging.warning(f"Invalid date format for row with Latitude {lat} and Longitude {lon}.")
    
    if pd.notna(lat) and pd.notna(lon):
        result = get_amenity_distances_and_counts(lat, lon, date=date)
        if result:
            return result
    
    # Return a default dictionary if processing fails
    return {col: None for col in new_columns}

def process_file(input_file, output_file):
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        logging.error(f"Error reading input file: {e}")
        return
    
    # Define new columns and initialize with None
    new_columns = {
        "nearest_bus_stop_distance": None,
        "nearest_mrt_distance": None,
        "nearest_mall_distance": None,
        "nearest_school_distance": None,
        "bus_stop_count": None,
        "mrt_count": None,
        "mall_count": None,
        "school_count": None,
    }
    for col in new_columns:
        df[col] = None

    results = [None] * len(df)
    # Use ThreadPoolExecutor with a low number of workers to further reduce parallelism
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_index = {
            executor.submit(process_row, row, new_columns): idx
            for idx, row in df.iterrows()
        }
        for future in tqdm(as_completed(future_to_index), total=len(future_to_index), desc="Processing rows"):
            idx = future_to_index[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                logging.error(f"Error processing row {idx}: {e}")
                results[idx] = {col: None for col in new_columns}
    
    # Update DataFrame with results
    for idx, res in enumerate(results):
        if res:
            for col in new_columns:
                df.at[idx, col] = res.get(col)
    
    try:
        df.to_csv(output_file, index=False)
        logging.info(f"Results saved to {output_file}")
    except Exception as e:
        logging.error(f"Error saving output file: {e}")
        
if __name__ == "__main__":
    # Update the input_file and output_file paths as needed.
    input_file = "C://Users//Chee Xuan Yang//SIT//y2_tri2//machine_learning//TampinesResaleFlat_with_LatLong.csv"
    output_file = "C://Users//Chee Xuan Yang//SIT//y2_tri2//machine_learning//TampinesResaleFlat_with_LatLong_with_Features.csv"
    process_file(input_file, output_file)
