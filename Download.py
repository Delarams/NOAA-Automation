#Program that downloads isd NOAA data directly into dropbox folder based on list of stations.

import os
import requests
from tqdm import tqdm
from full_station_list import station_ids 

base_url = "https://www.ncei.noaa.gov/data/global-hourly/access"
years = list(range(1992, 2025))

# MUST CHANGE PATH LUE BASED ON STATE/PROVINCE
output_folder = "/Users/delaram/Dropbox/Kansas_isd_data"
os.makedirs(output_folder, exist_ok=True)


def download_data(station_id, year):
    filename = f"{station_id}_{year}.csv"
    station_folder = os.path.join(output_folder, station_id)
    os.makedirs(station_folder, exist_ok=True)
    filepath = os.path.join(station_folder, filename)



    if os.path.exists(filepath):
        return f"Skipped {station_id} ({year}) — already exists"

    url = f"{base_url}/{year}/{station_id}.csv"

    try:
        response = requests.get(url, timeout=20)
        if response.status_code == 200:
            with open(filepath, "wb") as f:
                f.write(response.content)
            return f" Downloaded {station_id} ({year})"
        else:
            return f" No data for {station_id} ({year}) — {response.status_code}"
    except Exception as e:
        return f" Failed for {station_id} ({year}) — {str(e)}"

# Run download loop
for station in tqdm(station_ids, desc="Downloading NOAA ISD Data"):
    for year in years:
        print(download_data(station, year))
