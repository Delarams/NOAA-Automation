# Program similar to Extract.py, works on a station name basis 

import os
import re
import pandas as pd
import requests

from legends import (
    gd1_legend,
    mw1_legend,
    cloud_8NhCLCMCH_legend,
    present_weather_legend,
    past_weather_legend,
    cloud_amount_legend,
    cloud_type_legend,
    cloud_height_legend
)

# MUST CHANGE VALUE BASED ON STATION NUMBER
STATION_ID = "71877099999"

def download_and_extract_noaa_csv(year, download_folder):
    os.makedirs(download_folder, exist_ok=True)
    url = f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/{STATION_ID}.csv"
    output_path = os.path.join(download_folder, f"{year}.csv")

    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded data to {output_path}")
        return output_path
    else:
        raise Exception(f"Failed to download NOAA data for {year}: HTTP {response.status_code}")

def getCloudInfo(rem):
    if pd.isna(rem):
        return None
    matches = re.findall(r'\b(FEW|SCT|BKN|OVC)(\d{3})\b', rem)
    layers = [f"{typ} at {int(height)*100}ft" for typ, height in matches]
    if not layers:
        match = re.search(r'\b(SKC|CLR|NSC)\b', rem)
        if match:
            return match.group(1)
        return None
    return ", ".join(layers)

def mw1(mw1_value):
    if pd.isna(mw1_value):
        return None
    first_code = str(mw1_value).strip().split(',')[0].zfill(2)
    return mw1_legend.get(first_code, "Unknown")

def detect_smoke(rem):
    if pd.isna(rem):
        return "No"
    return "Yes" if "FU" in rem else "No"

def extract_total_cloud_cover(rem):
    if pd.isna(rem) or not rem.startswith("SYN"):
        return None
    groups = rem.split()
    if len(groups) >= 3:
        nddff = groups[2]
        N = nddff[0]
        return gd1_legend.get(N, f"Unknown N: {N}")
    return None

def extract_cloud_types(rem):
    if pd.isna(rem) or not rem.startswith("SYN"):
        return None
    groups = rem.split()
    try:
        before_333 = []
        for group in groups[3:]:
            if group == "333":
                break
            before_333.append(group)
        cloud_group = next((g for g in before_333 if g.startswith("8") and len(g) == 5), None)
        if cloud_group:
            _, Nh, CL, CM, CH = cloud_group
            Nh_text = cloud_8NhCLCMCH_legend["N"].get(Nh, f"Unknown Nh: {Nh}")
            CL_text = cloud_8NhCLCMCH_legend["CL"].get(CL, f"Unknown CL: {CL}")
            CM_text = cloud_8NhCLCMCH_legend["CM"].get(CM, f"Unknown CM: {CM}")
            CH_text = cloud_8NhCLCMCH_legend["CH"].get(CH, f"Unknown CH: {CH}")
            return f"Nh: {Nh_text}, CL: {CL_text}, CM: {CM_text}, CH: {CH_text}"
    except Exception:
        return None
    return None

def extract_cloud_base_height(rem):
    if pd.isna(rem) or not rem.startswith("SYN"):
        return None
    groups = rem.split()
    if len(groups) >= 2:
        second_group = groups[1]
        if len(second_group) >= 3:
            h = second_group[2]
            height_ranges = {
                '0': "0-100 ft", '1': "100-300 ft", '2': "300-600 ft", '3': "600-900 ft",
                '4': "900-1900 ft", '5': "1900-3200 ft", '6': "3200-4900 ft", '7': "4900-6500 ft",
                '8': "6500-8000 ft", '9': "8000+ ft", '/': "Unknown"
            }
            return height_ranges.get(h, f"Unknown h: {h}")
    return None

def extract_synop_weather(rem):
    if pd.isna(rem) or not rem.startswith("SYN"):
        return None
    groups = rem.split()
    try:
        before_333 = []
        for group in groups[3:]:
            if group == "333":
                break
            before_333.append(group)
        weather_group = next((g for g in before_333 if g.startswith("7") and len(g) == 5), None)
        if weather_group:
            ww, W1, W2 = weather_group[1:3], weather_group[3], weather_group[4]
            ww_desc = present_weather_legend.get(ww, f"Unknown ww: {ww}")
            W1_desc = past_weather_legend.get(W1, f"Unknown W1: {W1}")
            W2_desc = past_weather_legend.get(W2, f"Unknown W2: {W2}")
            return f"Present: {ww_desc}, Past: {W1_desc}, {W2_desc}"
    except Exception:
        return None
    return None

def extract_individual_cloud_layers(rem):
    if pd.isna(rem) or not rem.startswith("SYN"):
        return None
    groups = rem.split()
    try:
        after_333 = []
        if "333" in groups:
            index_333 = groups.index("333")
            after_333 = groups[index_333 + 1:]
        cloud_layers = []
        for group in after_333:
            if group.startswith("8") and len(group) == 5:
                Ns, C, hs = group[1], group[2], group[3:]
                Ns_text = cloud_amount_legend.get(Ns, f"Unknown Ns: {Ns}")
                C_text = cloud_type_legend.get(C, f"Unknown C: {C}")
                hs_text = cloud_height_legend.get(hs, f"Unknown hs: {hs}")
                cloud_layers.append(f"Ns: {Ns_text}, C: {C_text}, hs: {hs_text}")
        return " | ".join(cloud_layers) if cloud_layers else None
    except Exception:
        return None

def getWeatherColumn(input_file, output_file):
    try:
        df = pd.read_csv(input_file)
        df['cloud_layers'] = df['REM'].apply(getCloudInfo)
        df['weather'] = df['MW1'].apply(mw1)
        df['Smoke'] = df['REM'].apply(detect_smoke)
        df['synop_total_cloud_cover'] = df['REM'].apply(extract_total_cloud_cover)
        df['synop_cloud_types'] = df['REM'].apply(extract_cloud_types)
        df['synop_cloud_base_height'] = df['REM'].apply(extract_cloud_base_height)
        df['synop_weather_report'] = df['REM'].apply(extract_synop_weather)
        df['individual_cloud_layers'] = df['REM'].apply(extract_individual_cloud_layers)

        df_final = df[['DATE', 'cloud_layers', 'weather', 'Smoke',
                       'synop_total_cloud_cover', 'synop_cloud_types',
                       'synop_cloud_base_height', 'synop_weather_report',
                       'individual_cloud_layers']]
        df_final.to_csv(output_file, index=False)
        print(f"Saved extracted file to {output_file}")
    except Exception as e:
        print("Error during extraction:", e)

def process_year(year):
    try:
        download_folder = "NOAA-Downloads"
        extract_folder = "NOAA-Extracted"
        os.makedirs(extract_folder, exist_ok=True)
        input_file = download_and_extract_noaa_csv(year, download_folder)
        output_file = os.path.join(extract_folder, f"{year}-Extracted.csv")
        getWeatherColumn(input_file, output_file)
    except Exception as e:
        print("Error:", e)

while True:
    year = input("Enter year (or press Enter to quit): ").strip()
    if not year:
        break
    process_year(year)
