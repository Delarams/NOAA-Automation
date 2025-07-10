# Program that exracts and decodes the relevant cloud and smoke data from the filtered stations

import os
import re
import pandas as pd

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

# MUST CHANGE VALUE BASED ON STATE/PROVINCE
INPUT_BASE = "Hawaii_filter"
# MUST CHANGE VALUE BASED ON STATE/PROVINCE
OUTPUT_BASE = "Hawaii_extracted"
os.makedirs(OUTPUT_BASE, exist_ok=True)

# Cloud/Weather decoding functions 
def getCloudInfo(rem):
    if pd.isna(rem): return None
    matches = re.findall(r'\b(FEW|SCT|BKN|OVC)(\d{3})\b', rem)
    layers = [f"{typ} at {int(height)*100}ft" for typ, height in matches]
    if not layers:
        match = re.search(r'\b(SKC|CLR|NSC)\b', rem)
        if match: return match.group(1)
        return None
    return ", ".join(layers)

def mw1(mw1_value):
    if pd.isna(mw1_value): return None
    first_code = str(mw1_value).strip().split(',')[0].zfill(2)
    return mw1_legend.get(first_code, "Unknown")

def detect_smoke(rem):
    if pd.isna(rem): return "No"
    return "Yes" if "FU" in rem else "No"

def extract_total_cloud_cover(rem):
    if pd.isna(rem) or not rem.startswith("SYN"): return None
    groups = rem.split()
    if len(groups) >= 3:
        N = groups[2][0]
        return gd1_legend.get(N, f"Unknown N: {N}")
    return None

def extract_cloud_types(rem):
    if pd.isna(rem) or not rem.startswith("SYN"): return None
    groups = rem.split()
    try:
        before_333 = [g for g in groups[3:] if g != "333"]
        cloud_group = next((g for g in before_333 if g.startswith("8") and len(g) == 5), None)
        if cloud_group:
            _, Nh, CL, CM, CH = cloud_group
            return f"Nh: {cloud_8NhCLCMCH_legend['N'].get(Nh, f'Unknown Nh: {Nh}')}, " \
                   f"CL: {cloud_8NhCLCMCH_legend['CL'].get(CL, f'Unknown CL: {CL}')}, " \
                   f"CM: {cloud_8NhCLCMCH_legend['CM'].get(CM, f'Unknown CM: {CM}')}, " \
                   f"CH: {cloud_8NhCLCMCH_legend['CH'].get(CH, f'Unknown CH: {CH}')}"
    except Exception:
        return None
    return None

def extract_cloud_base_height(rem):
    if pd.isna(rem) or not rem.startswith("SYN"): return None
    groups = rem.split()
    if len(groups) >= 2 and len(groups[1]) >= 3:
        h = groups[1][2]
        return {
            '0': "0-100 ft", '1': "100-300 ft", '2': "300-600 ft", '3': "600-900 ft",
            '4': "900-1900 ft", '5': "1900-3200 ft", '6': "3200-4900 ft", '7': "4900-6500 ft",
            '8': "6500-8000 ft", '9': "8000+ ft", '/': "Unknown"
        }.get(h, f"Unknown h: {h}")
    return None

def extract_synop_weather(rem):
    if pd.isna(rem) or not rem.startswith("SYN"): return None
    groups = rem.split()
    try:
        before_333 = []
        for group in groups[3:]:
            if group == "333": break
            before_333.append(group)
        weather_group = next((g for g in before_333 if g.startswith("7") and len(g) == 5), None)
        if weather_group:
            ww, W1, W2 = weather_group[1:3], weather_group[3], weather_group[4]
            return f"Present: {present_weather_legend.get(ww, f'Unknown ww: {ww}')}, " \
                   f"Past: {past_weather_legend.get(W1, f'Unknown W1: {W1}')}, {past_weather_legend.get(W2, f'Unknown W2: {W2}')}"
    except Exception:
        return None
    return None

def extract_individual_cloud_layers(rem):
    if pd.isna(rem) or not rem.startswith("SYN"): return None
    groups = rem.split()
    try:
        if "333" in groups:
            after_333 = groups[groups.index("333") + 1:]
            cloud_layers = []
            for g in after_333:
                if g.startswith("8") and len(g) == 5:
                    Ns, C, hs = g[1], g[2], g[3:]
                    cloud_layers.append(f"Ns: {cloud_amount_legend.get(Ns, f'Unknown Ns: {Ns}')}, "
                                        f"C: {cloud_type_legend.get(C, f'Unknown C: {C}')}, "
                                        f"hs: {cloud_height_legend.get(hs, f'Unknown hs: {hs}')}")
            return " | ".join(cloud_layers) if cloud_layers else None
    except Exception:
        return None
    return None



def process_station_folder(station_folder):
    station_id = os.path.basename(station_folder)
    output_folder = os.path.join(OUTPUT_BASE, f"{station_id}_extracted")
    os.makedirs(output_folder, exist_ok=True)

    for file in os.listdir(station_folder):
        if file.endswith(".csv"):
            input_path = os.path.join(station_folder, file)
            try:
                df = pd.read_csv(input_path)
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

                output_file = os.path.join(output_folder, file.replace(".csv", "_extracted.csv"))
                df_final.to_csv(output_file, index=False)
                print(f"Extracted: {output_file}")
            except Exception as e:
                print(f"Failed to process {input_path}: {e}")

def main():
    print("Scanning for station folders...")
    for folder in os.listdir(INPUT_BASE):
        station_path = os.path.join(INPUT_BASE, folder)
        if os.path.isdir(station_path):
            print(f"Processing station: {folder}")
            process_station_folder(station_path)

if __name__ == "__main__":
    main()
