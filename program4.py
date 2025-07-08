import os
import re
import pandas as pd

INPUT_FOLDER = "71877099999" 
OUTPUT_BASE = "Calgary_extracted"
COLUMNS_TO_EXTRACT = ['DATE', 'cloud_layers', 'weather', 'Smoke',
                      'synop_total_cloud_cover', 'synop_cloud_types',
                      'synop_cloud_base_height', 'synop_weather_report',
                      'individual_cloud_layers']

output_folder = os.path.join(OUTPUT_BASE, f"{os.path.basename(INPUT_FOLDER)}_extracted")
os.makedirs(output_folder, exist_ok=True)

# --- Raw Extraction Functions (no legend decoding) ---

def getCloudInfo(rem):
    if pd.isna(rem): return None
    matches = re.findall(r'\b(FEW|SCT|BKN|OVC)(\d{3})\b', rem)
    return ", ".join([f"{typ}{height}" for typ, height in matches]) if matches else None

def mw1_raw(mw1_value):
    return str(mw1_value).strip().split(',')[0] if pd.notna(mw1_value) else None

def detect_smoke(rem):
    if pd.isna(rem): return None
    return "FU" if "FU" in rem else None

def extract_total_cloud_cover(rem):
    if pd.isna(rem) or not rem.startswith("SYN"): return None
    groups = rem.split()
    if len(groups) >= 3:
        return groups[2][0]  # Raw N code
    return None

def extract_cloud_types(rem):
    if pd.isna(rem) or not rem.startswith("SYN"): return None
    groups = rem.split()
    try:
        before_333 = [g for g in groups[3:] if g != "333"]
        cloud_group = next((g for g in before_333 if g.startswith("8") and len(g) == 5), None)
        return cloud_group if cloud_group else None
    except Exception:
        return None

def extract_cloud_base_height(rem):
    if pd.isna(rem) or not rem.startswith("SYN"): return None
    groups = rem.split()
    if len(groups) >= 2 and len(groups[1]) >= 3:
        return groups[1][2]  # h digit only
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
        return weather_group if weather_group else None
    except Exception:
        return None

def extract_individual_cloud_layers(rem):
    if pd.isna(rem) or not rem.startswith("SYN"): return None
    groups = rem.split()
    try:
        if "333" in groups:
            after_333 = groups[groups.index("333") + 1:]
            return " | ".join([g for g in after_333 if g.startswith("8") and len(g) == 5])
    except Exception:
        return None

# --- Processing ---

def process_csv_files(input_folder):
    for file in os.listdir(input_folder):
        if file.endswith(".csv"):
            input_path = os.path.join(input_folder, file)
            try:
                df = pd.read_csv(input_path)

                # Create new raw-extracted columns (no decoding)
                df['cloud_layers'] = df['REM'].apply(getCloudInfo)
                df['weather'] = df['MW1'].apply(mw1_raw)
                df['Smoke'] = df['REM'].apply(detect_smoke)
                df['synop_total_cloud_cover'] = df['REM'].apply(extract_total_cloud_cover)
                df['synop_cloud_types'] = df['REM'].apply(extract_cloud_types)
                df['synop_cloud_base_height'] = df['REM'].apply(extract_cloud_base_height)
                df['synop_weather_report'] = df['REM'].apply(extract_synop_weather)
                df['individual_cloud_layers'] = df['REM'].apply(extract_individual_cloud_layers)

                df_final = df[[col for col in COLUMNS_TO_EXTRACT if col in df.columns]]

                output_file = os.path.join(output_folder, file.replace(".csv", "_extracted.csv"))
                df_final.to_csv(output_file, index=False)
                print(f"Extracted: {output_file}")
            except Exception as e:
                print(f"Failed to process {input_path}: {e}")

if __name__ == "__main__":
    print(f"Processing CSV files in: {INPUT_FOLDER}")
    process_csv_files(INPUT_FOLDER)
