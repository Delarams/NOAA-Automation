# NOAA ISD Weather Data Processing Suite

This repository contains Python scripts for automating the download, filtering, and extraction of weather data from the [NOAA Integrated Surface Dataset (ISD)](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database). The programs support both raw extraction and detailed decoding of cloud cover, weather, and smoke-related features.



##  Program Overview

### 1. `Download.py`

Downloads ISD CSV files for a given list of station IDs and years, saving them into station-specific subfolders within a specified Dropbox directory.


**Features:** 
- Uses the official NOAA API to download data.
- Skips already-downloaded files to avoid redundancy.
- Requires a `full_station_list.py` file with a `station_ids` list.

**Modify Before Use:**
- Update the `output_folder` path to match the desired state/province Dropbox folder.
- Confirm that `station_ids` is defined in the import.
- You must download the dropbox folder into your computer, or change destination to another path. 


---

### 2. `filter.py`

Copies only the station folders listed in `filter_stations.py` into a new directory for downstream filtering or processing. This program is designed to separate stations with Sky-Cover-Layer and Sky-Condition-Observation data

**Features:**
- Creates a filtered folder (e.g., `Hawaii_filter`) containing only relevant stations.

**Modify Before Use:**
- Update `DROPBOX_ROOT` to your state/province NOAA data folder.
- Set `DEST_FOLDER` to your desired output folder name.
- Ensure `stations` is defined in `filter_stations.py`.

---

### 3. `Extract.py`

Extracts and decodes meaningful weather and cloud-related information from filtered NOAA data.

**Features:**
- Parses cloud layers, present/past weather, smoke, total cloud cover, cloud base height, and individual cloud layers from REM and MW1 columns.
- Requires `legends.py` with decoding dictionaries for:
  - Cloud amount and type
  - Cloud height
  - Weather codes
- Outputs cleaned CSVs with new columns to `*_extracted` folders.

**Modify Before Use:**
- Set `INPUT_BASE` to your filtered folder (e.g., `Hawaii_filter`).
- Set `OUTPUT_BASE` to the desired output directory for extracted results.

---

### 4. `filter-automation.py`
**Purpose:**  
Extracts raw weather-related codes and groups (without decoding) from a single station folder. 

**Features:**
- Extracts REM and MW1 information including smoke indicators and SYNOP groups.
- Does not require decoding dictionaries.
- Outputs simplified versions of the files for quick analysis.

**Modify Before Use:**
- Set `INPUT_FOLDER` to the name of a single station folder (e.g., `71877099999`).
- Set `OUTPUT_BASE` to the parent folder where `_extracted` subfolder will be created.

---

##  Dependencies

All scripts require the following Python libraries:

"```bash"
"pip install pandas requests tqdm"

--- 

Also ensure your working directory contains:

full_station_list.py — defines station_ids

filter_stations.py — defines stations

legends.py — contains decoding dictionaries (for Extract.py only)
