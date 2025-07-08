import os
import shutil


from Arkansas_filter_stations import stations


DROPBOX_ROOT = "/Users/delaram/Dropbox/Hawaii_isd_data" 
DEST_FOLDER = "Hawaii_filter"


def find_station_folders(base_path, station_list):
    matched_paths = []
    for root, dirs, _ in os.walk(base_path):
        for d in dirs:
            if d in station_list:
                full_path = os.path.join(root, d)
                matched_paths.append(full_path)
    return matched_paths

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, DEST_FOLDER)
    os.makedirs(output_dir, exist_ok=True)

    matched_folders = find_station_folders(DROPBOX_ROOT, stations)

    if not matched_folders:
        print("No matching station folders found.")
        return

    for folder_path in matched_folders:
        folder_name = os.path.basename(folder_path)
        dest_path = os.path.join(output_dir, folder_name)
        if os.path.exists(dest_path):
            print(f"Skipping existing folder: {folder_name}")
        else:
            shutil.copytree(folder_path, dest_path)
            print(f"Copied: {folder_name}")

if __name__ == "__main__":
    main()
