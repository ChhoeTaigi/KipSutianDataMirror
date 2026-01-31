
import os
import requests
import zipfile
import datetime
import shutil
import hashlib
import json
import sys
from pathlib import Path

try:
    from convert_kautian import convert_kautian
except ImportError:
    # If standard import fails (e.g. running from root), try appending the script dir
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from convert_kautian import convert_kautian

# Constants
URL_ODS = "https://sutian.moe.edu.tw/media/senn/ods/kautian.ods"
URL_SUTIAU_ZIP = "https://sutian.moe.edu.tw/media/senn/sutiau-mp3.zip"
URL_LEKU_ZIP = "https://sutian.moe.edu.tw/media/senn/leku-mp3.zip"

BASE_DIR = Path(__file__).resolve().parent.parent
PUBLIC_DIR = BASE_DIR / "public"
MANIFEST_FILE = PUBLIC_DIR / "manifest.json"

def get_timestamp_dir():
    """Generates a timestamped directory path."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M")
    return PUBLIC_DIR / timestamp

def download_file(url, target_path):
    """Downloads a file from a URL to a target path with progress indication."""
    print(f"Downloading {url} to {target_path}...")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_length = r.headers.get('content-length')
            
            with open(target_path, 'wb') as f:
                if total_length is None: # no content length header
                    f.write(r.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    for data in r.iter_content(chunk_size=4096):
                        dl += len(data)
                        f.write(data)
                        done = int(50 * dl / total_length)
                        sys.stdout.write(f"\r[{'=' * done}{' ' * (50-done)}] {dl/1024/1024:.2f} MB")
                        sys.stdout.flush()
            print() # Newline after progress bar
    except Exception as e:
        print(f"\nError downloading {url}: {e}")
        return False
    return True

def calculate_file_hash(filepath):
    """Calculates the SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def extract_zip(zip_path, extract_to):
    """Extracts a zip file to the target directory."""
    print(f"Extracting {zip_path} to {extract_to}...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            members = zip_ref.namelist()
            total_files = len(members)
            for i, member in enumerate(members):
                zip_ref.extract(member, extract_to)
                if i % 100 == 0:
                     sys.stdout.write(f"\rExtracting: {i}/{total_files} files")
                     sys.stdout.flush()
            print(f"\rExtracted {total_files} files.")
    except Exception as e:
        print(f"Error extracting {zip_path}: {e}")
        return False
    return True

def load_manifest():
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_manifest(data):
    with open(MANIFEST_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def main():
    print("Starting update check...")
    
    # 1. Setup Temp Directory
    temp_dir = PUBLIC_DIR / "temp_update"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    files_to_check = {
        "kautian.ods": URL_ODS,
        "sutiau-mp3.zip": URL_SUTIAU_ZIP,
        "leku-mp3.zip": URL_LEKU_ZIP
    }
    
    downloaded_hashes = {}
    
    try:
        # 2. Download all files to temp
        for filename, url in files_to_check.items():
            temp_path = temp_dir / filename
            if not download_file(url, temp_path):
                print(f"Failed to download {filename}. Aborting.")
                shutil.rmtree(temp_dir)
                return
            downloaded_hashes[filename] = calculate_file_hash(temp_path)
            
        # 3. Check Manifest
        current_manifest = load_manifest()
        current_hashes = current_manifest.get("files", {})
        
        changes_detected = False
        if not current_hashes:
            print("No existing manifest found. Proceeding with initial update...")
            changes_detected = True
        else:
            for filename, new_hash in downloaded_hashes.items():
                old_hash = current_hashes.get(filename)
                if new_hash != old_hash:
                    print(f"Change detected in {filename}.")
                    changes_detected = True
                    break
        
        if not changes_detected:
            print("No changes detected in any files. All hashes match manifest.")
            print("Clean up and exit.")
            shutil.rmtree(temp_dir)
            return

        print("Proceeding with update...")
        
        # 4. Create Timestamp Directory
        target_dir = get_timestamp_dir()
        bunji_dir = target_dir / "bunji"
        imtong_dir = target_dir / "imtong"
        
        bunji_dir.mkdir(parents=True, exist_ok=True)
        imtong_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created version directory: {target_dir}")

        # 5. Move/Process Files
        
        # ODS
        temp_ods = temp_dir / "kautian.ods"
        target_ods = bunji_dir / "kautian.ods"
        shutil.move(temp_ods, target_ods)
        
        # Convert ODS
        print("Converting ODS to CSV/JSON...")
        csv_path = bunji_dir / "kautian.csv"
        json_path = bunji_dir / "kautian.json"
        convert_kautian(str(target_ods), str(csv_path), str(json_path))

        # Sutiau Zip
        temp_sutiau = temp_dir / "sutiau-mp3.zip"
        sutiau_target_dir = imtong_dir / "sutiau"
        sutiau_target_dir.mkdir(exist_ok=True)
        if extract_zip(temp_sutiau, sutiau_target_dir):
            os.remove(temp_sutiau)
        
        # Leku Zip
        temp_leku = temp_dir / "leku-mp3.zip"
        leku_target_dir = imtong_dir / "leku"
        leku_target_dir.mkdir(exist_ok=True)
        if extract_zip(temp_leku, leku_target_dir):
            os.remove(temp_leku)
            
        # 6. Update Manifest
        new_manifest = {
            "last_updated": datetime.datetime.now().isoformat(),
            "latest_version_dir": str(target_dir.name),
            "files": downloaded_hashes
        }
        save_manifest(new_manifest)
        print("Updated manifest.json.")

        # 7. Update Current Reference (public/bunji)
        print("Updating current reference files in public/bunji/...")
        current_bunji_dir = PUBLIC_DIR / "bunji"
        current_bunji_dir.mkdir(exist_ok=True)
        
        for filename in ["kautian.ods", "kautian.csv", "kautian.json"]:
            src = bunji_dir / filename
            dst = current_bunji_dir / filename
            if src.exists():
                shutil.copy2(src, dst)
        
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

    print("\nUpdate complete!")
    print(f"Data stored in: {target_dir}")

if __name__ == "__main__":
    main()
