
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
    from convert_KipSutianData import convert_KipSutianData
except ImportError:
    # If standard import fails (e.g. running from root), try appending the script dir
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from convert_KipSutianData import convert_KipSutianData

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

def extract_zip_flat(zip_path, extract_to):
    """Extracts all files from a zip to the target directory, ignoring internal folder structure."""
    print(f"Extracting {zip_path} to {extract_to} (flattened)...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            members = zip_ref.infolist()
            total_files = len(members)
            extracted_count = 0
            for i, member in enumerate(members):
                if member.is_dir():
                    continue
                
                # Get just the filename, ignoring the path in the zip
                filename = os.path.basename(member.filename)
                if not filename:
                    continue
                    
                target_path = os.path.join(extract_to, filename)
                
                with zip_ref.open(member) as source, open(target_path, "wb") as target:
                    shutil.copyfileobj(source, target)
                
                extracted_count += 1
                if extracted_count % 100 == 0:
                     sys.stdout.write(f"\rExtracting: {extracted_count} files")
                     sys.stdout.flush()
            print(f"\rExtracted {extracted_count} files.")
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
        "KipSutianData.ods": URL_ODS,
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
        
        target_dir = None
        
        if not changes_detected:
            print("No changes detected in any files.")
            latest_version_dir_name = current_manifest.get("latest_version_dir")
            if latest_version_dir_name:
                target_dir = PUBLIC_DIR / latest_version_dir_name
                if target_dir.exists():
                     print(f"Using existing latest directory: {target_dir}")
                else:
                    print(f"Existing directory {latest_version_dir_name} not found. Forcing creation of new one.")
                    changes_detected = True
            else:
                print("Latest version directory not defined in manifest. Forcing creation of new one.")
                changes_detected = True

        if changes_detected:
            print("Proceeding with update/creation of new folder...")
            
            # 4. Create Timestamp Directory
            target_dir = get_timestamp_dir()
            imtong_dir = target_dir / "imtong"
            tangloo_dir = target_dir / "tangloo"
            
            imtong_dir.mkdir(parents=True, exist_ok=True)
            tangloo_dir.mkdir(parents=True, exist_ok=True)
            print(f"Created version directory: {target_dir}")

            # 5. Move/Process Files
            
            # ODS
            temp_ods = temp_dir / "KipSutianData.ods"
            tangloo_ods = tangloo_dir / "KipSutianData.ods"
            shutil.move(temp_ods, tangloo_ods)
            
            # Sutiau Zip
            temp_sutiau = temp_dir / "sutiau-mp3.zip"
            tangloo_sutiau = tangloo_dir / "sutiau-mp3.zip"
            shutil.move(temp_sutiau, tangloo_sutiau)
            
            sutiau_target_dir = imtong_dir / "sutiau"
            sutiau_target_dir.mkdir(exist_ok=True)
            extract_zip_flat(tangloo_sutiau, sutiau_target_dir)
            
            # Leku Zip
            temp_leku = temp_dir / "leku-mp3.zip"
            tangloo_leku = tangloo_dir / "leku-mp3.zip"
            shutil.move(temp_leku, tangloo_leku)

            leku_target_dir = imtong_dir / "leku"
            leku_target_dir.mkdir(exist_ok=True)
            extract_zip_flat(tangloo_leku, leku_target_dir)
                
            # 6. Update Manifest
            new_manifest = {
                "last_updated": datetime.datetime.now().isoformat(),
                "latest_version_dir": str(target_dir.name),
                "files": downloaded_hashes
            }
            save_manifest(new_manifest)
            print("Updated manifest.json.")

        # Always run ODS conversion (Bunji)
        tangloo_ods = target_dir / "tangloo" / "KipSutianData.ods"
        if not tangloo_ods.exists():
             print(f"Error: ODS file not found at {tangloo_ods}")
             return

        bunji_dir = target_dir / "bunji"
        bunji_dir.mkdir(parents=True, exist_ok=True)

        print(f"Converting ODS to CSV/JSON in {bunji_dir}...")
        csv_path = bunji_dir / "KipSutianData.csv"
        json_path = bunji_dir / "KipSutianData.json"
        
        # We need to make sure convert_KipSutianData is imported and functional
        convert_KipSutianData(str(tangloo_ods), str(csv_path), str(json_path))
        
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

    print("\nUpdate complete!")
    print(f"Data stored in: {target_dir}")

if __name__ == "__main__":
    main()
