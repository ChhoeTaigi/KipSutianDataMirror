# GEMINI.md

This file provides guidance to Gemini CLI when working with code in this repository.

## Project Overview

Data mirror for the Taiwan Ministry of Education's Taiwanese Hokkien Dictionary (教育部臺灣台語常用詞辭典, sutian.moe.edu.tw). Part of the ChhoeTaigi project. The repo downloads dictionary data (ODS spreadsheet + MP3 audio zips), converts the ODS to CSV/JSON, and hosts everything via GitHub Pages.

Licensed under CC BY-ND 3.0 TW — the data cannot be modified (derivative works prohibited).

## Commands

```bash
# Activate the Python 3.12 venv
source .venv/bin/activate

# Run the full update pipeline (downloads from sutian.moe.edu.tw, checks for changes via SHA256 hashes, extracts audio, converts ODS → CSV/JSON)
cd script && python update_data.py

# Run only the ODS → CSV/JSON conversion
cd script && python convert_KipSutianData.py
```

Dependencies: `requests`, `pandas`, `odfpy`. Install with `pip install requests pandas odfpy`.

## Architecture

### Data Pipeline (`script/`)

1. **`update_data.py`** — Main entry point. Downloads three files from sutian.moe.edu.tw (KipSutianData.ods, sutiau-mp3.zip, leku-mp3.zip), compares SHA256 hashes against `public/manifest.json` to detect changes, creates a timestamped version directory under `public/`, extracts MP3s flat (no subdirectories), then calls `convert_KipSutianData()`.

2. **`convert_KipSutianData.py`** — Converts the ODS spreadsheet into hierarchical JSON and flat CSV. The ODS has ~15 sheets representing a relational structure: main entity `詞目` (entries) → `義項` (definitions) → `例句` (example sentences), plus many relation sheets (synonyms, antonyms, variant readings, etc.). The converter builds a tree (entries containing definitions containing sentences and relations), outputs that as JSON, then flattens it to one-row-per-entry CSV with aggregated numbered fields.

### Data Layout (`public/`)

```
public/
  manifest.json              # Tracks latest version and file hashes
  {YYYYMMDD-HHMM}/           # Timestamped version directory
    bunji/                    # Converted text data (CSV + JSON)
    imtong/                   # Extracted audio
      sutiau/                 # Entry pronunciation MP3s
      leku/                   # Example sentence MP3s
    tangloo/                  # Original downloads (ODS, zips) — gitignored
```

### Hosting

- `index.md` is the GitHub Pages landing page
- `.nojekyll` disables Jekyll processing
- Audio files are served directly from `public/{version}/imtong/`

## Important Rules

- **`README.md` and `index.md` must stay in sync.** They have identical content. When updating version references or any other content in one, always update the other to match.

## Terminology (Taiwanese Hokkien)

- **bunji** (文字) — text data
- **imtong** (音通) — audio
- **tangloo** (檔路) — file storage / archives
- **sutiau** (詞條) — dictionary entries
- **leku** (例句) — example sentences
- **KipSutianData** (教育部辭典資料) — dictionary data
