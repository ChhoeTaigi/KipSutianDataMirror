
import pandas as pd
import json
import os
import sys
import re

# Define input/output paths
# Define input/output paths
# Define input/output paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'public')

INPUT_FILE = os.path.join(DATA_DIR, 'KipSutianData.ods')
CSV_OUTPUT = os.path.join(DATA_DIR, 'KipSutianData.csv')
JSON_OUTPUT = os.path.join(DATA_DIR, 'KipSutianData.json')

def load_sheet(excel_file, sheet_name):
    """Helper to load a sheet, returning empty DataFrame if not found or error."""
    try:
        if sheet_name in excel_file.sheet_names:
            print(f"Loading sheet: {sheet_name}...")
            return pd.read_excel(excel_file, sheet_name=sheet_name)
        else:
            print(f"Warning: Sheet '{sheet_name}' not found.")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error loading sheet '{sheet_name}': {e}")
        return pd.DataFrame()


def convert_KipSutianData(input_file=INPUT_FILE, csv_output=CSV_OUTPUT, json_output=JSON_OUTPUT):
    print(f"Reading {input_file}...")
    try:
        xl = pd.ExcelFile(input_file, engine='odf')
    except ImportError:
        print("Please install odfpy: pip install odfpy")
        return
    except Exception as e:
        print(f"Error opening file: {e}")
        return


    # 1. Load DataFrames
    # Main Entity
    df_entries = load_sheet(xl, '詞目')
    
    # Sub-entities (1-to-Many linked by 詞目id)
    df_definitions = load_sheet(xl, '義項')
    
    # Sub-entities (1-to-Many linked by 詞目id, 義項id)
    df_sentences = load_sheet(xl, '例句')
    
    # Other Entry-level relations (linked by 詞目id)
    # List of sheets that are simple lists linked to entries
    entry_relations = {
        '又唸作': '又唸作',
        '合音唸作': '合音唸作',
        '俗唸作': '俗唸作',
        '語音差異': '語音差異',
        '詞彙比較': '詞彙比較',
        '名': '名',
        '姓': '姓',
        '異用字': '異用字',
        '詞目tuì詞目近義': '詞目tuì詞目近義',
        '詞目tuì詞目反義': '詞目tuì詞目反義',
    }
    
    loaded_entry_relations = {}
    for sheet, key in entry_relations.items():
        loaded_entry_relations[key] = load_sheet(xl, sheet)

    # Definition-level relations (linked by 詞目id, 義項id)
    def_relations = {
        '義項tuì義項近義': '義項tuì義項近義',
        '義項tuì義項反義': '義項tuì義項反義',
        '義項tuì詞目近義': '義項tuì詞目近義', 
        '義項tuì詞目反義': '義項tuì詞目反義',
    }
    
    loaded_def_relations = {}
    for sheet, key in def_relations.items():
        loaded_def_relations[key] = load_sheet(xl, sheet)


    # Ensure IDs are strings to key matching work
    def normalize_id(val):
        if pd.isna(val):
            return None
        s = str(val).strip()
        if s.endswith('.0'):
            return s[:-2]
        return s

    def sanitize_ids(df, id_cols=['詞目id', '義項id']):
         for col in id_cols:
             if col in df.columns:
                 df[col] = df[col].apply(normalize_id)
         return df

    # Sanitize all loaded DataFrames
    df_entries = sanitize_ids(df_entries, ['詞目id'])
    df_definitions = sanitize_ids(df_definitions, ['詞目id', '義項id'])
    df_sentences = sanitize_ids(df_sentences, ['詞目id', '義項id'])
    
    for k in loaded_entry_relations:
        loaded_entry_relations[k] = sanitize_ids(loaded_entry_relations[k], ['詞目id'])
        
    for k in loaded_def_relations:
        loaded_def_relations[k] = sanitize_ids(loaded_def_relations[k], ['詞目id', '義項id'])

    print("Processing and Merging Data...")

    # --- Pre-process Relations for fast lookup ---
    
    # 1. Build Definition ID -> Entry ID map
    # Needed because relation sheets might miss '詞目id' but have '義項id'
    def_to_entry_map = {}
    if not df_definitions.empty:
        for _, row in df_definitions.iterrows():
            d_id = row.get('義項id')
            e_id = row.get('詞目id')
            if pd.notna(d_id) and pd.notna(e_id):
                def_to_entry_map[str(d_id)] = str(e_id)

    # 2. Backfill '詞目id' in definition relations if missing
    for key, df in loaded_def_relations.items():
        if '義項id' in df.columns and '詞目id' not in df.columns:
            print(f"Backfilling '詞目id' for {key}...")
            # We map '義項id' -> '詞目id'
            # Note: IDs are already sanitized to strings
            df['詞目id'] = df['義項id'].map(def_to_entry_map)

    # Helper to group by keys and convert to list of dicts
    def group_relation(df, group_keys, value_keys=None):
        if df.empty:
            return {}
        
        records = df.to_dict('records')
        grouped = {}
        for row in records:
            # Create a tuple key for grouping
            key = tuple(row.get(k) for k in group_keys)
            
            # If key contains None/NaN, we can't attach it to a valid entry/def usually
            if any(pd.isna(k) for k in key):
                continue

            # If value_keys provided, filter row, else take whole row (excluding group keys?)
            if value_keys:
                item = {k: row.get(k) for k in value_keys if k in row}
            else:
                item = {k: v for k, v in row.items() if k not in group_keys}
            
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(item)
        return grouped

    # Group Sentences by (詞目id, 義項id)
    sentences_map = group_relation(df_sentences, ['詞目id', '義項id'])
    
    # Group Entry Relations by (詞目id)
    entry_rel_maps = {}
    for key, df in loaded_entry_relations.items():
        entry_rel_maps[key] = group_relation(df, ['詞目id'])
        
    # Group Definition Relations by (詞目id, 義項id)
    def_rel_maps = {}
    for key, df in loaded_def_relations.items():
        def_rel_maps[key] = group_relation(df, ['詞目id', '義項id'])


    # --- Construct Hierarchical JSON Structure ---
    
    entries_list = []
    
    # We iterate over df_entries
    for _, entry_row in df_entries.iterrows():
        entry_id = entry_row.get('詞目id')
        entry_data = entry_row.to_dict()
        
        # Attach Entry-level relations
        for key, rel_map in entry_rel_maps.items():
            if (entry_id,) in rel_map:
                entry_data[key] = rel_map[(entry_id,)]
            else:
                entry_data[key] = []

        # Find Definitions for this Entry
        # Optimization: group definitions by 詞目id outside loop
        pass # implemented below
        
    # Optimization: Group Definitions by 詞目id
    definitions_map = {}
    if not df_definitions.empty:
        # Sort to ensure order? Usually definition ID helps.
        # df_definitions = df_definitions.sort_values('義項id') 
        def_records = df_definitions.to_dict('records')
        for row in def_records:
            e_id = row.get('詞目id')
            if e_id not in definitions_map:
                definitions_map[e_id] = []
            definitions_map[e_id].append(row)

    # Now build the full tree
    for _, entry_row in df_entries.iterrows():
        entry_id = entry_row.get('詞目id')
        entry_data = entry_row.to_dict()
        
        # clean up NaNs in entry_data
        entry_data = {k: (v if pd.notna(v) else None) for k, v in entry_data.items()}

        # Attach Entry relations
        for key, rel_map in entry_rel_maps.items():
            entry_data[key] = rel_map.get((entry_id,), [])

        # Attach Definitions
        defs = definitions_map.get(entry_id, [])
        processed_defs = []
        for d in defs:
            def_id = d.get('義項id')
            def_data = {k: (v if pd.notna(v) else None) for k, v in d.items()}
            
            # Attach Sentences to Definition
            def_data['例句'] = sentences_map.get((entry_id, def_id), [])
            
            # Attach Definition relations
            for key, rel_map in def_rel_maps.items():
                def_data[key] = rel_map.get((entry_id, def_id), [])
            
            processed_defs.append(def_data)
        
        entry_data['義項'] = processed_defs
        entries_list.append(entry_data)


    # --- Save JSON ---
    print(f"Saving JSON to {json_output}...")
    with open(json_output, 'w', encoding='utf-8') as f:
        json.dump(entries_list, f, ensure_ascii=False, indent=2)


    # --- Construct Flat CSV Structure ---
    # Granularity: Definition Level
    # If an entry has NO definitions (rare?), we still want a row? 
    # Let's assume right join or outer join logic.

    def format_hanji(text):
        if not text:
            return text
        return text.replace('【', '(').replace('】', ')')

    def format_lomaji(text):
        if not text:
            return text
        # e.g. "【白】tsénn/tsínn" -> "tsénn(白)/tsínn(白)"
        # e.g. "【白】gōo" -> "gōo(白)"
        match = re.match(r'^【(.*?)】(.*)$', text)
        if match:
            tag = match.group(1)
            content = match.group(2)
            # split by '/'
            parts = content.split('/')
            new_parts = [f"{p.strip()}({tag})" for p in parts]
            return "/".join(new_parts)
        return text
    
    csv_rows = []
    
    for entry in entries_list:
        # Base entry data (exclude lists)
        base_entry = {k: v for k, v in entry.items() if not isinstance(v, list)}
        
        # Apply formatting to main columns
        if '漢字' in base_entry:
            base_entry['漢字'] = format_hanji(base_entry.get('漢字', ''))
        if '羅馬字' in base_entry:
            base_entry['羅馬字'] = format_lomaji(base_entry.get('羅馬字', ''))

        # Flatten simple lists for CSV (Entry relations)
        # We can json.dumps distinct relationship lists or join them with a delimiter if they are simple strings
        for key in entry_relations.values():
            if key == '異用字':
                # Special handling for 異用字: extract value and join with /
                yi_yong_ji_list = entry.get(key, [])
                if yi_yong_ji_list:
                    # Extract '漢字' value from each item in the list
                    values = [item.get('漢字', '') for item in yi_yong_ji_list if item.get('漢字')]
                    base_entry[key] = "/".join(values)
                else:
                    base_entry[key] = None
            elif key == '語音差異':
                # Special handling for 語音差異: remove '漢字' and format as Key：Value\n
                diff_list = entry.get(key, [])
                if diff_list:
                    formatted_lines = []
                    for item in diff_list:
                        # Filter out '漢字'
                        filtered_item = {k: v for k, v in item.items() if k != '漢字'}
                        # Format as Key：Value
                        for k, v in filtered_item.items():
                             if pd.notna(v):
                                formatted_lines.append(f"{k}：{v}")
                    base_entry[key] = "\n".join(formatted_lines)
                else:
                    base_entry[key] = None
            elif key in ['詞目tuì詞目近義', '詞目tuì詞目反義']:
                # Special handling for synonyms/antonyms: extract '對應詞目漢字' and join with /
                rel_list = entry.get(key, [])
                if rel_list:
                     # Extract '對應詞目漢字' value from each item in the list
                    values = [item.get('對應詞目漢字', '') for item in rel_list if item.get('對應詞目漢字')]
                    base_entry[key] = "/".join(values)
                else:
                    base_entry[key] = None
            elif entry.get(key):
                base_entry[key] = json.dumps(entry[key], ensure_ascii=False)
            else:
                base_entry[key] = None

        definitions = entry.get('義項', [])
        
        # Merge columns from definitions into base_entry
        # We want ONE row per Entry.
        
        # 1. Helper to aggregate text lists
        # We need to handle:
        # - 詞性 (likely distinct per def)
        # - 解說 (distinct per def)
        # - 例句 (list of sentences per def)
        # - relations (json per def)

        if not definitions:
            # No definitions, just append base
            csv_rows.append(base_entry)
        else:
            # Aggregate fields
            combined_jie_shuo = [] # For "1. ({詞性}) {解說}"
            combined_ci_xing = []  # For "1. {詞性}" (optional, but good to keep)
            
            # Example sentence aggregators
            all_sentences_main = []
            all_sentences_huayu = []
            all_sentences_audio = []
            
            # Relation aggregators
            combined_relations = {k: [] for k in def_relations.values()}

            # sentence_global_idx = 1 # Removed global index
            
            # Wrapper to determine prefix format
            num_defs = len(definitions)
            
            for idx, d in enumerate(definitions, 1):
                # Extract fields
                ci_xing = d.get('詞性', '')
                if pd.isna(ci_xing): ci_xing = ''
                
                jie_shuo = d.get('解說', '')
                if pd.isna(jie_shuo): jie_shuo = ''
                
                # Format: 1. ({詞性}) {解說}
                # If 詞性 is present, wrap in parens.
                part_str = f"({ci_xing})" if ci_xing else ""
                # Avoid extra space if part_str is empty
                line_content = f"{part_str} {jie_shuo}".strip() 
                combined_jie_shuo.append(f"{idx}. {line_content}")
                
                if ci_xing:
                    combined_ci_xing.append(f"{idx}. {ci_xing}")
                
                # Process Sentences
                sentences = d.get('例句', [])
                if sentences:
                    for s_idx, s in enumerate(sentences, 1):
                        hanji = s.get('漢字', '') or ''
                        lomaji = s.get('羅馬字', '') or ''
                        huayu = s.get('華語', '') or ''
                        audio = s.get('音檔檔名', '') or ''
                        
                        hanji = format_hanji(hanji)
                        lomaji = format_lomaji(lomaji)
                        
                        # Nested Numbering Logic
                        if num_defs > 1:
                            if len(sentences) > 1:
                                prefix = f"{idx}-{s_idx}."
                            else:
                                prefix = f"{idx}."
                        else:
                            prefix = f"{s_idx}."
                        
                        main_str = f"{prefix} {hanji}"
                        if lomaji:
                            main_str += f" ({lomaji})"
                        
                        all_sentences_main.append(main_str)
                        all_sentences_huayu.append(f"{prefix} {huayu}")
                        all_sentences_audio.append(f"{prefix} {audio}")
                
                # Process Relations
                for key in def_relations.values():
                    val = d.get(key)
                    if val:
                        if key in ['義項tuì義項近義', '義項tuì義項反義', '義項tuì詞目近義', '義項tuì詞目反義']:
                            # Extract '對應詞目漢字'
                            extracted_values = []
                            for item in val:
                                target_word = item.get('對應詞目漢字', '')
                                if target_word:
                                    extracted_values.append(target_word)
                            
                            if extracted_values:
                                joined_val = "/".join(extracted_values)
                                combined_relations[key].append(f"{idx}. {joined_val}")
                        else:
                            combined_relations[key].append(f"{idx}. {json.dumps(val, ensure_ascii=False)}")

            # Assign aggregated values to base_entry
            base_entry['解說'] = "\n".join(combined_jie_shuo)
            base_entry['詞性'] = "\n".join(combined_ci_xing)
            
            if all_sentences_main:
                base_entry['例句'] = "\n".join(all_sentences_main)
                base_entry['例句-華語'] = "\n".join(all_sentences_huayu)
                base_entry['例句-音檔'] = "\n".join(all_sentences_audio)
            else:
                 base_entry['例句'] = None
                 base_entry['例句-華語'] = None
                 base_entry['例句-音檔'] = None

            for key, val_list in combined_relations.items():
                if val_list:
                    base_entry[key] = "\n".join(val_list)
                else:
                    base_entry[key] = None
            
            csv_rows.append(base_entry)

    df_csv = pd.DataFrame(csv_rows)
    print(f"Saving CSV to {csv_output}...")
    df_csv.to_csv(csv_output, index=False, encoding='utf-8')

    print("Conversion complete!")

if __name__ == "__main__":
    convert_KipSutianData()
