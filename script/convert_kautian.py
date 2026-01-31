
import pandas as pd
import json
import os
import sys

# Define input/output paths
# Define input/output paths
# Define input/output paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'public', 'bunji')

INPUT_FILE = os.path.join(DATA_DIR, 'kautian.ods')
CSV_OUTPUT = os.path.join(DATA_DIR, 'kautian.csv')
JSON_OUTPUT = os.path.join(DATA_DIR, 'kautian.json')

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


def convert_kautian(input_file=INPUT_FILE, csv_output=CSV_OUTPUT, json_output=JSON_OUTPUT):
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


    print("Processing and Merging Data...")

    # --- Pre-process Relations for fast lookup ---
    
    # Helper to group by keys and convert to list of dicts
    def group_relation(df, group_keys, value_keys=None):
        if df.empty:
            return {}
        
        # Ensure keys are integers if possible, or strings
        # But pandas groupby handles mixed types well usually.
        # We'll just use to_dict('records') then manual grouping might be safer/clearer
        # or use pandas groupby.
        
        records = df.to_dict('records')
        grouped = {}
        for row in records:
            # Create a tuple key for grouping
            key = tuple(row.get(k) for k in group_keys)
            
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
    
    csv_rows = []
    
    for entry in entries_list:
        # Base entry data (exclude lists)
        base_entry = {k: v for k, v in entry.items() if not isinstance(v, list)}
        
        # Flatten simple lists for CSV (Entry relations)
        # We can json.dumps distinct relationship lists or join them with a delimiter if they are simple strings
        for key in entry_relations.values():
            if entry.get(key):
                base_entry[key] = json.dumps(entry[key], ensure_ascii=False)
            else:
                base_entry[key] = None

        definitions = entry.get('義項', [])
        
        if not definitions:
            # Entry with no definitions, just add the entry info
            csv_rows.append(base_entry)
        else:
            for d in definitions:
                row = base_entry.copy()
                # Add/Overwrite with definition info (exclude lists)
                for k, v in d.items():
                    if not isinstance(v, list):
                        # Avoid key collision if any? 詞目id is same. 
                        # 義項id is unique to def.
                        row[k] = v
                
                # Flatten Definition relations (Sentences, etc.)
                if d.get('例句'):
                    row['例句'] = json.dumps(d['例句'], ensure_ascii=False)
                else:
                    row['例句'] = None
                    
                for key in def_relations.values():
                    if d.get(key):
                        row[key] = json.dumps(d[key], ensure_ascii=False)
                    else:
                        row[key] = None
                
                csv_rows.append(row)

    df_csv = pd.DataFrame(csv_rows)
    print(f"Saving CSV to {csv_output}...")
    df_csv.to_csv(csv_output, index=False, encoding='utf-8')

    print("Conversion complete!")

if __name__ == "__main__":
    convert_kautian()
