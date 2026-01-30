
import pandas as pd
import json

def convert_kautian():
    input_file = 'kautian.ods'
    csv_output = 'kautian.csv'
    json_output = 'kautian.json'

    print(f"Reading {input_file}...")
    try:
        # read_excel supports .ods with engine='odf'
        # install odfpy if not present: pip install odfpy
        df = pd.read_excel(input_file, engine='odf')
        
        print(f"Converting to {csv_output}...")
        df.to_csv(csv_output, index=False, encoding='utf-8')
        
        print(f"Converting to {json_output}...")
        # orient='records' creates a list of dicts: [{col1: val1, ...}, ...]
        df.to_json(json_output, orient='records', force_ascii=False, indent=2)
        
        print("Conversion complete!")
        print(f"CSV saved to: {csv_output}")
        print(f"JSON saved to: {json_output}")

    except ImportError as e:
        print(f"Import error: {e}")
        print("Please install required packages: pip install pandas odfpy openpyxl")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    convert_kautian()
