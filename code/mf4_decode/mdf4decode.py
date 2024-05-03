from asammdf import MDF
import pandas as pd
import os

def find_mf4_files(input_folder):
    mf4_files = []
    for root, dirs, files in os.walk(input_folder):
        mf4_files.extend([os.path.join(root, f) for f in files if f.lower().endswith('.mf4')])
    return mf4_files

def find_dbc_files(input_folder):
    dbc_files = []
    for root, dirs, files in os.walk(input_folder):
        dbc_files.extend([os.path.join(root, f) for f in files if f.lower().endswith('.dbc')])
    return dbc_files

def extract_device_id(mf4_file):
    try:
        mdf = MDF(mf4_file)
        device_info = mdf.info()
        comment_text = device_info.get('comment', '')
        comment_lines = comment_text.split('\n')
        for line in comment_lines:
            if 'serial number' in line:
                return line.split('>')[1].split('<')[0].strip()
        return 'xxxxxx'  # Default serial number if not found
    except Exception as e:
        print(f"Error extracting device ID from {mf4_file}: {e}")
        return 'xxxxxx'  # Default serial number if extraction fails

def decode_and_create_parquet(mf4_file, dbc_file, device_id):
    try:
        mdf = MDF(mf4_file)
        kwargs = {'database_files': {"CAN": [(dbc_file, 0)]}, 'version': '4.11', 'prefix': ''}
        extracted = mdf.extract_bus_logging(**kwargs)
        extracted_df = extracted.to_dataframe(time_from_zero=False, time_as_date=True, use_display_names=True)
        extracted_df.index = pd.to_datetime(extracted_df.index)
        year = extracted_df.index.year.values[0]
        month = extracted_df.index.month.values[0]
        day = extracted_df.index.day.values[0]
        device_folder = os.path.join('output', device_id, f'{year}/{month}/{day}')
        os.makedirs(device_folder, exist_ok=True)
        parquet_file_path = os.path.join(device_folder, os.path.splitext(os.path.basename(mf4_file))[0] + '.parquet')
        extracted_df.to_parquet(parquet_file_path)
        print(f"Parquet file saved to: {parquet_file_path}")
    except Exception as e:
        print(f"Error processing {mf4_file} with {dbc_file}: {e}")

def process_mf4_and_dbc_files(input_folder):
    mf4_files = find_mf4_files(input_folder)
    dbc_files = find_dbc_files(input_folder)
    print(f"MF4 files found: {mf4_files}")
    print(f"DBC files found: {dbc_files}")
    for mf4_file in mf4_files:
        for dbc_file in dbc_files:
            device_id = extract_device_id(mf4_file)
            decode_and_create_parquet(mf4_file, dbc_file, device_id)

# Usage example:
input_folder = 'input'
process_mf4_and_dbc_files(input_folder)
