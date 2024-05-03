from asammdf import MDF
import pandas as pd
import os

input_folder = 'input'

# Collect all .mf4 files recursively in the input directory
mf4_files = []
device_id = []
for root, dirs, files in os.walk(input_folder):
    mf4_files.extend([os.path.join(root, f) for f in files if f.endswith('.MF4') or f.endswith('.mf4')])


# Collect all .dbc files recursively in the input directory
dbc_files = []
for root, dirs, files in os.walk(input_folder):
    dbc_files.extend([os.path.join(root, f) for f in files if f.endswith('.dbc')])

print(f"MF4 files found: {mf4_files}")
print(f"DBC files found: {dbc_files}")

# Iterate over each .mf4 file and its associated .dbc files
for mf4_file in mf4_files:
    for dbc_file in dbc_files:
        try:
            # Load the .mf4 file
            mdf = MDF(mf4_file)

            # Extract the device information
            device_info = mdf.info()

            # Extract the serial number from the comment field
            comment_text = device_info.get('comment', '')
            comment_lines = comment_text.split('\n')
            serial_number = 'xxxxxx'  # Default serial number if not found
            for line in comment_lines:
                if 'serial number' in line:
                    serial_number = line.split('>')[1].split('<')[0].strip()
                    break

            # Extract the bus logging data using the provided DBC file(s)
            kwargs = {'database_files': {"CAN": [(dbc_file, 0)]}, 'version': '4.11', 'prefix': ''}
            extracted = mdf.extract_bus_logging(**kwargs)

            # Convert to DataFrame
            extracted_df = extracted.to_dataframe(time_from_zero=False, time_as_date=True, use_display_names=True)
            extracted_df.index = pd.to_datetime(extracted_df.index)

            # Extract the year, month, and day from the datetime index
            year = extracted_df.index.year.values[0]
            month = extracted_df.index.month.values[0]
            day = extracted_df.index.day.values[0]

            # Create folder structure based on device ID, year, month, and day
            device_folder = os.path.join('output', serial_number, f'{year}/{month}/{day}')
            os.makedirs(device_folder, exist_ok=True)

            # Path to save Parquet file
            parquet_file_path = os.path.join(device_folder, os.path.splitext(os.path.basename(mf4_file))[0] + '.parquet')

            # Save to Parquet
            extracted_df.to_parquet(parquet_file_path)

            print(f"Parquet file saved to: {parquet_file_path}")
            
        except Exception as e:
            print(f"Error processing {mf4_file} with {dbc_file}: {e}")
            # If there's an error, skip to the next .dbc file
            continue
