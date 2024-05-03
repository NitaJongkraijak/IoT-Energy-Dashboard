from asammdf import MDF
import pandas as pd
import os
import re
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Protob Conversion to Parquet") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
# Define the regex pattern
pattern = r'CAN\d\.VBOX_\d+'

def process_prefix(extracted_df, prefix):
    # Filter columns based on prefix
    prefix_columns = [col for col in extracted_df.columns if col.startswith(prefix)]
    print(prefix_columns)
    # Create folder structure based on prefix only
    year = extracted_df.index.year.values[0]
    month = extracted_df.index.month.values[0]
    day = extracted_df.index.day.values[0]

    output_folder = os.path.join('output', prefix, str(year), str(month), str(day))
    os.makedirs(output_folder, exist_ok=True)
    # Create subset DataFrame with columns matching the prefix
    subset_df = extracted_df[prefix_columns]
    # Concatenate columns with the same prefix
    combined_df = pd.DataFrame(subset_df)  # Convert Series to DataFrame

    # # Path to save Parquet file
    parquet_file_path = os.path.join(output_folder, os.path.splitext(os.path.basename(input_file_path))[0]+'.parquet')

    # Save to Parquet
    combined_df.to_parquet(parquet_file_path)
    combined_df.write.parquet("output/proto.parquet")
    
    print(f"Parquet file saved to: {parquet_file_path}")
    
# Path to your input file
input_file_path = 'input/00000003.MF4'
# Path to your DBC file
dbc_file_path = 'input/VB20SL_SI.dbc'

mdf = MDF(input_file_path)
kwargs = {'database_files': {"CAN": [(dbc_file_path, 0)]}, 'version': '4.11', 'prefix': '', 'consolidated_j1939': True}

# Ensure that the path to your DBC file is correct

extracted = mdf.extract_bus_logging(**kwargs)
# Extract the bus logging data using the provided DBC file(s)

extracted_df = extracted.to_dataframe(time_from_zero=False, time_as_date=True, use_display_names=True)
extracted_df.index = pd.to_datetime(extracted_df.index)

# Get unique prefixes (CAN1.VBOX_1, CAN1.VBOX_2, etc.)
prefixes = {match.group() for col in extracted_df.columns for match in re.finditer(pattern, col)}
print(prefixes)
# Process each prefix separately
for prefix in prefixes:
    process_prefix(extracted_df, prefix)