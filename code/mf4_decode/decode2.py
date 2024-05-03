import os
from asammdf import MDF

# Input and output directories
input_directory = 'input'
output_directory = 'output'

# List to store pairs of .MF4 and .dbc files
file_pairs = []

# Iterate through all files in the directory
for filename in os.listdir(input_directory):
    # Construct the full path to the file
    file_path = os.path.join(input_directory, filename)
    
    # Check if the file has a .MF4 extension
    if filename.endswith('.MF4'):
        # Store the .MF4 file path
        mf4_file_path = file_path
    # Check if the file has a .dbc extension
    elif filename.endswith('.dbc'):
        # Store the .dbc file path
        dbc_file_path = file_path
        # If both .MF4 and .dbc file paths are found, append them as a pair
        if 'mf4_file_path' in locals():
            file_pairs.append((mf4_file_path, dbc_file_path))

# Process each pair of .MF4 and .dbc files in the list
for mf4_file_path, dbc_file_path in file_pairs:
    # Define the database dictionary with the DBC file and channel index
    databases = {
        "CAN": [(dbc_file_path, 0)]  # Assuming channel index is 0
    }
    
    # Load the MF4 file
    mdf = MDF(mf4_file_path)
    
    # Extract bus logging data using the provided DBC file(s)
    extracted = mdf.extract_bus_logging(database_files=databases)
    
    # Convert extracted data to a DataFrame
    extracted_df = extracted.to_dataframe()
    
    # Construct the output file path
    output_file_path = os.path.join(output_directory, os.path.splitext(os.path.basename(mf4_file_path))[0] + '_extract.parquet')
    
    # Save the DataFrame to Parquet format
    extracted_df.to_parquet(output_file_path)
