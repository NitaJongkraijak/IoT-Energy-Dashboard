from asammdf import MDF
import pyarrow
import pandas as pd

mdf = MDF(r'input/OBD2/feista_obd2.mf4')
kwargs = {'database_files': {"CAN": [("input\OBD2\CSS-Electronics-OBD2-v1.4.dbc",1)]}, 'version': '4.11', 'prefix': ''}

# Ensure that the path to your DBC file is correct

extracted = mdf.extract_bus_logging(**kwargs)
# Extract the bus logging data using the provided DBC file(s)

extracted_df = extracted.to_dataframe(time_from_zero=False, time_as_date=True,use_display_names=True)
# extracted_df.index = pd.to_datetime(extracted_df.index)

extracted_df.to_parquet('output/extract_odb.parquet')
