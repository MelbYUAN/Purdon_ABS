# Sometimes a request to a URL fails and then succeeds moments later
import pandas as pd
import sys

# Add the path to the directory where the custom modules are located
sys.path.append("D:/Purdon2")
from dhandler import extract_data, construct_building_activity_url

# Define the API URL, the reason why I put dwelling sorts in the URL
# is to reduce the size of the DataFrame, otherwise,
# it will be too large and encounter internet error (504)
api_urls = {
    '2016_2021':"https://data.api.abs.gov.au/rest/data/ABS,BA_SA2_2016-21,1.0.0/...TOT+150+130+134+133+132+131+120+122+121+110.AUS+STE..M?dimensionAtObservation=AllDimensions",
    '2021_onwards': 'https://data.api.abs.gov.au/rest/data/ABS,BA_SA2,2.0.0/...TOT+150+130+134+133+132+131+120+122+121+110.AUS+STE..M?dimensionAtObservation=AllDimensions'
}

# Extract the data
data_2016_2021 = extract_data(api_urls['2016_2021'])
data_2021_onwards = extract_data(api_urls['2021_onwards'])

# Merge concatenated DataFrames for each year
merged_data = None
merged_data = pd.merge(data_2016_2021, data_2021_onwards, on=["MEASURE", "SECTOR", "WORK_TYPE", "BUILDING_TYPE", "REGION_TYPE", "REGION"], how="outer")

# Perform unpivot (melt) (month columns to rows)
unpivoted_data = pd.melt(merged_data, id_vars=["MEASURE", "SECTOR", "WORK_TYPE", "BUILDING_TYPE", "REGION_TYPE", "REGION"], var_name="Month", value_name="VALUE")

# # Pivot the data as adding measures as columns
build_app = unpivoted_data.pivot(index=["REGION", "REGION_TYPE", "WORK_TYPE", "SECTOR", "BUILDING_TYPE", "Month"], columns="MEASURE", values="VALUE").reset_index()

# Sort the DataFrame
build_app.sort_values(by=["REGION", "REGION_TYPE", "WORK_TYPE", "SECTOR", "BUILDING_TYPE", "Month"], inplace=True)

# Calculate moving Sums
build_app[['Year-End Dwelling Units', 'Year-End Building Jobs Value']] = build_app.groupby(["REGION_TYPE", "REGION", "WORK_TYPE", "SECTOR", "BUILDING_TYPE"])[['Number of dwelling units', 'Value of building jobs']].transform(lambda x: x.rolling(window=12).sum())

# Rename columns for clarity
build_app.rename(columns={'Number of dwelling units': 'Dwelling Units', 
                             'Value of building jobs': 'Building Jobs Value',
                             "REGION": 'State', 
                             'REGION_TYPE': 'Region Type',
                             "WORK_TYPE": 'Building Work Type', 
                             'SECTOR': 'Sector', 
                             "BUILDING_TYPE": 'Building Type'}, inplace=True)

build_app["Month Time"] = pd.to_datetime(build_app["Month"], format="%Y-%m") + pd.DateOffset(days=31)
build_app["Month Time"] = build_app["Month Time"] - pd.to_timedelta(build_app["Month Time"].dt.day, unit="D")
build_app.to_csv('build_app.csv')


