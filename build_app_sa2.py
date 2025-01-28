# Description: This script extracts data from the Australian Bureau of Statistics (ABS) API 
# to retrieve building approvals data by Statistical Areas Level 2 (SA2) and Local Government Areas (LGAs) 
# and exports the data to an Excel file.

# Further Development required, not finalised yet
import sys
import pandas as pd
import asyncio
import aiohttp

# Add the path to the directory where your custom modules are located
sys.path.append("D:/Purdon")  # Add the directory

from dhandler2 import extract_data
import pandas as pd

# Define the base URLs
base_urls = {
    '2016_2021':'https://data.api.abs.gov.au/rest/data/ABS,BA_SA2_2016-21,1.0.0/...130.SA2..M?dimensionAtObservation=AllDimensions',
    #'2021_onwards': 'https://api.data.abs.gov.au/data/ABS,BA_SA2,2.0.0/'
}
# https://data.api.abs.gov.au/rest/data/ABS,BA_SA2_2016-21,1.0.0/...700+TOT+100+150+130+134+133+132+131+120+122+121+110.SA2.102011028.M?dimensionAtObservation=AllDimensions

# BUILDING_TYPE dictionary with names as keys and indexes as values for the specified indexes
building_type = {
    "All Buildings": "TOT",
    "Total Residential": "100",
    "Total Other Residential": "150",
    "Apartments - Total including those attached to a house": "130",
    "Apartments - In a nine or more storey block": "134",
    "Apartments - In a four to eight storey block": "133",
    "Apartments - In a three storey block": "132",
    "Apartments - In a one or two storey block": "131",
    "Semi-detached, row or terrace houses, townhouses - Total": "120",
    "Semi-detached, row or terrace houses, townhouses - Two or more storeys": "122",
    "Semi-detached, row or terrace houses, townhouses - One storey": "121",
    "Houses": "110",
    "Total Non-Residential": "700"
}

merged_data = None
# Define an asynchronous function to run extract_data for each API URL
async def extract_data_async(session, url, retries=3, delay=5):
    data = await asyncio.to_thread(extract_data, url, retries, delay)
    return data

async def fetch_and_merge_data():
    # Initialize an empty DataFrame for all merged data
    merged_data = pd.DataFrame()

    # Loop over each building type and extract data
    async with aiohttp.ClientSession() as session:
        tasks = []
        for key, value in building_type.items():
            for val in value:
                url = f'https://data.api.abs.gov.au/rest/data/ABS,BA_SA2_2016-21,1.0.0/...{val}.SA2..M?dimensionAtObservation=AllDimensions'
                tasks.append(extract_data_async(session, url))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)

        # Concatenate all the results into one DataFrame
        for result in results:
            if result is not None:
                merged_data = pd.concat([merged_data, result], ignore_index=True, sort=False)

    # Return the merged data
    return merged_data

# Run the asynchronous data fetching and merging
async def main():
    merged_data = await fetch_and_merge_data()
    print(merged_data.head())

# Start the asynchronous event loop
if __name__ == "__main__":
    asyncio.run(main())

for key, value in building_type.items():
    # Extract the data
    data1 = extract_data(f'https://data.api.abs.gov.au/rest/data/ABS,BA_SA2_2016-21,1.0.0/...{value}.SA2..M?dimensionAtObservation=AllDimensions')
    data2 = extract_data(f'https://data.api.abs.gov.au/rest/data/ABS,BA_SA2,2.0.0/...{value}.SA2..M?dimensionAtObservation=AllDimensions')

    # Concatenate DataFrames for each year
    merged_data_temp = pd.concat([data1, data2], ignore_index=True, sort=False)
    
    # Combine all merged data into one DataFrame
    merged_data = pd.concat([merged_data, merged_data_temp], ignore_index=True, sort=False)
