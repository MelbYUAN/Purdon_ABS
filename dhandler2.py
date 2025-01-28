# As ABS changed their API, base url is now 'https://api.data.abs.gov.au/rest/data/ABS,BA_SA2,2.0.0/'
import sdmx
import pandas as pd
from urllib.parse import urlparse
from fmapping import field_index
from requests.exceptions import HTTPError
import time

# Define a function to extract data from the API
def extract_data(api, retries=3, delay=5, timeout=120):
    for attempt in range(retries):
        try:
            # Create an ABS SDMX client with custom timeout
            client = sdmx.Client('ABS')
            client.session.request = client.session.request.__func__.__get__(client.session, type(client.session))
            client.session.timeout = timeout  # Set the timeout for the session

            # Parse the URL to extract parts
            parsed_url = urlparse(api)
            
            # Extract the path segments (split by '/')
            path_segments = parsed_url.path.split('/')

            # Automatically detect the resource_id and key
            resource_id = path_segments[3].split(',')[1] # Extract 'BA_SA2_2016-21' from the second segment
            key = path_segments[4] # Extract the '1.9.1.110..1+2+3+4+5+6+7+8+AUS.M' part

            # Use client.data to fetch the dataset
            data_response = client.data(
            resource_id=resource_id, 
            key=key, 
            params={
                'dimensionAtObservation': 'AllDimensions'
                }
            )

            # Convert the data to a pandas DataFrame
            data = sdmx.to_pandas(data_response,
                            datetime=dict(dim="TIME_PERIOD", freq="FREQ", axis=1),
                            )
            
            # Reset Index to Turn It Into Columns
            data = data.reset_index()

            # Apply the mapping to each column based on the field_index
            for col in data.columns:
                if col in field_index:
                    data[col] = data[col].map(field_index[col])
            
            # Make sure all columns are strings as time series columns might be integers
            data.columns = data.columns.astype(str)

            return data
        
        except (HTTPError, Exception) as e:
            print(f"Error processing API {api}: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. Skipping this request.")
                return None

# Define a function to construct the API URL
def construct_building_approvals_url(base_url, measure, sector, work_type, building_type, region_type, region, freq='M'):
    # base_url = 'https://api.data.abs.gov.au/data/ABS,BA_SA2,2.0.0/'
    url = f'{measure}.{sector}.{work_type}.{building_type}.{region_type}.{region}.{freq}?dimensionAtObservation=AllDimensions'
    return base_url + url

def construct_building_activity_url(base_url, measure, state, price_adjustment, work_type, sector, building_type, adjustment_type, freq='Q'):
    # base_url = 'https://api.data.abs.gov.au/data/ABS,BUILDING_ACTIVITY,1.0.0/'
    url = f'{measure}.{state}.{price_adjustment}.{work_type}.{sector}.{building_type}.{adjustment_type}.{freq}?dimensionAtObservation=AllDimensions'
    return base_url + url
