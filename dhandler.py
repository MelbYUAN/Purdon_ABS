import sdmx
import pandas as pd
from urllib.parse import urlparse
from fmapping import field_index
from requests.exceptions import HTTPError
import time
from datetime import date
from dateutil.relativedelta import relativedelta

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
            
# To extract data from work not commenced and dwellings not commenced, as it releases quarterly, we need to get the end of the two prior quarter
def get_end_of_two_quarters_ago():
    today = date.today()
    date_minus_quarters = today - relativedelta(months=6)
    # Calculate the correct end of quarter
    # Adjust month to align with quarter end
    month = date_minus_quarters.month
    if 1 <= month <= 3:
        quarter_end_month = 3  # End of first quarter
    elif 4 <= month <= 6:
        quarter_end_month = 6  # End of second quarter
    elif 7 <= month <= 9:
        quarter_end_month = 9  # End of third quarter
    else:
        quarter_end_month = 12  # End of fourth quarter

    # Set date to the last day of the calculated end of quarter
    end_of_previous_quarter = date_minus_quarters.replace(month=quarter_end_month, day=1)
    
    # Format the date into "month-year" format and convert month to lowercase
    formatted_date = end_of_previous_quarter.strftime("%b-%Y").lower()
    
    return formatted_date

def get_two_month_prior():
    today = date.today()
    date_minus_two_months = today - relativedelta(months=2)
    formatted_date = date_minus_two_months.strftime("%b-%Y").lower()
    return formatted_date

# Define a function to construct the API URL
def construct_building_approvals_url(base_url, measure, sector, work_type, building_type, region_type, region, freq='M'):
    # base_url = 'https://api.data.abs.gov.au/data/ABS,BA_SA2,2.0.0/'
    url = f'{measure}.{sector}.{work_type}.{building_type}.{region_type}.{region}.{freq}?dimensionAtObservation=AllDimensions'
    return base_url + url

def construct_building_activity_url(base_url, measure, state, price_adjustment, work_type, sector, building_type, adjustment_type, freq='Q'):
    # base_url = 'https://api.data.abs.gov.au/data/ABS,BUILDING_ACTIVITY,1.0.0/'
    url = f'{measure}.{state}.{price_adjustment}.{work_type}.{sector}.{building_type}.{adjustment_type}.{freq}?dimensionAtObservation=AllDimensions'
    return base_url + url
