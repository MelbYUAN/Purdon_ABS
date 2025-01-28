import sys
# Add the path to the directory where your custom modules are located
sys.path.append("D:/Purdon")  # Add the directory

import requests
import pandas as pd
from dhandler import get_two_month_prior

def download_file(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as file:
            file.write(response.content)
        print("File downloaded successfully!")
    else:
        print("Failed to download the file:", response.status_code)

# URL to the Excel file
file_url = f'https://www.abs.gov.au/statistics/industry/building-and-construction/building-approvals-australia/{get_two_month_prior()}/87310087.xlsx'
download_file(file_url, 'building_approvals_demolition.xlsx')

# Load the Excel file into a DataFrame
df = pd.read_excel('building_approvals_demolition.xlsx', sheet_name='Data1', header=0)

# Check if the first column header is empty and set it if necessary
if df.columns[0] == '' or pd.isna(df.columns[0]) or df.columns[0] != 'Date':
    df.rename(columns={df.columns[0]: 'Date'}, inplace=True)

# Remove specific rows if needed
df.drop(df.index[:9], inplace=True)  # Adjusted to drop from row 0 to 8 after setting index

# Convert 'Date' to datetime and transform to 'YYYYQX' format
df['Date'] = pd.to_datetime(df['Date']).dt.to_period('Q').astype(str)

# Optionally, convert the 'Date' column to datetime and set it as index
# df.set_index('Date', inplace=True)

# Melt the DataFrame to a long format
df_long = df.melt(id_vars=['Date'], var_name='metadata', value_name='Dwelling Units Approved for Demolition')

# Split the metadata column into separate columns for measure, building_type, and region
df_long[['measure', 'Building Type', 'Region']] = df_long['metadata'].str.split(';', n=2, expand=True) # maxsplit=2: This ensures only the first two semicolons split the string, resulting in exactly three items.

# Clean up extra spaces
df_long['measure'] = df_long['measure'].str.strip('; ').str.strip()
df_long['Building Type'] = df_long['Building Type'].str.strip()
df_long['Region'] = df_long['Region'].str.strip('; ').str.strip()
df_long['Region Type'] = 'States and Territories'
df_long['Work Type'] = 'Total Work'
df_long['Sector'] = 'Total Sector'
df_long.rename(columns={'Date': 'Quarter',}, inplace=True)

# # Drop the metadata column as it’s no longer needed
df_long.drop(columns=['metadata', 'measure'], inplace=True)

# Replace "Total (Type of Building)" with "Total" in the "Building Type" column
df_long['Building Type'] = df_long['Building Type'].replace("Total (Type of Building)", "Total")

# Sort the DataFrame by 'Quarter' to ensure proper timeline alignment
df_long.sort_values(by='Quarter', inplace=True)

# Calculate the 4-quarter moving sum based on the specified columns
df_long['Year-End Dwelling Units Approved for Demolition'] = (
    df_long.sort_values('Quarter')
    .groupby(['Building Type', 'Region', 'Region Type', 'Work Type', 'Sector'])['Dwelling Units Approved for Demolition']
    .transform(lambda x: x.rolling(window=4, min_periods=4).sum())
)

df_long.rename(columns={'Work Type': 'Building Work Type'}, inplace=True)

# Rearrange columns
column_order = [
    'Region', 'Region Type', 'Building Work Type', 'Sector', 'Building Type', 'Quarter', 
    'Dwelling Units Approved for Demolition', 'Year-End Dwelling Units Approved for Demolition'
]
df_long = df_long[column_order]

building_approvals_demolition = df_long
building_approvals_demolition['Dwelling Units Approved for Demolition'] = building_approvals_demolition['Dwelling Units Approved for Demolition'].astype(float)
building_approvals_demolition['Year-End Dwelling Units Approved for Demolition'] = building_approvals_demolition['Year-End Dwelling Units Approved for Demolition'].astype(float)

building_approvals_demolition['Quarter Time'] = pd.PeriodIndex(building_approvals_demolition['Quarter'], freq='Q').to_timestamp(how='end')
building_approvals_demolition['Quarter Time'] = building_approvals_demolition['Quarter Time'].dt.normalize()

# Save the structured data to a new Excel file
building_approvals_demolition.to_csv('building_approvals_demolition_1.csv', index=False)
print(building_approvals_demolition.dtypes)
