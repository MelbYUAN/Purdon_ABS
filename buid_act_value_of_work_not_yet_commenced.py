import requests
import pandas as pd
from dhandler import get_end_of_two_quarters_ago

def download_file(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as file:
            file.write(response.content)
        print("File downloaded successfully!")
    else:
        print("Failed to download the file:", response.status_code)

# URL to the Excel file
file_url = f'https://www.abs.gov.au/statistics/industry/building-and-construction/building-activity-australia/{get_end_of_two_quarters_ago()}/87520079.xlsx'
download_file(file_url, 'building_activity_value_of_work_not_yet_commenced.xlsx')

# Load the Excel file into a DataFrame
df = pd.read_excel('building_activity_value_of_work_not_yet_commenced.xlsx', sheet_name='Data1', header=0)

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
df_long = df.melt(id_vars=['Date'], var_name='metadata', value_name='Value of work not yet commenced')

# Split the metadata column into separate columns for measure, building_type, and region
df_long[['measure', 'Building Type', 'Region']] = df_long['metadata'].str.split(';', n=2, expand=True) # maxsplit=2: This ensures only the first two semicolons split the string, resulting in exactly three items.

# Drop rows where 'measure' column has the value 'Value of work in the pipeline'
df_long = df_long[df_long['measure'] == 'Value of work not yet commenced ']

# Clean up extra spaces
df_long['measure'] = df_long['measure'].str.strip('; ').str.strip()
df_long['Building Type'] = df_long['Building Type'].str.strip()
df_long['Region'] = df_long['Region'].str.strip('; ').str.strip()
df_long['Region Type'] = 'States and Territories'
df_long['Building Work Type'] = 'Total Work'
df_long['Sector Own'] = 'Total Sector'
df_long['Adjustment Type'] = 'Original'
df_long['Price Adjustment'] = 'Current Prices'

df_long.rename(columns={'Date': 'Quarter',
                        'Sector Own': 'Sector',
                        'Region': 'State'}, inplace=True)

# # Drop the metadata column as it’s no longer needed
df_long.drop(columns=['metadata', 'measure'], inplace=True)

# Replace "Total (Type of Building)" with "Total" in the "Building Type" column
df_long['Building Type'] = df_long['Building Type'].replace("Total (Type of Building)", "Total")

# Sort the DataFrame by 'Quarter' to ensure proper timeline alignment
df_long.sort_values(by='Quarter', inplace=True)

# Calculate the 4-quarter moving sum based on the specified columns
df_long['Year-End Value of Work Not Yet Commenced'] = (
    df_long.sort_values('Quarter')
    .groupby(['Building Type', 'State', 'Region Type', 'Building Work Type', 'Sector', 'Adjustment Type', 'Price Adjustment'])['Value of work not yet commenced']
    .transform(lambda x: x.rolling(window=4, min_periods=4).sum())
)

# Rearrange columns
column_order = [
    'State', 'Region Type', 'Building Work Type', 'Sector', 'Building Type', 'Price Adjustment','Adjustment Type', 'Quarter', 
    'Value of work not yet commenced', 'Year-End Value of Work Not Yet Commenced'
]
building_activity_value_of_work_yet_to_be_done = df_long[column_order]
building_activity_value_of_work_yet_to_be_done['Value of work not yet commenced'] = building_activity_value_of_work_yet_to_be_done['Value of work not yet commenced'].astype(float)
building_activity_value_of_work_yet_to_be_done['Year-End Value of Work Not Yet Commenced'] = building_activity_value_of_work_yet_to_be_done['Year-End Value of Work Not Yet Commenced'].astype(float)

# Convert "Quarter" to datetime and strip the time portion
building_activity_value_of_work_yet_to_be_done["Quarter Time"] = pd.PeriodIndex(building_activity_value_of_work_yet_to_be_done["Quarter"], freq='Q').to_timestamp(how='end')
building_activity_value_of_work_yet_to_be_done["Quarter Time"] = building_activity_value_of_work_yet_to_be_done["Quarter Time"].dt.normalize()

# Save the structured data to a new Excel file
building_activity_value_of_work_yet_to_be_done.to_csv('building_activity_value_of_work_not_yet_commenced_1.csv', index=False)
print(building_activity_value_of_work_yet_to_be_done.dtypes)


