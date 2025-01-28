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
file_url = f'https://www.abs.gov.au/statistics/industry/building-and-construction/building-activity-australia/{get_end_of_two_quarters_ago()}/87520080.xlsx'
download_file(file_url, 'building_activity_dwelling_units_not_yet_commenced.xlsx')

# Load the Excel file into a DataFrame
df = pd.read_excel('building_activity_dwelling_units_not_yet_commenced.xlsx', sheet_name='Data1', header=0)

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
df_long = df.melt(id_vars=['Date'], var_name='metadata', value_name='Dwelling Units Not Yet Commenced')

# Split the metadata column into separate columns for measure, building_type, and region
df_long[['measure', 'Building Type', 'Building Work Type', 'Region']] = df_long['metadata'].str.split(';', n=3, expand=True) # maxsplit=3: This ensures only the first three semicolons split the string, resulting in exactly three items.

# Clean up extra spaces
df_long['measure'] = df_long['measure'].str.strip('; ').str.strip()
df_long['Building Type'] = df_long['Building Type'].str.strip()
df_long['Region'] = df_long['Region'].str.strip('; ').str.strip()
df_long['Region Type'] = 'States and Territories'
df_long['Sector Own'] = 'Total Sector'
df_long['Adjustment Type'] = 'Original'
df_long['Building Work Type'] = df_long['Building Work Type'].str.strip()
df_long['Price Adjustment'] = 'Current Prices'
df_long.rename(columns={'Date': 'Quarter',
                        'Sector Own': 'Sector',
                        'Region': 'State'}, inplace=True)

# # Drop the metadata column as it’s no longer needed
df_long.drop(columns=['metadata', 'measure'], inplace=True)

# Replace "Total (Type of Building)" with "Total" in the "Building Type" column
df_long['Building Type'] = df_long['Building Type'].replace("Total (Type of Building)", "Total")
df_long['Building Work Type'] = df_long['Building Work Type'].replace("Total (Type of Work)", "Total Work")

# Sort the DataFrame by 'Quarter' to ensure proper timeline alignment
df_long.sort_values(by='Quarter', inplace=True)

# Calculate the 4-quarter moving sum based on the specified columns
df_long['Year-End Dwelling Units Not Yet Commenced'] = (
    df_long.sort_values('Quarter')
    .groupby(['Building Type', 'State', 'Region Type', 'Building Work Type', 'Sector', 'Adjustment Type', 'Price Adjustment',])['Dwelling Units Not Yet Commenced']
    .transform(lambda x: x.rolling(window=4, min_periods=4).sum())
)

# Rearrange columns
column_order = [
    'State', 'Region Type', 'Building Work Type', 'Sector', 'Building Type', 'Adjustment Type', 'Quarter', 'Price Adjustment',
    'Dwelling Units Not Yet Commenced', 'Year-End Dwelling Units Not Yet Commenced'
]
build_act_dwelling_units_not_yet_commenced = df_long[column_order]
build_act_dwelling_units_not_yet_commenced['Dwelling Units Not Yet Commenced'] = build_act_dwelling_units_not_yet_commenced['Dwelling Units Not Yet Commenced'].astype(float)
build_act_dwelling_units_not_yet_commenced['Year-End Dwelling Units Not Yet Commenced'] = build_act_dwelling_units_not_yet_commenced['Year-End Dwelling Units Not Yet Commenced'].astype(float)

# Convert "Quarter" to datetime and strip the time portion
build_act_dwelling_units_not_yet_commenced["Quarter Time"] = pd.PeriodIndex(build_act_dwelling_units_not_yet_commenced["Quarter"], freq='Q').to_timestamp(how='end')
build_act_dwelling_units_not_yet_commenced["Quarter Time"] = build_act_dwelling_units_not_yet_commenced["Quarter Time"].dt.normalize()

# Save the structured data to a new Excel file
build_act_dwelling_units_not_yet_commenced.to_csv('building_activity_dwelling_units_not_yet_commenced_1.csv', index=False)
print(build_act_dwelling_units_not_yet_commenced.dtypes)

