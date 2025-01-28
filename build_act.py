import pandas as pd
import sys

# Add the path to the directory where the custom modules are located
sys.path.append("D:/Purdon2")
from dhandler import extract_data, construct_building_activity_url

# Define the API URL, the reason why I put current and original prices in the URL
# is to reduce the size of the DataFrame, otherwise,
# it will be too large and encounter internet error (504)
api_url = "https://data.api.abs.gov.au/rest/data/ABS,BUILDING_ACTIVITY,1.0.0/..CUR....10.Q?dimensionAtObservation=AllDimensions"

# # Previous URL
# api_url = "https://api.data.abs.gov.au/data/ABS,BUILDING_ACTIVITY,1.0.0/..CUR....10.Q?dimensionAtObservation=AllDimensions"

# Extract the data
data = extract_data(api_url)

# Unpivot (melt) the DataFrame to convert quarterly columns into rows, be careful with the duplicates
unpivoted_df = pd.melt(
    data,
    id_vars=[
        "MEASURE",
        "REGION",
        "PRICE_ADJ",
        "BLD_WORK_TYPE",
        "SECTOR_OWN",
        "TYPE_BLDG",
        "TSEST",
    ],
    var_name="Quarter",
    value_name="VALUE",
)

# Pivot the data to convert measure columns into various columns
pivoted_df = unpivoted_df.pivot(
    index=[
        "REGION",
        "PRICE_ADJ",
        "BLD_WORK_TYPE",
        "SECTOR_OWN",
        "TYPE_BLDG",
        "TSEST",
        "Quarter",
    ],
    columns=["MEASURE"],
    values="VALUE",
).reset_index()

# Sort the pivoted DataFrame based on several fields, prepare for calculating moving Sums
pivoted_df.sort_values(
    by=[
        "REGION",
        "PRICE_ADJ",
        "BLD_WORK_TYPE",
        "SECTOR_OWN",
        "TYPE_BLDG",
        "TSEST",
        "Quarter",
    ],
    inplace=True,
)

# Define the list of measures for the moving sum calculation
measures = [
    "Number of dwelling units commenced",
    "Number of dwelling units completed",
    "Number of dwelling units under construction",
    "Value of work done during quarter",
    "Value of work yet to be done",
    "Value of work commenced",  # Newly added measure
    "Value of work completed",  # Newly added measure
    "Value of work under construction",  # Newly added measure
]

# Compute 4-quarter moving sum for selected columns
pivoted_df[
    [
        "Year-End Dwelling Units Commenced",
        "Year-End Dwelling Units Completed",
        "Year-End Dwelling Units Under Construction",
        "Year-End Work Done During Quarter",
        "Year-End Work Yet To Be Done",
        "Year-End Work Commenced",
        "Year-End Work Completed",
        "Year-End Work Under Construction",
    ]
] = pivoted_df.groupby(
    ["REGION", "PRICE_ADJ", "BLD_WORK_TYPE", "SECTOR_OWN", "TYPE_BLDG", "TSEST"]
)[
    measures
].transform(
    lambda x: x.rolling(window=4).sum()
)

# Filter the DataFrame for rows where 'PRICE_ADJ' is 'Current Prices' and 'Adjustment Type' is 'Original'
build_act = pivoted_df[
    (pivoted_df["PRICE_ADJ"] == "Current Prices") & (pivoted_df["TSEST"] == "Original")
].copy()


# Rename columns for final presentation
build_act.rename(
    columns={
        "Number of dwelling units commenced": "Dwelling Units Commenced",
        "Number of dwelling units completed": "Dwelling Units Completed",
        "Number of dwelling units under construction": "Dwelling Units Under Construction",
        "Value of work commenced": "Work Commenced",
        "Value of work completed": "Work Completed",
        "Value of work under construction": "Work Under Construction",
        "Value of work done during quarter": "Work Done",
        "Value of work yet to be done": "Work Yet to be Done",
        "REGION": "State",
        "PRICE_ADJ": "Price Adjustment",
        "BLD_WORK_TYPE": "Building Work Type",
        "SECTOR_OWN": "Sector",
        "TYPE_BLDG": " Building Type",
        "TSEST": "Adjustment Type",
    },
    inplace=True,
)

# Remove leading/trailing whitespaces from column names in the DataFrame
build_act.columns = build_act.columns.str.strip()

# Convert "Quarter" to datetime and strip the time portion
build_act["Quarter Time"] = pd.PeriodIndex(build_act["Quarter"], freq='Q').to_timestamp(how='end')
build_act["Quarter Time"] = build_act["Quarter Time"].dt.normalize()

build_act.to_csv("build_act.csv", index=False)
print(build_act.dtypes)
