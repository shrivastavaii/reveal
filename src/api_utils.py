import requests
import pandas as pd

BASE_URL = "https://api.census.gov/data/timeseries/intltrade/imports/hs"

def get_census_data(country_code, hs_code, time, api_key):
    """
    Fetch import data from the U.S. Census International Trade HS dataset.

    Parameters
    ----------
    country_code : str
        Census country code (e.g., Canada = "1220").
    hs_code : str
        HS commodity code (2, 4, or 6 digits, e.g., "280530").
    time : str
        The time period in 'YYYY-MM' format.
        Example: '2024-12' for annual totals, or '2024-01' for January 2024.
    api_key : str
        Your Census API key.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the API response.
        Columns typically include:
        - CTY_NAME (country name)
        - I_COMMODITY (HS code)
        - I_COMMODITY_LDESC (description)
        - COMM_LVL (commodity level)
        - GEN_VAL_MO (monthly value) or GEN_VAL_YR (annual value)
    """

    # Infer COMM_LVL from HS code length (2, 4, or 6 digits)
    comm_lvl = len(hs_code)

    # Pick variables based on whether month == "12" (annual totals) or not (monthly data)
    month = time.split("-")[1]
    if month == "12":
        variables = "CTY_NAME,I_COMMODITY,I_COMMODITY_LDESC,COMM_LVL,GEN_VAL_YR"
    else:
        variables = "CTY_NAME,I_COMMODITY,I_COMMODITY_LDESC,COMM_LVL,GEN_VAL_MO"

    # Build query parameters
    params = {
        "get": variables,
        "CTY_CODE": country_code,
        "I_COMMODITY": hs_code,
        "COMM_LVL": str(comm_lvl),
        "time": time,
        "key": api_key
    }

    # Call API
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()  # throw error if request failed

    data = response.json()

    # Convert JSON (array-of-arrays) into DataFrame
    if isinstance(data, list) and len(data) > 1:
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
    else:
        return pd.DataFrame()
