import requests
import pandas as pd
from time import sleep

API_KEY = "PUT_YOUR_API_KEY_HERE"


CRITICAL_HS = {
    "cobalt": ["810520", "810530"],
    "nickel": ["750110", "750120", "750210", "750400"],
    "graphite": ["250410", "250490", "380110"],
    "lithium": ["280530", "282520"],
    "aluminum": ["760110", "760120", "760200", "760612", "760711"]
}

# Flatten list
ALL_HS = sorted({item for sublist in CRITICAL_HS.values() for item in sublist})

BASE = "https://api.census.gov/data/timeseries/intltrade/exports/hs"


def get_single(yr, hs_code):
    """
    Pulls a single year + single HS code.
    """
    params = {
        "get": "CTY_NAME,HS6,ALL_VAL_YR",
        "time": yr,
        "CTY_CODE": "1220",    # Canada
        "HS6": hs_code,        # ONE at a time
        "key": API_KEY
    }

    r = requests.get(BASE, params=params)

    if r.status_code != 200:
        print(f"Error {yr} HS {hs_code}: {r.text}")
        return None

    try:
        data = r.json()
    except:
        print(f"JSON error for year {yr}, HS {hs_code}")
        print("Response was:\n", r.text)
        return None

    header = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=header)
    df["YEAR"] = yr
    df["HS6"] = hs_code
    return df


def build_dataset(start=2015, end=2024):
    frames = []

    for yr in range(start, end + 1):
        for hs in ALL_HS:
            print(f"Pulling {yr}, HS {hs}...")
            df = get_single(yr, hs)
            if df is not None:
                frames.append(df)
            sleep(0.3)

    if not frames:
        raise ValueError("No data downloaded.")

    df = pd.concat(frames, ignore_index=True)

    # Map HS6 → mineral
    hs_map = {}
    for mineral, codes in CRITICAL_HS.items():
        for code in codes:
            hs_map[code] = mineral

    df["Mineral"] = df["HS6"].map(hs_map)

    df.rename(columns={
        "CTY_NAME": "Country",
        "ALL_VAL_YR": "Value_USD"
    }, inplace=True)

    df = df[["YEAR", "Mineral", "HS6", "Country", "Value_USD"]]

    return df


if __name__ == "__main__":
    df = build_dataset()
    df.to_csv("../data/critminerals_api.csv", index=False)
    print("✔ Saved to data/critminerals_api.csv")
