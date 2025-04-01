import os
import requests
import pandas as pd
from io import StringIO
from datetime import datetime

def fetch_weather_data(station_id, year, month, day):
    """
    Fetch weather data from the climate.weather.gc.ca API for a given station ID and date.
    
    The API endpoint is structured as follows:
    
    https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv
      &stationID={station_id}&Year={year}&Month={month}&Day={day}
      &time=LST&timeframe=1&submit=Download+Data
    
    Parameters:
      - station_id (int): The weather station ID.
      - year (int): The year for which data is requested.
      - month (int): The month (as an integer) for which data is requested.
      - day (int): The day (as an integer) for which data is requested.
      
    Returns:
      - pandas.DataFrame: A DataFrame containing the weather data.
    """
    base_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"
    params = {
        "format": "csv",
        "stationID": station_id,
        "Year": year,
        "Month": month,
        "Day": day,
        "time": "LST",
        "timeframe": "1",
        "submit": "Download Data"
    }
    
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data for station {station_id} on {year}-{month}-{day}. HTTP status: {response.status_code}")
    
    # Load the CSV text into a DataFrame
    df = pd.read_csv(StringIO(response.text))
    return df

def check_nulls(df):
    """
    Check for null values in the DataFrame and print counts per column.
    """
    null_counts = df.isnull().sum()
    print("Null counts per column:")
    print(null_counts)

def check_outliers(df, numeric_columns, factor=1.5):
    """
    Check for outliers in each numeric column using the IQR method.
    
    Parameters:
      - df (DataFrame): The DataFrame to check.
      - numeric_columns (list): List of column names that are numeric.
      - factor (float): The multiplier for the IQR to determine outlier bounds.
      
    Returns:
      - dict: A dictionary with outlier details for each numeric column.
    """
    outlier_info = {}
    for col in numeric_columns:
        if col in df.columns:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - factor * IQR
            upper_bound = Q3 + factor * IQR
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            outlier_info[col] = {"count": len(outliers), "lower_bound": lower_bound, "upper_bound": upper_bound}
            print(f"Outlier check for '{col}': {len(outliers)} outliers found")
    return outlier_info

def check_missing_days(df, date_column='Date/Time (LST)'):
    """
    Check for missing days in the DataFrame based on a date column.
    
    Parameters:
      - df (DataFrame): The DataFrame to inspect.
      - date_column (str): The name of the column containing date values.
      
    Returns:
      - DatetimeIndex: A list of dates that are missing in the DataFrame.
    """
    if date_column not in df.columns:
        print(f"Date column '{date_column}' not found in data")
        return None
    
    # Convert the date column to datetime (if not already)
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    min_date = df[date_column].min()
    max_date = df[date_column].max()
    
    full_range = pd.date_range(start=min_date, end=max_date, freq='D')
    missing_days = full_range.difference(df[date_column])
    print(f"Missing days in data: {missing_days}")
    return missing_days

def main():
    # Define station IDs and years for which data will be fetched
    station_ids = [26953, 31688]
    years = [2023, 2024]
    
    # Create a folder to store raw data if it doesn't exist
    raw_data_folder = "raw_data"
    os.makedirs(raw_data_folder, exist_ok=True)
    
    combined_data = []
    
    # Loop over each station and year combination
    for station_id in station_ids:
        for year in years:
            # Generate all dates for the given year
            start_date = pd.Timestamp(year=year, month=1, day=1)
            end_date = pd.Timestamp(year=year, month=12, day=31)
            date_range = pd.date_range(start=start_date, end=end_date)
            
            for current_date in date_range:
                try:
                    # Extract month and day from the current date
                    month = current_date.month
                    day = current_date.day
                    print(f"\nFetching data for station {station_id} for date {year}-{month}-{day}...")
                    
                    df = fetch_weather_data(station_id, year, month, day)
                    
                    # Perform data quality checks
                    print("Performing null value check:")
                    check_nulls(df)
                    
                    numeric_columns = df.select_dtypes(include='number').columns.tolist()
                    if numeric_columns:
                        print("Performing outlier check:")
                        check_outliers(df, numeric_columns)
                    else:
                        print("No numeric columns found for outlier check.")
                    
                    print("Checking for missing days:")
                    # Use the correct date column name based on the CSV header.
                    check_missing_days(df, date_column='Date/Time (LST)')
                    
                    # Save the raw data to a CSV file for this station and date
                    file_name = f"weather_{station_id}_{year}_{month}_{day}.csv"
                    file_path = os.path.join(raw_data_folder, file_name)
                    df.to_csv(file_path, index=False)
                    print(f"Raw data saved to: {file_path}")
                    
                    # Add the station ID to the DataFrame (if not already present)
                    if 'Station_ID' not in df.columns:
                        df['Station_ID'] = station_id
                    combined_data.append(df)
                    
                except Exception as e:
                    print(f"Error processing station {station_id} for date {year}-{month}-{day}: {e}")
    
    # Combine all fetched data into a single CSV file
    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)
        combined_file = os.path.join(raw_data_folder, "combined_weather_data.csv")
        final_df.to_csv(combined_file, index=False)
        print(f"\nCombined raw data saved to: {combined_file}")

if __name__ == "__main__":
    main()