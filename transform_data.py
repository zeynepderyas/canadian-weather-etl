import os
import pandas as pd
from datetime import datetime

RAW_DATA_PATH = os.path.join("raw_data", "combined_weather_data.csv")
GEONAMES_PATH = "geonames.csv"
FINAL_OUTPUT_PATH = "final_output.csv"

DATE_COLUMN = "Date/Time (LST)" # Date column in the raw weather data
TEMPERATURE_COLUMN = "Temp (°C)" # Temperature column in the raw weather data

def load_data():
    """
    Load raw weather data and geonames metadata.
    """
    weather_df = pd.read_csv(RAW_DATA_PATH)
    geonames_df = pd.read_csv(GEONAMES_PATH)
    return weather_df, geonames_df

def clean_weather_data(df):
    """
    Clean the raw weather data:
      - Convert the date column to datetime.
      - Remove rows with invalid or future dates.
      - Create a month-level column (date_month) and extract year and month.
    """
    # Convert the date column to datetime
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors='coerce')
    
    # Drop rows with invalid values
    df = df.dropna(subset=[DATE_COLUMN])
    
    # Remove future dates (using today's date as cutoff)
    today = pd.Timestamp(datetime.today().date())
    df = df[df[DATE_COLUMN] <= today]
    
    # Create a column representing the month for example YYYY-MM
    df['date_month'] = df[DATE_COLUMN].dt.strftime('%Y-%m')
    
    # Extract year and month for aggregation 
    df['year'] = df[DATE_COLUMN].dt.year
    df['month'] = df[DATE_COLUMN].dt.month
    return df

def aggregate_weather_data(df):
    """
    Aggregate weather data at the station and month level:
      - Compute average, minimum, and maximum temperatures.
    """
    agg_df = df.groupby(['Station_ID', 'year', 'month', 'date_month'])[TEMPERATURE_COLUMN].agg(
        temperature_celsius_avg='mean',
        temperature_celsius_min='min',
        temperature_celsius_max='max'
    ).reset_index()
    return agg_df

def calculate_year_on_year_delta(agg_df):
    """
    For each station and calendar month, calculate the year-on-year temperature delta:
      (Current Year Avg Temperature) - (Previous Year Avg Temperature)
    The first available year for each station/month combination will have a null delta.
    """
    # Sort to ensure proper diff calculation
    agg_df = agg_df.sort_values(by=['Station_ID', 'month', 'year'])
    agg_df['temperature_celsius_yoy_avg'] = agg_df.groupby(['Station_ID', 'month'])['temperature_celsius_avg'].diff()
    return agg_df

def join_with_geonames(agg_df, geonames_df):
    """
    Join the aggregated weather data with the geonames dimension table.
    """
    # Mapping from weather station numeric IDs to geonames IDs
    mapping = {26953: "CBCBY", 31688: "EKJCH"}
    agg_df['climate_id'] = agg_df['Station_ID'].map(mapping)
    
    # Merge using the geonames 'id' column and the mapped 'climate_id'
    final_df = pd.merge(agg_df, geonames_df, left_on='climate_id', right_on='id', how='left')
    
    # Select and reorder final columns
    final_df = final_df[[
        'name',
        'id',
        'latitude',
        'longitude',
        'date_month',
        'feature.id',
        'map',
        'temperature_celsius_avg',
        'temperature_celsius_min',
        'temperature_celsius_max',
        'temperature_celsius_yoy_avg'
    ]]
    
    # Rename columns to match final table 
    final_df.rename(columns={
        'name': 'station_name',
        'id': 'climate_id',
        'feature.id': 'feature_id'
    }, inplace=True)
    
    return final_df

def main():
    # Load raw weather and geonames data
    weather_df, geonames_df = load_data()
    
    # Clean and preprocess the weather data
    weather_df = clean_weather_data(weather_df)
    
    # Aggregate data at station and month level
    agg_df = aggregate_weather_data(weather_df)
    
    # Calculate year-on-year temperature differences
    agg_df = calculate_year_on_year_delta(agg_df)
    
    # Join the aggregated weather data with geonames metadata
    final_df = join_with_geonames(agg_df, geonames_df)
    
    # Save the final transformed dataset to CSV
    final_df.to_csv(FINAL_OUTPUT_PATH, index=False)
    print(f"Final transformed data saved to {FINAL_OUTPUT_PATH}")

if __name__ == "__main__":
    main()