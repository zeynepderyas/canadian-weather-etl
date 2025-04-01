# canadian-weather-etl
A Python ETL pipeline to fetch, clean, and analyze Canadian daily weather data with geolocation enrichment.


This project is all about collecting and processing daily weather data from Canada's official climate API. It automatically downloads data for specific weather stations and time periods, checks the quality of the data, groups it by month, and finally merges it with geographic metadata from a separate file. The result is a clean, enriched dataset that's ready for analysis.

The data pipeline is written in Python and works in two stages. The first script, extract_data.py, connects to the API and fetches daily weather data for selected station IDs (e.g. 26953 and 31688) for the years 2023 and 2024. For each day, it performs basic data quality checks such as detecting missing values, identifying outliers, and making sure there are no missing dates. All raw data is saved to a folder named raw_data, and a combined version of all data is saved as combined_weather_data.csv.

The second script, transform_data.py, picks up where the first one left off. It cleans the raw data, filters out any invalid or future dates, and then groups the data by station and month. For each group, it calculates the average, minimum, and maximum temperatures, and even computes year-over-year temperature changes for the same calendar month. Then, it joins the results with a metadata file (geonames.csv) to add location info like station names and coordinates. The final processed dataset is saved as final_output.csv.

This pipeline provides a complete, end-to-end process for building a clean weather dataset with both temporal and spatial insights. It's a great starting point for data science projects involving climate analysis, trend detection, or geographic visualization.

Note:
follow these steps to run the project via terminal;
1. Navigate to the project folder on your Desktop (e.g., 'buluttan')
cd ~/Desktop/buluttan

2. Install required Python libraries
pip install pandas requests

3. Fetch weather data from the API (this step may take some time)
python extract_data.py

4. Clean and aggregate the data, then generate the final output
python transform_data.py

5. The final_output.csv file will be saved in your current directory

