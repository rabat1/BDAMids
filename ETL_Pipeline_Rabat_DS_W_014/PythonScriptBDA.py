def run_weather_etl():
import requests
import pandas as pd
from pymongo import MongoClient
import gdown
import os

# ============ STEP 1: EXTRACT DATA FROM MULTIPLE SOURCES ============ #

# 1️⃣ Fetch Real-time Weather Data from WeatherAPI
api_key = os.getenv("WEATHER_API_KEY", "d95b293a864c4da8aaa182800252903")
location = "New York"
weather_api_url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={location}"

try:
    response = requests.get(weather_api_url)
    response.raise_for_status()
    weather_data = response.json()
    weather_df = pd.json_normalize(weather_data)
except Exception as e:
    print(f"⚠️ Error fetching weather data: {e}")
    weather_df = pd.DataFrame()

# 2️⃣ Fetch Historical Weather Data from Open-Meteo API
open_meteo_url = (
    "https://archive-api.open-meteo.com/v1/archive?"
    "latitude=40.71&longitude=-74.00&start_date=2024-01-01&end_date=2024-01-31"
    "&daily=temperature_2m_max&timezone=America/New_York"
)

try:
    response = requests.get(open_meteo_url)
    response.raise_for_status()
    historical_data = response.json()
    historical_df = pd.DataFrame(historical_data.get('daily', {}))
except Exception as e:
    print(f"⚠️ Error fetching historical data: {e}")
    historical_df = pd.DataFrame()

# 3️⃣ Fetch NOAA Climate Data (CSV)
noaa_csv_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2023/01001099999.csv"

try:
    noaa_df = pd.read_csv(noaa_csv_url, low_memory=False)
except Exception as e:
    print(f"⚠️ Error loading NOAA data: {e}")
    noaa_df = pd.DataFrame()

# 4️⃣ Download Google Drive CSV (Simulated)
file_id = "1AQyIk2E2M4Y5Dn3xizjVA_zkBI_JO2Bf"
file_url = f"https://drive.google.com/uc?id={file_id}"
file_path = "weatherHistory.csv"

try:
    gdown.download(file_url, file_path, quiet=False)
    google_drive_df = pd.read_csv(file_path)
    google_drive_df.rename(columns={"Formatted Date": "date", "Temperature (C)": "Temperature"}, inplace=True)
except Exception as e:
    print(f"⚠️ Error downloading Google Drive data: {e}")
    google_drive_df = pd.DataFrame()

# 5️⃣ Fetch MongoDB Data
mongo_uri = os.getenv(
    "MONGO_URI",
    "mongodb+srv://Madiha:#Madiha@cluster0.gq8x6qj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)

try:
    mongo_client = MongoClient(mongo_uri)
    db = mongo_client["weather_db"]
    collection = db["weather_records"]
    mongo_data = list(collection.find({}, {"_id": 0}))
    mongo_df = pd.DataFrame(mongo_data)
except Exception as e:
    print(f"⚠️ Error connecting to MongoDB: {e}")
    mongo_df = pd.DataFrame()

# ============ STEP 2: TRANSFORM DATA ============ #

# Handle Missing Data
for df in [weather_df, historical_df, noaa_df, google_drive_df, mongo_df]:
    df.fillna(method="ffill", inplace=True)
    df.fillna(method="bfill", inplace=True)

# Convert Celsius to Fahrenheit where applicable
if not weather_df.empty and "current.temp_c" in weather_df.columns:
    weather_df["current.temp_f"] = (weather_df["current.temp_c"] * 9 / 5) + 32

if not historical_df.empty:
    historical_df["temperature_2m_max_f"] = (historical_df["temperature_2m_max"] * 9 / 5) + 32

if not google_drive_df.empty:
    google_drive_df["Temperature_F"] = (google_drive_df["Temperature"] * 9 / 5) + 32

# Standardize Date Formats
if not weather_df.empty and "location.localtime" in weather_df.columns:
    weather_df["date"] = pd.to_datetime(weather_df["location.localtime"]).dt.date

if not historical_df.empty:
    historical_df["date"] = pd.to_datetime(historical_df["time"]).dt.date

if not noaa_df.empty and "DATE" in noaa_df.columns:
    noaa_df["date"] = pd.to_datetime(noaa_df["DATE"]).dt.date

if not google_drive_df.empty:
    google_drive_df["date"] = pd.to_datetime(google_drive_df["date"], errors="coerce", utc=True).dt.date

# Remove Duplicates
for df in [weather_df, historical_df, noaa_df, google_drive_df, mongo_df]:
    df.drop_duplicates(inplace=True)

# Aggregate Data: Ensure One Record Per Date
def aggregate_daily(df, temp_column, new_column):
    if not df.empty:
        return df.groupby("date", as_index=False).agg({temp_column: "mean"}).rename(columns={temp_column: new_column})
    return pd.DataFrame()

historical_df = aggregate_daily(historical_df, "temperature_2m_max", "temperature_2m_max")
noaa_df = aggregate_daily(noaa_df, "HourlyDryBulbTemperature", "HourlyDryBulbTemperature")
google_drive_df = aggregate_daily(google_drive_df, "Temperature", "Temperature")

# Weather Condition Categorization
weather_mapping = {
    "Sunny": "Clear",
    "Partly cloudy": "Cloudy",
    "Cloudy": "Cloudy",
    "Overcast": "Cloudy",
    "Rain": "Precipitation",
    "Snow": "Precipitation",
    "Thunderstorm": "Severe Weather"
}

if not weather_df.empty and "current.condition.text" in weather_df.columns:
    weather_df["current.condition.text"] = weather_df["current.condition.text"].map(weather_mapping)

# Keep Only Relevant Columns
weather_df = weather_df[["date", "current.temp_c", "current.temp_f", "current.condition.text"]] if not weather_df.empty else pd.DataFrame()
historical_df = historical_df[["date", "temperature_2m_max"]] if not historical_df.empty else pd.DataFrame()
noaa_df = noaa_df[["date", "HourlyDryBulbTemperature"]] if not noaa_df.empty else pd.DataFrame()
google_drive_df = google_drive_df[["date", "Temperature"]] if not google_drive_df.empty else pd.DataFrame()

# Merge DataFrames
dfs = [weather_df, historical_df, noaa_df, google_drive_df]
final_df = pd.DataFrame()
for df in dfs:
    if not df.empty:
        final_df = df if final_df.empty else final_df.merge(df, on="date", how="outer")

# Drop Empty Columns
final_df.dropna(axis=1, how="all", inplace=True)

# Convert Date to String for MongoDB
final_df["date"] = final_df["date"].astype(str)

# Clean NaN Values
final_records = [{k: v for k, v in record.items() if pd.notna(v)} for record in final_df.to_dict(orient="records")]

# ============ STEP 3: LOAD DATA INTO DATABASE ============ #
if final_records:
    try:
        collection.delete_many({})  # Delete previous records
        collection.insert_many(final_records)  # Insert new records
        print("✅ Data successfully stored in MongoDB!")
    except Exception as e:
        print(f"⚠️ Error inserting into MongoDB: {e}")
else:
    print("⚠️ No valid records to store in MongoDB!")

print("📊 Final Merged DataFrame:")
print(final_df.head())
