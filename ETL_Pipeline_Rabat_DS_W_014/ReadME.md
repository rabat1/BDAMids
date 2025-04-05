# BDAMids
# 🌤️ Weather Data ETL Pipeline

This project implements a weather analytics ETL (Extract, Transform, Load) pipeline using multiple data sources including APIs, CSV files, Google Drive, and databases (MongoDB and PostgreSQL). The pipeline consolidates and cleans weather data for analysis and trend detection.

## 📌 Project Features

- ✅ **Extracts data** from:
  - WeatherAPI (Real-time weather)
  - Open-Meteo (Historical weather)
  - NOAA (CSV)
  - Google Drive (Simulated CSV)
  - MongoDB Atlas (NoSQL)
- 🧹 **Transforms data** by:
  - Filling missing values
  - Converting temperatures
  - Standardizing date formats
  - Aggregating and merging multiple datasets
- 💾 **Loads cleaned data** into:
  - MongoDB Atlas
- 🔁 **Automated Scheduling**:
  - Uses a scheduler (`schedule` module) to automate the ETL pipeline at regular intervals

---

## 🛠️ Technologies Used

- Python
- Pandas
- Requests
- Pymongo
- gdown
- Schedule
- Google Colab (optional)

---

## 📂 Project Structure

```
📁 weather-etl-pipeline/
├── weather_etl.py         # Main ETL logic
├── scheduler.py           # Scheduler to run ETL job periodically
├── db_config.json         # MongoDB URI and API key configuration
├── requirements.txt       # Python dependencies
└── README.md              # Project overview
```

---

## ⚙️ Setup Instructions

1. **Clone the Repository**

```bash
git clone https://github.com/your-username/weather-etl-pipeline.git
cd weather-etl-pipeline
```

2. **Install Dependencies**

```bash
pip install -r requirements.txt
```

3. **Update Configuration**

Edit `db_config.json`:

```json
{
  "MONGO_URI": "your_mongodb_uri_here",
  "WEATHER_API_KEY": "your_weatherapi_key_here"
}
```

4. **Run the ETL Pipeline**

```bash
python weather_etl.py
```

5. **Run the Scheduler**

```bash
python scheduler.py
```

---

## 📈 Output

- Final cleaned and merged dataset is printed to console
- Data is stored in MongoDB `weather_db.weather_records`
- Ready for analytics and visualizations

---

## 📚 Data Sources

- [WeatherAPI](https://www.weatherapi.com/)
- [Open-Meteo](https://open-meteo.com/)
- [NOAA CSV](https://www.ncei.noaa.gov/)
- [Google Drive CSV File](https://drive.google.com/)

---

## ✍️ Author

**Rabat**  
*Big Data Analytics – Weather Data ETL Project*
