# scheduler.py

import schedule
import time
from PythonScriptBDA import run_weather_etl

# Schedule the job to run every 6 hours
schedule.every(6).hours.do(run_weather_etl)

print("📆 Scheduler started. Waiting to run tasks...")

while True:
    schedule.run_pending()
    time.sleep(60)  # wait a minute between checks
