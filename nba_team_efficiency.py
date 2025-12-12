import pandas as pd
import requests
import time
import random
import os

# --- Configuration ---
URL = "https://www.espn.com/nba/hollinger/teamstats"
# 🌟 REQUIRED FILENAME: Use the specified CSV name
OUTPUT_FILENAME = "nba_team_efficiency.csv" 

# Column indices confirmed to pull Offensive and Defensive Efficiency (Hollinger Ratings)
TEAM_COL_INDEX = 1
OFF_EFF_COL_INDEX = 10 
DEF_EFF_COL_INDEX = 11

# Add a realistic User-Agent header to avoid 403 error
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

print(f"Starting NBA Team Efficiency Scraper from ESPN Hollinger...")

try:
    # 1. Introduce a random sleep timer (2 to 5 seconds)
    sleep_time = random.uniform(2, 5)
    print(f"Pausing for {sleep_time:.2f} seconds...")
    time.sleep(sleep_time) 

    # 2. Use requests with the custom headers
    response = requests.get(URL, headers=HEADERS)
    response.raise_for_status() 

    # 3. Read HTML tables
    tables = pd.read_html(response.text)

    # 4. Identify the main stats table
    main_df = None
    for table in tables:
        if len(table) >= 30 and table.shape[1] > DEF_EFF_COL_INDEX:
            main_df = table
            break
            
    if main_df is None:
        print("🛑 Error: Could not identify the main NBA team stats table. Exiting.")
        exit()
        
    # 5. Clean and select relevant columns
    main_df = main_df[~main_df.iloc[:, 0].astype(str).str.contains('Rk|RK', case=False, na=False)]

    stats_df = main_df.iloc[:, [TEAM_COL_INDEX, OFF_EFF_COL_INDEX, DEF_EFF_COL_INDEX]].copy()
    
    stats_df.columns = ['TEAM_NAME', 'OFF_EFF_ORtg', 'DEF_EFF_DRtg']
    stats_df = stats_df.dropna(subset=['TEAM_NAME'])
    
    # --- 6. SAVE TO CSV with required filename ---
    stats_df.to_csv(OUTPUT_FILENAME, index=False)
    
    # 7. Confirmation Message
    print("\n## ✅ Success: Data Scraped and Saved")
    print(f"File created: {os.path.abspath(OUTPUT_FILENAME)}")
    
except requests.exceptions.RequestException as e:
    print(f"🛑 Failed to fetch data. Details: {e}")
except Exception as e:
    print(f"🛑 An unexpected error occurred: {e}")
