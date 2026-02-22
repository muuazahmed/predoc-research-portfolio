print('-----------------START ACLED PROCESSING-------------------------')
import pandas as pd
import numpy as np
import os
import sys

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Navigate from code_Muaz/build/ -> Project_MNC_conflict (Root)
PROJECT_ROOT = os.path.join(SCRIPT_DIR, "..", "..") 

# INPUT PATH
# Relative path: data/raw/ACLED/ACLED_Africa.csv
INPUT_FILE = os.path.join(PROJECT_ROOT, "data", "raw", "ACLED", "ACLED_Africa.csv")

# OUTPUT PATH
# Relative path: data/raw_cleaned/ACLED_cleaned/
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "raw_cleaned", "ACLED_cleaned")


# Output filename
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "Event_Level_Dataset.csv")

###########################################
##### PROCESSING #####
###########################################

print(f"Loading raw data from: {INPUT_FILE}")
try:
    # Load raw data
    df = pd.read_csv(INPUT_FILE, sep=',')
except FileNotFoundError:
    print(f"❌ Error: Could not find file at {INPUT_FILE}")
    sys.exit(1)

print(f"Initial rows: {len(df):,}")

# 1. Filter: Keep only Violent Event Types
# (Battles, Explosions/Remote violence, Violence against civilians)
violent_types = [
    'Battles', 
    'Explosions/Remote violence', 
    'Violence against civilians'
]
df_violent = df[df['event_type'].isin(violent_types)].copy()
print(f"Rows after filtering for violence: {len(df_violent):,}")

# 2. Filter: Keep Non-State Actors
# ACLED Interaction Codes:
# 1 = State Forces
# 2 = Rebel Groups
# 3 = Political Militias
# 4 = Identity Militias
# 5 = Rioters
# 6 = Protesters
# 7 = Civilians
# 8 = External/Other Forces

# We want events where AT LEAST ONE side is a Non-State Armed Group (2 or 3 or 4)

nonstate_codes = [2, 3, 4] 

mask_nonstate = (df_violent['inter1'].isin(nonstate_codes)) | \
                (df_violent['inter2'].isin(nonstate_codes))

df_final = df_violent[mask_nonstate].copy()
print(f"Rows after filtering for non-state actors: {len(df_final):,}")

# 3. Clean Dates
# Convert event_date to datetime objects
df_final['event_date'] = pd.to_datetime(df_final['event_date'])
df_final['year'] = df_final['event_date'].dt.year
df_final['month'] = df_final['event_date'].dt.month
# Create integer date format YYYYMMDD for compatibility with old Stata workflow
df_final['date_int'] = df_final['event_date'].dt.strftime('%Y%m%d').astype(int)

# 4. Generate Unique Group IDs (Optional but good for tracking)
# This creates a simple code for every unique actor name found in actor1
df_final['actor1_id'] = df_final['actor1'].astype('category').cat.codes + 10000

# 5. Create Event-Level Variables (Violence flags)
# Violence against civilians (Interaction 27 or 37 usually, but ACLED varies)
# We use the sub_event_type for cleaner logic if available, or interaction codes.
df_final['vio_civilian'] = (df_final['event_type'] == 'Violence against civilians').astype(int)

# 6. Select Final Columns
cols_to_keep = [
    'event_id_cnty', 'event_date', 'year', 'month', 'date_int',
    'event_type', 'sub_event_type', 
    'actor1', 'assoc_actor_1', 'inter1',
    'actor2', 'assoc_actor_2', 'inter2',
    'interaction',
    'country', 'admin1', 'location', 
    'latitude', 'longitude', 'geo_precision',
    'fatalities', 'notes'
]

# Ensure we only keep columns that actually exist in the raw data
final_cols = [c for c in cols_to_keep if c in df_final.columns]
df_export = df_final[final_cols]

###########################################
##### EXPORT #####
###########################################
print(f"Saving {len(df_export):,} events to: {OUTPUT_FILE}")
df_export.to_csv(OUTPUT_FILE, index=False)
print("DONE")