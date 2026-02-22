import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import logging
import re
import sys

# =============================================================================
# RESEARCH NOTE: SCALABILITY & COMPLEXITY
# This script uses an inverted-index + nested regex loop (O(N*M)). 
# While optimized for single-country extraction (~30s for DRC), it does not 
# scale linearly for multi-country panels. For global scaling, replace the 
# nested loop with a Trie-based or Aho-Corasick matcher.
# =============================================================================

# --- CONFIGURATION: PORTABLE PATHS ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(SCRIPT_DIR, "..", "..")

KEY_JSON_PATH = os.path.join(SCRIPT_DIR, "google_key.json")
INPUT_ACTOR_FILE = os.path.join(PROJECT_ROOT, "data", "raw_cleaned", "ACLED_cleaned", "actor_list_all_armed_groups.csv")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "GDELT_BigQuery")

TARGET_COUNTRY = "Democratic Republic of Congo"
TARGET_COUNTRY_CODE = "CG"  # FIPS 10-4 Code
START_YEAR = 1997
END_YEAR = 2026

# --- TRANSNATIONAL ACTOR HANDLING ---
# METHODOLOGY NOTE: Task 1 filters actors by their "Modal Country" in ACLED.
# This risks dropping major cross-border groups (e.g. LRA is coded as Uganda).
# We explicitly force-include these known transnational actors for DRC.
TRANSNATIONAL_OVERRIDES = [
    "ADF", "Allied Democratic Forces", 
    "LRA", "Lord's Resistance Army", 
    "FDLR", "Democratic Forces for the Liberation of Rwanda",
    "M23", "March 23 Movement", 
    "Boko Haram" 
]

# --- CONSTANTS (DRC-SPECIFIC) ---
# NOTE: These lists are DRC-SPECIFIC. 
# They must be replaced or expanded if running for other countries.

# Generic terms that should never be searched alone
GENERIC_STOPWORDS = {
    "MILITIA", "REBELS", "REBEL", "GROUP", "FORCES", "GANG", "POLICE", 
    "MILITARY", "ARMY", "FACTION", "MOVEMENT", "FRONT", "VILLAGE", 
    "ATTACKERS", "GUNMEN", "SOLDIERS", "TROOPS", "FIGHTERS"
}

# Context-Aware Safety Filter (Blacklist)
UNSAFE_SINGLE_WORDS = {
    # Western Names (False Positives in global media)
    'CHARLES', 'BENJAMIN', 'DAVID', 'JOHN', 'PETER', 'MICHAEL', 'JAMES',
    'THOMAS', 'THEO', 'ALAIN', 'DOMINIQUE', 'SAMUEL', 'GIDEON',
    'BROWN', 'WHITE', 'GREEN', 'BLACK', 'SMITH', 'JOHNSON',
    # DRC Ethnic Groups (Civilians vs Combatants ambiguity)
    'LENDU', 'HEMA', 'HUTU', 'TUTSI', 'NANDE', 'BANYAMULENGE',
    'TWA', 'LUBA', 'KONGO', 'MONGO',
    # Geography (Locations vs Factions)
    'TORONTO', 'MONTREAL', 'OTTAWA', 'VANCOUVER', 'BOSTON', 'CHICAGO',
    'GOMA', 'BUKAVU', 'KINSHASA', 'LUBUMBASHI', 'ITURI', 'KIVU', 'KATANGA'
}

# --- LOGGING SETUP ---
os.makedirs(OUTPUT_DIR, exist_ok=True)
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')

logging.basicConfig(
    level=logging.INFO, 
    format='%(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, "bigquery_fetch_log.txt"), mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_bigquery_client():
    if not os.path.exists(KEY_JSON_PATH):
        raise FileNotFoundError(f"❌ Missing Google Key file: {KEY_JSON_PATH}")
    credentials = service_account.Credentials.from_service_account_file(KEY_JSON_PATH)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

def fetch_country_events(client, country_code, start_year, end_year):
    """
    Downloads ALL events for target country.
    Uses SQL Integer comparison (SQLDATE) for partition pruning (Cost/Speed Optimization).
    """
    # GDELT 2.0 (2015-Present)
    query_v2 = f"""
        SELECT 
            CAST(SQLDATE AS STRING) as event_date,
            CAST(SUBSTR(CAST(SQLDATE AS STRING), 1, 4) AS INT64) as year,
            Actor1Name, Actor1Code, Actor1Type1Code, Actor1CountryCode,
            Actor2Name, Actor2Code, Actor2Type1Code, Actor2CountryCode,
            EventCode, EventBaseCode, EventRootCode,
            QuadClass, GoldsteinScale, NumMentions, NumSources, NumArticles,
            ActionGeo_Lat as latitude, 
            ActionGeo_Long as longitude,
            ActionGeo_FullName as location,
            ActionGeo_CountryCode as country_code,
            ActionGeo_ADM1Code as admin1,
            SOURCEURL as source_url,
            'GDELT_2.0' as gdelt_version
        FROM `gdelt-bq.gdeltv2.events`
        WHERE 
            ActionGeo_CountryCode = '{country_code}'
            AND SQLDATE >= 20150101
            AND SQLDATE <= {end_year}1231
    """
    
    # GDELT 1.0 (Historical)
    query_v1 = f"""
        SELECT 
            CAST(SQLDATE AS STRING) as event_date,
            Year as year,
            Actor1Name, Actor1Code, Actor1Type1Code, Actor1CountryCode,
            Actor2Name, Actor2Code, Actor2Type1Code, Actor2CountryCode,
            EventCode, EventBaseCode, EventRootCode,
            QuadClass, GoldsteinScale, NumMentions, NumSources, NumArticles,
            ActionGeo_Lat as latitude,
            ActionGeo_Long as longitude,
            ActionGeo_FullName as location,
            ActionGeo_CountryCode as country_code,
            ActionGeo_ADM1Code as admin1,
            SOURCEURL as source_url,
            'GDELT_1.0' as gdelt_version
        FROM `gdelt-bq.full.events`
        WHERE 
            ActionGeo_CountryCode = '{country_code}'
            AND Year >= {start_year}
            AND Year < 2015
    """
    
    logging.info("="*70)
    logging.info("🚀 GDELT BigQuery Data Fetch")
    logging.info("="*70)
    
    logging.info(f"📥 Fetching GDELT 1.0 ({start_year}-2014)...")
    df_v1 = client.query(query_v1).to_dataframe()
    logging.info(f"   ✓ Retrieved {len(df_v1):,} events")
    
    logging.info(f"📥 Fetching GDELT 2.0 (2015-{end_year})...")
    df_v2 = client.query(query_v2).to_dataframe()
    logging.info(f"   ✓ Retrieved {len(df_v2):,} events")
    
    df_combined = pd.concat([df_v1, df_v2], ignore_index=True)
    logging.info(f"\n✅ Total events: {len(df_combined):,}")
    return df_combined

def load_actor_list(file_path, country_filter):
    """
    Loads ACLED list with Transnational Actor Override using REGEX LOOKAROUNDS.
    This ensures precision is maintained even for the overrides.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Input actor file not found: {file_path}")
    
    logging.info(f"\n📋 Loading ACLED actors from: {file_path}")
    df = pd.read_csv(file_path)
    
    # 1. Standard Country Filter
    mask_country = df['country'] == country_filter
    
    # 2. Transnational Override (STRICT REGEX)
    # Replaced loose substring match with lookaround regex to align with precision philosophy
    def matches_override(actor_name):
        name_upper = str(actor_name).strip().upper()
        for override in TRANSNATIONAL_OVERRIDES:
            # Use same logic as main matcher: No alphanumeric neighbors
            pattern = r'(?<![A-Z0-9])' + re.escape(override.upper()) + r'(?![A-Z0-9])'
            if re.search(pattern, name_upper):
                return True
        return False

    mask_override = df['actor_name'].apply(matches_override)
    
    # Combine masks
    df_filtered = df[mask_country | mask_override].copy()
    
    # Logging
    kept_via_override = len(df_filtered) - len(df[mask_country])
    logging.info(f"   Actors matching {country_filter}: {len(df[mask_country])}")
    logging.info(f"   Transnational Actors restored via regex override: {kept_via_override}")
    
    # 3. Generic Name Filter
    ignore_starts = ['Unidentified', 'Unknown', 'Civilians', 'Protesters', 'Rioters', 'Military', 'Police']
    mask_valid = df_filtered['actor_name'].apply(lambda x: not any(str(x).startswith(p) for p in ignore_starts))
    
    return df_filtered[mask_valid].copy()

def create_search_variants(actor_name):
    """
    Creates context-aware search variants.
    """
    name = str(actor_name).strip()
    variants = []
    
    # Remove country tags
    name = re.sub(r'\(Democratic Republic of Congo\)', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\(DRC\)', '', name, flags=re.IGNORECASE)
    name = name.strip()
    
    def is_safe_faction(faction_text):
        faction = faction_text.strip().upper()
        words = faction.split()
        
        if len(words) >= 2: return True
        if any(term in faction for term in ['BRIGADE', 'BATTALION', 'WING', 'DIVISION', 'CORPS']): return True
        if faction in UNSAFE_SINGLE_WORDS: return False
        if len(faction) < 4: return False
        return False 
    
    # --- Acronyms ---
    if ':' in name:
        parts = name.split(':', 1)
        acronym = parts[0].strip().upper()
        full_name = parts[1].strip().upper()
        full_name = re.sub(r'\(.*?\)', '', full_name).strip()
        
        if len(acronym) >= 2 and acronym not in GENERIC_STOPWORDS:
            variants.append(acronym)
            if acronym == "M23": variants.extend(["M-23", "M 23"])
            if acronym == "ADF": variants.extend(["ADF-NALU", "ADF NALU"])
        
        if len(full_name) >= 3 and full_name not in GENERIC_STOPWORDS:
            variants.append(full_name)
            if "MARCH 23" in full_name: variants.append("MARCH 23")
            if "ALLIED DEMOCRATIC" in full_name: variants.append("ALLIED DEMOCRATIC")
            
    # --- Factions ---
    elif '(' in name:
        base = re.sub(r'\(.*?\)', '', name).strip().upper()
        faction_match = re.search(r'\((.*?)\)', name)
        
        if faction_match:
            faction = faction_match.group(1).strip().upper()
            
            if len(base) > 3 and base not in GENERIC_STOPWORDS:
                variants.append(base)
            
            if is_safe_faction(faction):
                variants.append(faction)
            else:
                if len(base) > 0 and base not in GENERIC_STOPWORDS:
                    variants.append(f"{base} {faction}")
                
                if len(faction) > 2:
                    variants.append(f"{faction} MILITIA")
                    variants.append(f"{faction} REBELS")
                    
    # --- Simple Names ---
    else:
        clean = name.upper().strip()
        if len(clean) >= 3 and clean not in GENERIC_STOPWORDS:
            variants.append(clean)
            
    return list(set(variants))

def match_actors(events_df, actors_df):
    """
    Filters events using Inverted Index + Lookaround Regex.
    METHODOLOGY NOTE: Prioritizes precision over recall. 
    Recall loss may be structured and higher in:
    - Francophone reporting (accents/diacritics)
    - Lower-resource media (OCR errors, non-standard punctuation like A.D.F.)
    - Earlier years (1997-2005)
    """
    logging.info("\n⚔️  Matching ACLED actors...")
    logging.info("   Building ACLED actor search variants...")
    
    acled_search_dict = {}
    for _, row in actors_df.iterrows():
        variants = create_search_variants(row['actor_name'])
        for v in variants:
            if v in GENERIC_STOPWORDS: continue
            v_upper = v.upper()
            if v_upper not in acled_search_dict: 
                acled_search_dict[v_upper] = []
            acled_search_dict[v_upper].append(row['actor_name'])
            
    logging.info(f"   Generated {len(acled_search_dict)} unique search variants")
    
    logging.info("   Extracting unique GDELT actor names...")
    unique_gdelt_actors = set()
    unique_gdelt_actors.update(events_df['Actor1Name'].dropna().unique())
    unique_gdelt_actors.update(events_df['Actor2Name'].dropna().unique())
    
    unique_gdelt_actors = {str(x).strip().upper() for x in unique_gdelt_actors 
                          if pd.notna(x) and len(str(x).strip()) > 2}
    
    logging.info(f"   Found {len(unique_gdelt_actors):,} unique actor names to scan")
    
    gdelt_to_acled = {}
    
    for gdelt_name in unique_gdelt_actors:
        matches = set()
        for acled_variant, acled_names in acled_search_dict.items():
            pattern = r'(?<![A-Z0-9])' + re.escape(acled_variant) + r'(?![A-Z0-9])'
            if re.search(pattern, gdelt_name):
                matches.update(acled_names)
        
        if matches:
            gdelt_to_acled[gdelt_name] = list(matches)
            
    logging.info(f"   Matched {len(gdelt_to_acled)} unique GDELT names")
    
    # Save Mapping
    mapping_df = pd.DataFrame([
        {'gdelt_actor_name': k, 'matched_acled_actors': '; '.join(v)}
        for k, v in gdelt_to_acled.items()
    ])
    mapping_df.to_csv(os.path.join(OUTPUT_DIR, "gdelt_acled_actor_mapping.csv"), index=False)
    
    # Map back to DataFrame
    def lookup_matches(row):
        matches = set()
        a1 = str(row['Actor1Name']).strip().upper()
        a2 = str(row['Actor2Name']).strip().upper()
        if a1 in gdelt_to_acled: matches.update(gdelt_to_acled[a1])
        if a2 in gdelt_to_acled: matches.update(gdelt_to_acled[a2])
        return list(matches) if matches else None
    
    events_df['matched_acled_actors'] = events_df.apply(lookup_matches, axis=1)
    events_df['has_acled_actor'] = events_df['matched_acled_actors'].notna()
    
    # --- DIAGNOSTICS: Split "Non-Local" into specific flags ---
    
    def check_flags(row):
        # 1. Missing Data
        a1_missing = pd.isna(row['Actor1CountryCode'])
        a2_missing = pd.isna(row['Actor2CountryCode'])
        
        # 2. Foreign Actors (Potential Transnational)
        a1_foreign = (not a1_missing) and (row['Actor1CountryCode'] != TARGET_COUNTRY_CODE)
        a2_foreign = (not a2_missing) and (row['Actor2CountryCode'] != TARGET_COUNTRY_CODE)
        
        return pd.Series([a1_missing or a2_missing, a1_foreign or a2_foreign])

    events_df[['actor_country_missing', 'actor_country_foreign']] = events_df.apply(check_flags, axis=1)
    
    # Flatten list
    events_df.loc[events_df['has_acled_actor'], 'matched_acled_actors'] = \
        events_df.loc[events_df['has_acled_actor'], 'matched_acled_actors'].apply(lambda x: '; '.join(x))
        
    return events_df

def main():
    try:
        logging.info("="*70)
        logging.info("GDELT BIGQUERY EXTRACTION (RESEARCH GRADE)")
        logging.info("="*70)
        
        client = get_bigquery_client()
        actors_df = load_actor_list(INPUT_ACTOR_FILE, TARGET_COUNTRY)
        all_events = fetch_country_events(client, TARGET_COUNTRY_CODE, START_YEAR, END_YEAR)
        
        final_df = match_actors(all_events, actors_df)
        
        # Save FULL
        output_full = os.path.join(OUTPUT_DIR, f"GDELT_Events_{TARGET_COUNTRY.replace(' ', '_')}_FULL.csv")
        final_df.to_csv(output_full, index=False)
        
        # Save MATCHED
        matched_df = final_df[final_df['has_acled_actor']].copy()
        output_matched = os.path.join(OUTPUT_DIR, f"GDELT_Events_{TARGET_COUNTRY.replace(' ', '_')}_MATCHED.csv")
        matched_df.to_csv(output_matched, index=False)
        
        # Stats
        logging.info(f"\n💾 MATCH STATISTICS:")
        logging.info(f"   Total Events: {len(final_df):,}")
        logging.info(f"   Matched Events: {len(matched_df):,}")
        logging.info(f"   Events with Foreign Actors: {matched_df['actor_country_foreign'].sum()}")
        logging.info(f"   Events with Missing Actor Country: {matched_df['actor_country_missing'].sum()}")
        logging.info("\n✅ EXTRACTION COMPLETE!")

    except Exception as e:
        logging.error(f"❌ ERROR: {e}")
        import traceback
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    main()