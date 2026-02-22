import os, re, glob, json, time, textwrap, ast
import pandas as pd
import pdfplumber
from google import genai
from google.genai import types

# ======================= CONFIG (EDIT FOR EACH BRAND) ========================

BRAND          = "New Balance"                        
BRAND_ALIASES  = ["New Balance"]       

BASE_DIR       = r"./data/Brands_Articles"                    
BRANDS_ROOT    = os.path.join(BASE_DIR, "Brands_Articles")
BRAND_PDF_DIR  = os.path.join(BRANDS_ROOT, BRAND)   

# Global media->country file
MEDIA_COUNTRY_GLOBAL = os.path.join(BASE_DIR, "Rana_Plaza", "factiva_titlelist_with_countries.xlsx")

# Year filter
YEAR_FILTER = 2013

# Brand-specific outputs
BRAND_LONG_CSV     = os.path.join(BRAND_PDF_DIR, f"{BRAND}_factiva_parsed_tags_long.csv")
BRAND_WIDE_CSV     = os.path.join(BRAND_PDF_DIR, f"{BRAND}_factiva_parsed_tags_wide.csv")
BRAND_RELEV_CSV    = os.path.join(BRAND_PDF_DIR, f"{BRAND}_relevance_labels.csv")
BRAND_CACHE_JSON   = os.path.join(BRAND_PDF_DIR, f"{BRAND}_relevance_cache.json")
BRAND_COUNTRY_CSV  = os.path.join(BRAND_PDF_DIR, f"{BRAND}_country_month_counts.csv")
BRAND_FINAL_XLSX   = os.path.join(BRAND_PDF_DIR, f"{BRAND}_final_database.xlsx")

# Factiva Tag Regex
TAG_LINE_RE = re.compile(r"^([A-Z]{2,4})(\s+.*)?$")
PAGE_FOOTER_RE = re.compile(r"Page \d+ of \d+.*Factiva", re.IGNORECASE)

# Language -> "obvious" audience country mapping
LANG_TO_COUNTRY = {
    "danish": "Denmark", "finnish": "Finland", "greek": "Greece",
    "hungarian": "Hungary", "polish": "Poland", "czech": "Czech Republic",
    "romanian": "Romania", "swedish": "Sweden", "norwegian": "Norway",
    "icelandic": "Iceland", "dutch": "Netherlands", "german": "Germany",
    "slovak": "Slovakia", "slovenian": "Slovenia", "bulgarian": "Bulgaria",
    "croatian": "Croatia", "serbian": "Serbia", "estonian": "Estonia",
    "latvian": "Latvia", "lithuanian": "Lithuania",
}

# ======================= GEMINI CONFIG ========================

# PASTE YOUR KEY HERE
API_KEY = "" 

# Model Selection:
GEMINI_MODEL = "gemini-2.5-flash"

# The limit is 5 requests/min. 
# We sleep 15 seconds to stay safe (4 requests/min).
SLEEP_BETWEEN = 0.1

# Initialize Client
client = genai.Client(api_key=API_KEY)


# ====================== PARSING LOGIC  ==========================

def parse_pdf_linear(path):
    rows = []
    with pdfplumber.open(path) as pdf:
        article_idx = -1
        cur_tag, cur_val = None, []
        
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=2, y_tolerance=3)
            if not text: continue
            lines = text.split('\n')
            
            for line in lines:
                line = line.strip()
                if not line: continue
                if PAGE_FOOTER_RE.search(line): continue

                match = TAG_LINE_RE.match(line)
                is_tag = False
                if match:
                    code_cand = match.group(1)
                    if code_cand in {"HD","TD","SN","PD","WC","SC","CO","IN","NS","RE","IPD","PUB","AN","LA","CY","SE","CR","ED"}:
                        is_tag = True
                        tag_code = code_cand
                        content_remainder = match.group(2).strip() if match.group(2) else ""

                if is_tag:
                    if cur_tag and article_idx >= 0:
                        full_text = " ".join(cur_val).strip()
                        if full_text:
                            rows.append((article_idx, cur_tag, full_text))
                    
                    if tag_code == "HD":
                        article_idx += 1
                        cur_tag = "HD"
                        cur_val = [content_remainder] if content_remainder else []
                    else:
                        cur_tag = tag_code
                        cur_val = [content_remainder] if content_remainder else []
                else:
                    if cur_tag and article_idx >= 0:
                        cur_val.append(line)
        
        if cur_tag and article_idx >= 0:
            full_text = " ".join(cur_val).strip()
            if full_text:
                rows.append((article_idx, cur_tag, full_text))

    return rows

def parse_folder(pdf_dir):
    data = []
    files = sorted(glob.glob(os.path.join(pdf_dir, "*.pdf")))
    if not files:
        print(f"WARNING: No PDF files found in {pdf_dir}")
        
    for path in files:
        print(f"Parsing: {os.path.basename(path)}...")
        rows = parse_pdf_linear(path)
        base = os.path.basename(path)
        for art_idx, code, text in rows:
            data.append((base, art_idx, code, text))
            
    df = pd.DataFrame(data, columns=["pdf_file","article_index","code","text"])
    return df

def to_wide(df_long):
    if df_long.empty: return pd.DataFrame()
    df_long = df_long.drop_duplicates(subset=["pdf_file","article_index","code"])
    wide = df_long.pivot_table(index=["pdf_file","article_index"], columns="code",
                               values="text", aggfunc="first")
    wide = wide.reset_index()
    wide.columns.name = None
    return wide

def normalize(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def build_article_text(row) -> str:
    cols = ["HD", "LP", "TD", "CO", "IN", "NS", "RE"]
    parts = []
    for c in cols:
        v = str(row.get(c, "") or "").strip()
        if v: parts.append(v)
    return " \n ".join(parts)

def contains_brand(text: str, aliases) -> bool:
    t = normalize(text)
    t_nospace = t.replace(" ", "")
    for a in aliases:
        a_norm = normalize(a)
        if re.search(rf"\b{re.escape(a_norm)}\b", t): return True
        if re.escape(a_norm.replace(" ", "")) in t_nospace: return True
    return False

def prefilter_brand_mention(row, brand, aliases) -> bool:
    text = build_article_text(row)
    return contains_brand(text, [brand] + list(aliases))

# ====================== GEMINI API / PROCESSING ==========================

def make_prompt(brand: str, aliases, row) -> str:
    hd = str(row.get("HD","") or "")
    sn = str(row.get("SN","") or "")
    pd_ = str(row.get("PD","") or "")

    text = build_article_text(row)
    # Gemini has a 2M token context, so we do NOT need to truncate aggressively.
    # text = text[:7000] <-- Removed truncation for better accuracy

    alias_str = ", ".join(sorted(set([brand] + list(aliases))))

    prompt = f"""
You are a careful Factiva article classifier. Do NOT guess.

Target Brand: "{brand}"
Aliases: {alias_str}

Definitions:
- CENTRAL mention = article is mainly about the brand (company actions, corporate news, products/campaigns, CSR, legal cases involving brand, supply chain involving brand).
- PASSING mention = brand appears but is not important to the story.
- LIST/SHOPPING = brand appears as one of many items, product listings, directories, “best brands”, price tables.
- HOMONYM = same word but not the brand/company (person name, place, unrelated term).

Rana Plaza:
Mark Yes ONLY if the article explicitly mentions "Rana Plaza" OR clearly refers to the 2013 Bangladesh factory collapse.

Return JSON ONLY with:
{{
  "brand_relevant": true/false,
  "brand_mention_type": "central|passing|list|homonym|none",
  "brand_evidence_sentences": ["...","..."],
  "rana_plaza": "Yes|No"
}}

Rules:
1) If you cannot quote at least ONE sentence from the text that contains the brand/alias in context, set brand_relevant=false and brand_mention_type="none".
2) If the only evidence is list/directory/price table, set brand_relevant=false.
3) If evidence suggests a person/place with the same name, set brand_relevant=false.
4) Otherwise brand_relevant=true ONLY if CENTRAL.

Metadata:
Headline: {hd}
Source: {sn}
Date: {pd_}

Text:
{text}
"""
    return textwrap.dedent(prompt).strip()

def call_gemini(prompt: str) -> str:
    """
    Calls Google Gemini API. Handles simple rate limits by waiting.
    """
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.0,
                response_mime_type="application/json"
            )
        )
        return response.text.strip()
    except Exception as e:
        print(f"\nGemini Error: {e}")
        if "429" in str(e):
            print("Hit Rate Limit (429). Sleeping 60s...")
            time.sleep(60)
        return ""

def safe_parse_json(raw: str) -> dict:
    raw = raw.strip()
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match: raw = match.group(0)

    try:
        return json.loads(raw)
    except:
        pass
    
    # Fallback Regex (should rarely be needed with Gemini JSON mode)
    brand_rel = bool(re.search(r'[\'"]brand_relevant[\'"]\s*:\s*true', raw, re.IGNORECASE))
    rana_val = "No"
    if re.search(r'[\'"]rana_plaza[\'"]\s*:\s*[\'"]?Yes', raw, re.IGNORECASE):
        rana_val = "Yes"
        
    return {"brand_relevant": brand_rel, "rana_plaza": rana_val, "raw": raw}

def lang_audience_country(la: str) -> str | None:
    if not isinstance(la, str) or not la.strip(): return None
    la_norm = la.lower()
    for key, country in LANG_TO_COUNTRY.items():
        if key in la_norm: return country
    return None

def evidence_contains_brand(evidence_sentences, brand, aliases):
    if not evidence_sentences: return False
    joined = " ".join(evidence_sentences)
    return contains_brand(joined, [brand] + list(aliases))

def enforce_rules(parsed, brand, aliases):
    mtype = (parsed.get("brand_mention_type") or "").lower()
    ev = parsed.get("brand_evidence_sentences") or []
    
    # Rule 1: No quoted evidence mentioning brand -> NOT relevant
    if not evidence_contains_brand(ev, brand, aliases):
        parsed["brand_relevant"] = False
        parsed["brand_mention_type"] = "none"

    # Rule 2: Only CENTRAL counts as relevant
    if mtype != "central":
        parsed["brand_relevant"] = False

    return parsed

# ========================= PIPELINE STEPS ===================================

def step1_parse_pdfs():
    print(f"[Step 1] Parsing PDFs for brand: {BRAND}")
    os.makedirs(BRAND_PDF_DIR, exist_ok=True)
    
    df_long = parse_folder(BRAND_PDF_DIR)
    if df_long.empty:
        print("ERROR: No data parsed. Check PDF location.")
        return

    df_wide = to_wide(df_long)
    df_long.to_csv(BRAND_LONG_CSV, index=False)
    df_wide.to_csv(BRAND_WIDE_CSV, index=False)
    print(f"Saved parsed data to: {BRAND_WIDE_CSV}")
    print(f"Total articles found: {len(df_wide)}")

def step2_llm_relevance():
    print(f"[Step 2] Gemini classification for brand: {BRAND}")
    if not os.path.exists(BRAND_WIDE_CSV):
        print("Run Step 1 first!")
        return

    df = pd.read_csv(BRAND_WIDE_CSV, dtype=str)
    
    # Load cache
    cache = {}
    if os.path.exists(BRAND_CACHE_JSON):
        with open(BRAND_CACHE_JSON,"r",encoding="utf-8") as f:
            cache = json.load(f)

    results = []
    print(f"Classifying {len(df)} articles using {GEMINI_MODEL}...")
    
    # Counter for periodic saving
    count_since_save = 0
    SAVE_EVERY = 50 

    for idx, row in df.iterrows():
        key = f"{row['pdf_file']}|{row['article_index']}"

        # 1. Prefilter: Skip if brand not textually present
        if not prefilter_brand_mention(row, BRAND, BRAND_ALIASES):
            results.append({
                "pdf_file": row["pdf_file"],
                "article_index": row["article_index"],
                "brand_relevant": False,
                "Mention_of_Rana_Plaza": "No",
                "raw_llm": "SKIPPED_PREFILTER"
            })
            continue
        
        # 2. Check Cache
        cached_val = cache.get(key)
        if cached_val and "rana_plaza" in str(cached_val):
            parsed = cached_val if "raw" not in cached_val else safe_parse_json(cached_val["raw"])
            parsed = enforce_rules(parsed, BRAND, BRAND_ALIASES)
        else:
            # 3. Call API
            prompt = make_prompt(BRAND, BRAND_ALIASES, row)
            raw_resp = call_gemini(prompt) # <--- Using Gemini now
            parsed = safe_parse_json(raw_resp)
            parsed = enforce_rules(parsed, BRAND, BRAND_ALIASES)
            parsed["raw"] = raw_resp
            cache[key] = parsed
            
            # Rate limit sleep
            time.sleep(SLEEP_BETWEEN)
            
            count_since_save += 1
            if count_since_save >= SAVE_EVERY:
                with open(BRAND_CACHE_JSON,"w",encoding="utf-8") as f:
                    json.dump(cache, f, ensure_ascii=False, indent=2)
                count_since_save = 0
                print(f" [Auto-saved at article {idx+1}]")

        print(f"Processed {idx+1}/{len(df)}...", end="\r")

        results.append({
            "pdf_file": row["pdf_file"],
            "article_index": row["article_index"],
            "brand_relevant": parsed.get("brand_relevant", False),
            "Mention_of_Rana_Plaza": parsed.get("rana_plaza", "No"),
            "raw_llm": parsed.get("raw", "")
        })

    print("")
    # Final Save
    with open(BRAND_CACHE_JSON,"w",encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    out_df = pd.DataFrame(results)
    out_df.to_csv(BRAND_RELEV_CSV, index=False)
    
    print(f"Saved relevance labels -> {BRAND_RELEV_CSV}")
    print(f"Brand Relevant: {out_df['brand_relevant'].sum()}")
    print(f"Rana Plaza Mentions: {(out_df['Mention_of_Rana_Plaza']=='Yes').sum()}")

def step3_country_and_panel():
    print(f"\n[Step 3] Aggregation for brand: {BRAND}")
    
    if not os.path.exists(BRAND_WIDE_CSV) or not os.path.exists(BRAND_RELEV_CSV):
        print("Missing input files.")
        return

    dfw = pd.read_csv(BRAND_WIDE_CSV, dtype=str, keep_default_na=False)
    rel = pd.read_csv(BRAND_RELEV_CSV, dtype={"pdf_file":str,"article_index":str})
    
    dfw["article_index"] = dfw["article_index"].astype(str)
    merged = dfw.merge(rel, on=["pdf_file","article_index"], how="left")
    
    merged["brand_relevant"] = merged["brand_relevant"].astype(str).str.lower() == "true"
    final_df = merged[merged["brand_relevant"]].copy()
    
    if os.path.exists(MEDIA_COUNTRY_GLOBAL):
        map_df = pd.read_excel(MEDIA_COUNTRY_GLOBAL, dtype=str, keep_default_na=False)
        map_df.columns = map_df.columns.str.strip()
        tgt_col = "country" if "country" in map_df.columns else "audience_country"
        sn_map = dict(zip(map_df["Source Code"].astype(str), map_df[tgt_col].astype(str)))

        final_df["Country_global"] = final_df["SC"].map(sn_map).fillna(final_df["SN"].map(sn_map))
        final_df["Country_lang"] = final_df["LA"].apply(lang_audience_country)
        final_df["Country"] = final_df["Country_global"].replace(r"^\s*$", pd.NA, regex=True)
        final_df["Country"] = final_df["Country"].fillna(final_df["Country_lang"])
        final_df["Country"] = final_df["Country"].fillna("NO country mapping")
    else:
        print("WARNING: Country mapping Excel not found.")
        final_df["Country"] = "Unknown"

    detailed_path = os.path.join(BRAND_PDF_DIR, f"{BRAND}_detailed_relevance_check.csv")
    final_df.to_csv(detailed_path, index=False)
    print(f"Saved detailed list -> {detailed_path}")

    final_df["dt"] = pd.to_datetime(final_df["PD"], dayfirst=True, errors="coerce")
    final_df["month"] = final_df["dt"].dt.month
    
    if YEAR_FILTER:
        final_df = final_df[final_df["dt"].dt.year == YEAR_FILTER]

    if final_df.empty:
        print("No relevant data for aggregation.")
        return

    grp = final_df.groupby(["Country","month"], as_index=False).agg(
        Number_of_Articles=("brand_relevant", "count"),
        Rana_Plaza_Mentions=("Mention_of_Rana_Plaza", lambda x: (x=="Yes").sum())
    )
    
    grp.insert(0, "Brand", BRAND)
    grp.rename(columns={"month":"Time"}, inplace=True)
    
    grp.to_csv(BRAND_COUNTRY_CSV, index=False)
    grp.to_excel(BRAND_FINAL_XLSX, index=False)
    print(f"Saved Panel -> {BRAND_COUNTRY_CSV}")
    print(grp.head())

if __name__ == "__main__":
    step1_parse_pdfs()      
    step2_llm_relevance()   
    step3_country_and_panel()