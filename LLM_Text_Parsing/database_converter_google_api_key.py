import os, re, glob, json, time, textwrap, ast
import pandas as pd
import pdfplumber
from google import genai
from google.genai import types
import concurrent.futures
import threading

# ======================= CONFIG (EDIT FOR EACH BRAND) ========================

BRAND          = "Mango"                        
BRAND_ALIASES  = ["Mango"]       

BASE_DIR       = r"C:\Data\Downloading"                    
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

# The free tier limit is 5 requests/min. 
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
You are a careful Factiva article classifier. Do NOT guess. Do NOT paraphrase or invent sentences.
Target Brand: "{brand}"
Aliases: {alias_str}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DEFINITIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CENTRAL mention = The article is mainly about the brand (company actions, corporate news,
products/campaigns, CSR, legal cases involving the brand, supply chain reporting).

  CRISIS EXCEPTION: If the article explicitly names Rana Plaza (qualifying under the
  Rana Plaza Definition below) AND explicitly states that the brand was directly
  connected to Rana Plaza — including but not limited to: sourced from the factory,
  produced garments at the factory, was linked to factories in Rana Plaza, audited the
  factory, was exposed or implicated by the collapse, was named in legal or compensation
  proceedings, or took a substantive public action directly in response to the collapse
  (e.g., signed the Accord, issued a formal statement, made compensation payments) —
  this counts as CENTRAL regardless of how briefly it is mentioned.
  IMPORTANT: The brand must be named explicitly and tied directly to Rana Plaza itself —
  not merely mentioned in the same article, and not merely described as having improved
  supply chain practices "since Rana Plaza" or "after 2013" in general terms.
  Implicit or industry-wide statements (e.g., "European retailers faced scrutiny after
  the collapse") do NOT trigger this exception unless the brand is named explicitly.
  A generic expression of condolences or a bare appearance in a list of brand names does
  NOT trigger this exception — see the LIST/CRISIS carve-out below.

PASSING mention = The brand appears in the article text but is not important to the main
story (e.g., used as a passing example, background colour, or comparison point).

LIST/SHOPPING = The brand appears as one item among many in a product listing, price
table, directory, "best brands" roundup, or similar enumeration.
  EXCEPTION — LIST/CRISIS CARVE-OUT: A list whose explicit purpose is to name brands
  connected to the Rana Plaza collapse (e.g., "brands that sourced from Rana Plaza",
  "companies implicated in the disaster") is NOT treated as LIST/SHOPPING. Instead,
  apply the Crisis Exception above to determine whether the mention is CENTRAL or PASSING.
  Even a single appearance in such a list qualifies as CENTRAL — brevity alone does not
  demote the mention to PASSING when the list's purpose is to document Rana Plaza ties.

HOMONYM = The same word appears but does not refer to the target brand or company
(e.g., the fruit "mango", a person's name, a place name, or an unrelated term).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RANA PLAZA DEFINITION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Set rana_plaza="Yes" ONLY if the article body explicitly names "Rana Plaza" OR uses a
phrase that unambiguously identifies the 2013 Bangladesh garment factory collapse
(e.g., "the Dhaka building collapse of 2013", "the Savar factory disaster").
Vague references to "a Bangladesh disaster" or "a factory accident" without further
identification do NOT qualify. When in doubt, set "No".

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EVIDENCE PRIORITY ORDER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

If the article contains sentences that point to different mention types, resolve conflicts
using this strict hierarchy (highest priority first):
  1. CENTRAL (including Crisis Exception / LIST-Crisis carve-out)
  2. LIST/SHOPPING
  3. PASSING
  4. HOMONYM
  5. NONE

Always classify using the highest-priority type supported by the evidence.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
METADATA BOUNDARY — CRITICAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Factiva documents often contain a metadata footer that is NOT part of the article body.
Treat ALL text appearing after ANY of the following markers as metadata, regardless of
where in the document the marker appears (top, middle, or bottom):
  "Search Summary", "Document Type:", "Subject:", "Industry:", "Company:", "Geographic:",
  "Load-Date:", "Language:", "Copyright", "Pub-Type:"
Once you encounter any of these markers, stop treating subsequent text as article body.
Do NOT treat any text appearing after these markers as article evidence. A brand name
appearing only in a "Company:" or "Subject:" metadata field does not count as a mention
in the article body.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CLASSIFICATION RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BEFORE CLASSIFYING: Read the entire article body before assigning any mention type.
Do NOT stop at the first occurrence of the brand. A later sentence may establish a
higher-priority mention type (e.g., a Rana Plaza connection in paragraph 8 outranks a
shopping list in paragraph 2). Apply the Evidence Priority Order only after reviewing
all occurrences of the brand and its aliases throughout the full text.

1. VERBATIM EVIDENCE REQUIRED: You must quote at least ONE verbatim sentence from the
   actual article body (not metadata, not a paraphrase, not an invented sentence) that
   contains the brand or an alias in context. If you cannot do this, set
   brand_relevant=false and brand_mention_type="none".

2. IGNORE METADATA: Do not use any text from the Factiva metadata footer as evidence
   (see Metadata Boundary section above).

3. CONFLICT RESOLUTION: If evidence sentences support different mention types, apply
   the Evidence Priority Order above — do not average or compromise between them.

4. RELEVANCE GATE: Set brand_relevant=true ONLY when brand_mention_type="central".
   For all other types (passing, list, homonym, none), set brand_relevant=false.

5. RANA PLAZA EVIDENCE: If rana_plaza="Yes", you MUST populate rana_plaza_evidence with
   the exact verbatim sentence(s) from the article body that justify this. If "No",
   leave the array empty.

6. ALIAS CONFIRMATION: If your evidence sentence contains an alias rather than the
   primary brand name, use the "reasoning" field in the JSON output to explicitly
   confirm that the alias refers to the target brand and not a homonym. If the
   surrounding sentence context does not clearly resolve the ambiguity (e.g., the alias
   is also a common word, fruit, person's name, or place), classify as HOMONYM rather
   than guessing.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUT FORMAT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return JSON ONLY — no commentary, no markdown fences, no text before or after.
The "reasoning" field MUST appear first and MUST be a single plain string with no
internal double-quotes (use single quotes if quoting within reasoning). Keep it
under 60 words. Do not use it to reproduce article text — use it only to state
your classification decision and, where applicable, confirm an alias.

{{
  "reasoning": "One or two sentences: state mention type, why, and alias confirmation if needed. No internal double-quotes.",
  "brand_relevant": true/false,
  "brand_mention_type": "central|passing|list|homonym|none",
  "brand_evidence_sentences": ["verbatim sentence from article body..."],
  "rana_plaza": "Yes|No",
  "rana_plaza_evidence": ["verbatim sentence from article body..." or empty]
}}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ARTICLE METADATA (for reference only — do not use as evidence)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Headline: {hd}
Source: {sn}
Date: {pd_}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ARTICLE TEXT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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
    print(f"Classifying {len(df)} articles using {GEMINI_MODEL} IN PARALLEL...")
    
    # We need a lock so multiple threads don't write to the cache file at the exact same millisecond
    cache_lock = threading.Lock()
    
    def process_single_article(row):
        key = f"{row['pdf_file']}|{row['article_index']}"

        # 1. Prefilter
        if not prefilter_brand_mention(row, BRAND, BRAND_ALIASES):
            return {
                "pdf_file": row["pdf_file"],
                "article_index": row["article_index"],
                "brand_relevant": False,
                "Mention_of_Rana_Plaza": "No",
                "raw_llm": "SKIPPED_PREFILTER"
            }
        
        # 2. Check Cache
        with cache_lock:
            cached_val = cache.get(key)
            
        if cached_val and "rana_plaza" in str(cached_val):
            parsed = cached_val if "raw" not in cached_val else safe_parse_json(cached_val["raw"])
            parsed = enforce_rules(parsed, BRAND, BRAND_ALIASES)
        else:
            # 3. Call API
            prompt = make_prompt(BRAND, BRAND_ALIASES, row)
            raw_resp = call_gemini(prompt) 
            parsed = safe_parse_json(raw_resp)
            parsed = enforce_rules(parsed, BRAND, BRAND_ALIASES)
            parsed["raw"] = raw_resp
            
            with cache_lock:
                cache[key] = parsed
            
            time.sleep(SLEEP_BETWEEN) 

        return {
            "pdf_file": row["pdf_file"],
            "article_index": row["article_index"],
            "brand_relevant": parsed.get("brand_relevant", False),
            "Mention_of_Rana_Plaza": parsed.get("rana_plaza", "No"),
            "raw_llm": parsed.get("raw", "")
        }

    # RUN IN PARALLEL (15 articles at the same time)
    MAX_THREADS = 15 
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # Submit all tasks to the executor
        future_to_row = {executor.submit(process_single_article, row): row for _, row in df.iterrows()}
        
        processed_count = 0
        for future in concurrent.futures.as_completed(future_to_row):
            results.append(future.result())
            processed_count += 1
            
            print(f"Processed {processed_count}/{len(df)}...", end="\r")
            
            # Auto-save every 50 articles
            if processed_count % 50 == 0:
                with cache_lock:
                    with open(BRAND_CACHE_JSON,"w",encoding="utf-8") as f:
                        json.dump(cache, f, ensure_ascii=False, indent=2)

    print("")
    # Final Save
    with open(BRAND_CACHE_JSON,"w",encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    out_df = pd.DataFrame(results)
    # Sort the dataframe back into original order
    out_df = out_df.sort_values(by=["pdf_file", "article_index"]) 
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

    # Use a regex to extract ONLY the "Day Month Year" part and ignore "ET 20:41"
    clean_dates = final_df["PD"].astype(str).str.extract(r"(\d{1,2}\s+[A-Za-z]+\s+\d{4})", expand=False)
    
    # Parse the cleaned dates
    final_df["dt"] = pd.to_datetime(clean_dates, dayfirst=True, errors="coerce")
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