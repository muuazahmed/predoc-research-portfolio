# Pre-Doctoral Research Portfolio: Empirical Data Engineering & Text Parsing

**Muaz Ahmed** | MRes Candidate, Paris School of Economics  
muaz.ahmed@ens.psl.eu | https://www.linkedin.com/in/muaz-ahmed1/

This repository contains a curated selection of data engineering, spatial analysis, and natural language processing (NLP) scripts developed during my Research Assistant roles at the University of Essex and the Paris School of Economics. 

These scripts demonstrate my ability to independently construct automated data pipelines, process messy unstructured text, and handle large-scale datasets using Python and R.

---

## 📂 Repository Structure

### 1. Large-Scale Data Pipelines (`/Large_Scale_Data_Pipelines`)
This directory contains Python scripts used to query, clean, and harmonize multi-million-row conflict datasets (GDELT and ACLED) into structured panel data.

* **`02_fetch_gdelt_data_bigquery.py`**: An automated pipeline that connects to Google Cloud Platform (BigQuery) to extract millions of records. It features a custom inverted-index and nested regular expression (Regex) algorithm to execute context-aware filtering, separating true transnational actors from false-positive geographic mentions.
* **`03a_create_event_level_dataset_from_ACLED.py`**: A robust cleaning script that standardizes dates, generates unique identifiers, and flags specific violence indicators to create a balanced, event-level panel dataset suitable for econometric estimation.

### 2. LLM Text Parsing & NLP (`/LLM_Text_Parsing`)
This directory showcases my workflow for converting thousands of unstructured media articles (Factiva PDFs) into a quantitative database using Python, Regex, and Large Language Models.

* **`database_converter_google_api_key.py`**: A comprehensive parsing script utilizing `pdfplumber` to linearly extract text and Factiva tags (e.g., HD, TD). It interfaces with the Google Gemini 2.5 API (handling rate limits and JSON responses) to classify articles based on complex, context-dependent relevance criteria regarding corporate reputational shocks.
* **`Final_database_converter.ipynb`**: A Jupyter Notebook demonstrating the integration of local open-source LLMs (via Ollama) to perform similar text-as-data classification tasks, ensuring data privacy and cost-efficiency during early pipeline testing.

### 3. Geospatial Analysis in R (`/Geospatial_Analysis_R`)
This directory highlights my ability to rapidly acquire new methodological tools and work across multiple programming languages.

* **`04a_spatial_join.R`**: An R script utilizing the `sf` package to perform complex geospatial operations. It normalizes Coordinate Reference Systems (CRS) and executes point-in-polygon joins to map over 200,000 geo-coordinated events to a standardized 10,678-cell geographic fishnet, aggregating the data for spatial panel regressions.

---

*Note: Proprietary data and sensitive API keys have been removed or sanitized from these scripts. They are provided purely as a demonstration of coding methodology and pipeline architecture.*