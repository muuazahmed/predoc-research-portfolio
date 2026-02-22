# 04a_spatial_join.R
# Purpose: Spatially join ACLED events to grid cells and aggregate data.
# Input: 
#   - Shapefile: data/raw/grid_cell_fishnet/MWfishnet_Africa.shp
#   - ACLED Data: data/process/ACLED_Event_Level_Dataset.csv
# Output: 
#   - Aggregated Data: data/intermediate/grid_cross_section_ACLED.csv

# 1. Setup ----------------------------------------------------------------
# Load necessary libraries
if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse, sf, this.path)

# --- RELATIVE PATH CONFIGURATION ---
# 1. Get the directory where this script is located)
script_dir <- this.path::this.dir()

# 2. Define the Project Root relative to the script
project_root <- file.path(script_dir, "..", "..")

# 3. Define Data Paths relative to that root
raw_path          <- file.path(project_root, "data", "raw")
raw_cleaned_path  <- file.path(project_root, "data", "raw_cleaned")
intermediate_path <- file.path(project_root, "data", "intermediate")

message(paste("Script location:", script_dir))
message(paste("Project Root detected as:", normalizePath(project_root)))

# Ensure intermediate directory exists
if (!dir.exists(intermediate_path)) {
  dir.create(intermediate_path, recursive = TRUE)
}

# 2. Load Data ------------------------------------------------------------
message("Loading data...")

# Load Grid Shapefile
grid_path <- file.path(raw_path, "grid_cell_fishnet", "MWfishnet_Africa.shp")
if (!file.exists(grid_path)) stop("Shapefile not found at: ", grid_path)
grid <- st_read(grid_path, quiet = TRUE)

# Load ACLED CSV
acled_path <- file.path(raw_cleaned_path, "ACLED_cleaned", "Event_Level_Dataset.csv")

if (!file.exists(acled_path)) {
  stop("❌ ACLED file not found at: ", acled_path)
}
acled <- read_csv(acled_path, show_col_types = FALSE)

# 3. CRS Consistency Check ------------------------------------------------
message("Checking CRS...")

# Standardize to WGS84 (EPSG:4326)
target_crs <- 4326

if (st_crs(grid)$epsg != target_crs) {
  message("Transforming grid CRS to WGS84...")
  grid <- st_transform(grid, target_crs)
}

# Convert ACLED to sf object
# Remove rows with missing coordinates
acled <- acled %>% filter(!is.na(longitude) & !is.na(latitude))
acled_sf <- st_as_sf(acled, coords = c("longitude", "latitude"), crs = target_crs, remove = FALSE)

# 4. Spatial Join ---------------------------------------------------------
message("Performing spatial join...")

# Ensure grid has a unique identifier
# Priority: cell_id -> gid -> ID -> create new one
grid_cols <- names(grid)
id_col <- "grid_id" # Default fallback

if ("cell_id" %in% grid_cols) {
  id_col <- "cell_id"
} else if ("gid" %in% grid_cols) {
  id_col <- "gid"
} else if ("ID" %in% grid_cols) {
  id_col <- "ID"
} else {
  message("No 'cell_id', 'gid', or 'ID' found. Creating row-based 'grid_id'...")
  grid <- grid %>% mutate(grid_id = row_number())
}

message(paste("Using grid identifier:", id_col))

# Spatial join: Join ACLED points to Grid polygons
# st_join(x, y) -> keeps geometry of x (points), adds attributes of y (grid)
joined_sf <- st_join(acled_sf, grid[id_col], join = st_within)

# 5. Aggregation ----------------------------------------------------------
message("Aggregating data...")

# Aggregate by Grid ID
aggregated <- joined_sf %>%
  st_drop_geometry() %>%
  group_by(across(all_of(id_col))) %>%
  summarise(
    acled_count = n(),
    acled_fatalities = sum(fatalities, na.rm = TRUE),
    .groups = "drop"
  )

# Merge back with the full list of grid IDs to keep "peaceful" cells
# We also create the "Dummy" variable here (1 if conflict, 0 if no conflict)
all_grids <- grid %>% st_drop_geometry() %>% select(all_of(id_col)) %>% distinct()

final_data <- all_grids %>%
  left_join(aggregated, by = id_col) %>%
  mutate(
    acled_count = replace_na(acled_count, 0),
    acled_fatalities = replace_na(acled_fatalities, 0),
    # Create the Binary Dummy Variable
    acled_dummy = ifelse(acled_count > 0, 1, 0)
  )

# 6. Export ---------------------------------------------------------------
output_file <- file.path(intermediate_path, "grid_cross_section_ACLED.csv")
message("Exporting to: ", output_file)

write_csv(final_data, output_file)

message("Done!")