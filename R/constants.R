### Package constants

# Constants -------------------------------------------------------------------

CACHE_NM_API_KEY = "nm_api_key"
PIPELINE_DIR <- "pipeline"

CONFIG_FILE <- file.path(PIPELINE_DIR, "config.json")
CONFIG_NAME_SX_KEY = "api-key-sx"
CONFIG_NAME_NM_KEY = "api-key-nm"

INPUT_DIR <- file.path(PIPELINE_DIR, "input")
OUTPUT_DIR <- file.path(PIPELINE_DIR, "output")
ARCHIVE_DIR <- file.path(PIPELINE_DIR, "archive")

SX_INPUT_DIR <- file.path(INPUT_DIR, "statxplore")
SX_OUTPUT_DIR <- file.path(OUTPUT_DIR,  "statxplore")
SX_ARCHIVE_DIR <- file.path(ARCHIVE_DIR,  "statxplore")
SX_DATE_ID_TOKEN <- "<date_id>"
SX_START_DATE <- as.Date("2015-08-01", origin = lubridate::origin)

HMRC_CSV_INPUT_DIR <- file.path(INPUT_DIR, "hmrc", "csv")
HMRC_EXCEL_INPUT_DIR <- file.path(INPUT_DIR, "hmrc", "excel")
HMRC_OUTPUT_DIR <- file.path(OUTPUT_DIR, "hmrc")
HMRC_ARCHIVE_DIR <- file.path(ARCHIVE_DIR, "hmrc")
HMRC_TC_FILENAME <- "tax-credits"
HMRC_TC_URL <- stringr::str_c(
    "https://www.gov.uk/government/statistics/child-and-working-tax-credits-",
    "statistics-provisional-awards-geographical-analyses-december-2013")

JSA_INPUT_DIR <- file.path(INPUT_DIR, "jsa")
JSA_INPUT_FILE <- file.path(JSA_INPUT_DIR, "historic-jsa.csv")
JSA_OUTPUT_DIR <- file.path(OUTPUT_DIR, "jsa")
JSA_ARCHIVE_DIR <- file.path(ARCHIVE_DIR, "jsa")
JSA_CROSSOVER_DATE <- as.Date("2019-02-01", origin = lubridate::origin)
JSA_END_DATE <- as.Date("2023-12-01", origin = lubridate::origin)
JSA_PROPORTION_FEB19 <- 0.9071

MASTER_START_DATE <- as.Date("2015-08-01", origin = lubridate::origin)

LEGACY_BENEFIT_TYPES <- c(
    "hb_total" = "housing",
    "hmrc_wc_total" = "children",
    "esa" = "incapacity",
    "jsa" = "unemployment",
    "legacy_total" = "total")

UC_BENEFIT_TYPES <- c(
    "uch_housing" = "housing",
    "uch_child" = "children",
    "uch_capability" = "incapacity",
    "ucp_search_work" = "unemployment",
    "uch_total" = "total")
