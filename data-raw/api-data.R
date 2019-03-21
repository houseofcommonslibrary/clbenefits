### Script for generating data files containing queries and links for APIs

read_file_trim <- function(filename) {
    stringr::str_trim(readr::read_file(filename))
}

SX_ESA_DATES_QUERY <- read_file_trim("sx-esa-dates.json")
SX_ESA_QUERY <- read_file_trim("sx-esa.json")

SX_HB_DATES_QUERY <- read_file_trim("sx-hb-dates.json")
SX_HB_QUERY <- read_file_trim("sx-hb.json")

SX_UCP_DATES_QUERY <- read_file_trim("sx-ucp-dates.json")
SX_UCP_QUERY <- read_file_trim("sx-ucp.json")

SX_UCH_DATES_QUERY <- read_file_trim("sx-uch-dates.json")
SX_UCH_QUERY <- read_file_trim("sx-uch.json")

NM_IS_DATE_URL <- read_file_trim("nm-is-date.txt")
NM_IS_URL <- read_file_trim("nm-is.txt")

NM_JSA_DATE_URL <- read_file_trim("nm-jsa-date.txt")
NM_JSA_URL <- read_file_trim("nm-jsa.txt")

NM_GEOGRAPHY_LOOKUP <- readr::read_csv(
    "nm-geography-lookup.csv",
    col_types = readr::cols(
        geography = readr::col_character(),
        gid = readr::col_character()))

usethis::use_data(
    SX_ESA_DATES_QUERY,
    SX_ESA_QUERY,
    SX_HB_DATES_QUERY,
    SX_HB_QUERY,
    SX_UCP_DATES_QUERY,
    SX_UCP_QUERY,
    SX_UCH_DATES_QUERY,
    SX_UCH_QUERY,
    NM_IS_DATE_URL,
    NM_IS_URL,
    NM_JSA_DATE_URL,
    NM_JSA_URL,
    NM_GEOGRAPHY_LOOKUP,
    internal = TRUE,
    overwrite = TRUE)
