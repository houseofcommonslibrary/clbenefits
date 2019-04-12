### Functions for running and managing the pipeline

# Constants -------------------------------------------------------------------

PIPELINE_DIR <- "pipeline"

CONFIG_FILE <- file.path(PIPELINE_DIR, "config.json")
CONFIG_NAME_SX_KEY = "api-key-sx"
CONFIG_NAME_NM_KEY = "api-key-nm"

INPUT_DIR <- file.path(PIPELINE_DIR, "input")
SX_INPUT_DIR <- file.path(INPUT_DIR, "statxplore")
NM_INPUT_DIR <- file.path(INPUT_DIR, "nomis")
HMRC_CSV_INPUT_DIR <- file.path(INPUT_DIR, "hmrc", "csv")
HMRC_EXCEL_INPUT_DIR <- file.path(INPUT_DIR, "hmrc", "excel")

OUTPUT_DIR <- file.path(PIPELINE_DIR, "output")
SX_OUTPUT_DIR <- file.path(OUTPUT_DIR,  "statxplore")
NM_OUTPUT_DIR <- file.path(OUTPUT_DIR, "nomis")
HMRC_OUTPUT_DIR <- file.path(OUTPUT_DIR, "hmrc")

# Config ----------------------------------------------------------------------

#' Load the config file
#'
#' \code{load_config} loads the config file from the pipeline directory. The
#' config file is a json file specifying the api keys for Stat-Xplore and
#' Nomis.
#'
#' @export

load_config <- function() {

    config_text <- readr::read_file(CONFIG_FILE)
    config <- jsonlite::fromJSON(config_text)

    if (! CONFIG_NAME_SX_KEY %in% names(config)) {
        stop("No Stat-Xplore API key provided in key configuration file")
    }

    if (! CONFIG_NAME_NM_KEY %in% names(config)) {
        stop("No Nomis API key provided in key configuration file")
    }

    set_sx_api_key(config[[CONFIG_NAME_SX_KEY]])
    set_nm_api_key(config[[CONFIG_NAME_NM_KEY]])
}

# Report ----------------------------------------------------------------------

report <- function(msg) cat(stringr::str_glue("{msg}\n\n"))

# Pipeline --------------------------------------------------------------------

#' Fetch all data from all sources
#'
#' \code{fetch_data} runs the full pipeline. It fetches all of the data from
#' each  source and stores the results as csvs in the output directory.
#'
#' @param verbose A boolean indicating whether the progress of the pipeline
#'   should be printed to the console.
#' @return A nested list of the results sets as dataframes.
#' @export

fetch_data <- function(verbose = TRUE) {

    load_config()

    sx <- fetch_sx(verbose)
    report("Writing Stat-Xplore data")
    purrr::map(names(sx), function(name) {
        filename <- file.path(SX_OUTPUT_DIR, stringr::str_glue("{name}.csv"))
        readr::write_csv(sx[[name]], filename)
    })

    nm <- fetch_nm(verbose)
    report("Writing Nomis data")
    purrr::map(names(nm), function(name) {
        filename <- file.path(NM_OUTPUT_DIR, stringr::str_glue("{name}.csv"))
        readr::write_csv(nm[[name]], filename)
    })

    hmrc <- read_hmrc(verbose)
    report("Writing HMRC data")
    filename <- file.path(HMRC_OUTPUT_DIR, "hmrc.csv")
    readr::write_csv(hmrc, filename)

    data <- list(
        esa = sx$esa,
        hb = sx$hb,
        ucp = sx$ucp,
        uch = sx$uch,
        uch_child = sx$uch_child,
        uch_housing = sx$uch_housing,
        uch_capability = sx$uch_capability,
        uch_total = sx$uch_total,
        jsa = nm$jsa,
        is = nm$is,
        hmrc = hmrc)

    report("Writing all data as RDS")
    saveRDS(data, file.path(OUTPUT_DIR, "data.rds"))

    data
}

#' Interpolate data from all sources (if necessary)
#'
#' \code{interpolcate_data} takes the data returned from \code{fetch_data} and
#' interpolates monthly data for those tables which require interpolation.
#'
#' @param data The list of tables returned from \code{fetch_data}.
#' @return An equivalent set of results tables which each contain monthly data.
#' @export

interpolate_data <- function(data) {

    data$esa <- interpolate_esa(data$esa)
    data$jsa <- interpolate_jsa(data$jsa)
    data$is <- interpolate_is(data$is)
    data$hmrc <- interpolate_hmrc(data$hmrc)

    data
}
