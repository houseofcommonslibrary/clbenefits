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

ARCHIVE_DIR <- file.path(PIPELINE_DIR, "archive")
SX_ARCHIVE_DIR <- file.path(ARCHIVE_DIR,  "statxplore")
NM_ARCHIVE_DIR <- file.path(ARCHIVE_DIR, "nomis")
HMRC_ARCHIVE_DIR <- file.path(ARCHIVE_DIR, "hmrc")

MASTER_START_DATE <- as.Date("2015-08-01", origin = lubridate::origin)

# Config ----------------------------------------------------------------------

#' Load the config file
#'
#' \code{load_config} loads the config file from the pipeline directory. The
#' config file is a json file specifying the api keys for Stat-Xplore and
#' Nomis.
#'
#' @export

load_config <- function() {

    config_str <- readr::read_file(CONFIG_FILE)
    config <- jsonlite::fromJSON(config_str)

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
    nm <- fetch_nm(verbose)
    hmrc <- read_hmrc(verbose)

    report("Writing Stat-Xplore data")
    purrr::map(names(sx), function(name) {
        filename <- file.path(SX_OUTPUT_DIR, stringr::str_glue("{name}.csv"))
        readr::write_csv(sx[[name]], filename)
    })

    report("Writing Nomis data")
    purrr::map(names(nm), function(name) {
        filename <- file.path(NM_OUTPUT_DIR, stringr::str_glue("{name}.csv"))
        readr::write_csv(nm[[name]], filename)
    })

    report("Writing HMRC data")
    filename <- file.path(HMRC_OUTPUT_DIR, "hmrc.csv")
    readr::write_csv(hmrc, filename)

    list(
        esa = sx$esa,
        hb = sx$hb,
        ucp = sx$ucp,
        uch = sx$uch,
        jsa = nm$jsa,
        is = nm$is,
        hmrc = hmrc)
}

#' Interpolate data from all sources (if necessary)
#'
#' \code{interpolcate_data} takes the data returned from \code{fetch_data} and
#' interpolates monthly data for those tables which require interpolation.
#'
#' @param fdata The list of dataframes returned from \code{fetch_data}.
#' @return An equivalent set of dataframes which each contain monthly data.
#' @export

interpolate_data <- function(fdata) {


    uch <- fdata$uch %>%
        dplyr::filter(.data$date >= MASTER_START_DATE) %>%
        dplyr::mutate(uch_int = 0)

    ucp <- fdata$ucp %>%
        dplyr::filter(.data$date >= MASTER_START_DATE) %>%
        dplyr::mutate(ucp_int = 0)

    hb <- fdata$hb %>%
        dplyr::filter(.data$date >= MASTER_START_DATE)%>%
        dplyr::mutate(hb_int = 0)

    esa <- interpolate_esa(fdata$esa) %>%
        dplyr::filter(.data$date >= MASTER_START_DATE)

    jsa <- interpolate_jsa(fdata$jsa) %>%
        dplyr::filter(.data$date >= MASTER_START_DATE)

    is <- interpolate_is(fdata$is) %>%
        dplyr::filter(.data$date >= MASTER_START_DATE)

    hmrc <- interpolate_hmrc(fdata$hmrc) %>%
        dplyr::filter(.data$date >= MASTER_START_DATE)

    list(
        esa = esa,
        hb = hb,
        ucp = ucp,
        uch = uch,
        jsa = jsa,
        is = is,
        hmrc = hmrc)
}

#' Create the master table
#'
#' \code{create_master} takes the full dataset returned from
#' \code{interpolate_data} and combines it into a single master dataframe.
#'
#' @param idata The list of dataframes returned from \code{interpolate_data}.
#' @return A dataframe of key benefits data with one row per unqiue combination
#'   of geogpraphic area and month.
#' @export

create_master <- function(idata) {

    # Extract key data from each dataset for joining
    uch_child <- idata$uch %>%
        dplyr::filter(
            .data$uch_child == "Yes",
            .data$uch_housing == "Total",
            .data$uch_capability == "Total") %>%
        dplyr::select(
            .data$gid,
            .data$date,
            uch_child = .data$uch)

    uch_housing <- idata$uch %>%
        dplyr::filter(
            .data$uch_child == "Total",
            .data$uch_housing == "Yes",
            .data$uch_capability == "Total") %>%
        dplyr::select(
            .data$gid,
            .data$date,
            uch_housing = .data$uch)

    uch_capability <- idata$uch %>%
        dplyr::filter(
            .data$uch_child == "Total",
            .data$uch_housing == "Total",
            .data$uch_capability == "Yes") %>%
        dplyr::select(
            .data$gid,
            .data$date,
            uch_capability = .data$uch)

    uch_total <- idata$uch %>%
        dplyr::filter(
            .data$uch_child == "Total",
            .data$uch_housing == "Total",
            .data$uch_capability == "Total") %>%
        dplyr::select(
            .data$gid,
            .data$date,
            uch_total = .data$uch,
            .data$uch_int)

    ucp_search_work <- idata$ucp %>%
        dplyr::filter(.data$ucp_gender == "Total") %>%
        dplyr::filter(.data$ucp_conditionality == "Searching for work") %>%
        dplyr::select(
            .data$gid,
            .data$date,
            ucp_search_work = .data$ucp)

    ucp_total <- idata$ucp %>%
        dplyr::filter(.data$ucp_gender == "Total") %>%
        dplyr::filter(.data$ucp_conditionality == "Total") %>%
        dplyr::select(
            .data$gid,
            .data$date,
            ucp_total = .data$ucp,
            .data$ucp_int)

    hb_not_emp <- idata$hb %>%
        dplyr::filter(.data$hb_status ==
            "Not in employment (and not on Passported Benefit)") %>%
        dplyr::select(
            .data$gid,
            .data$date,
            hb_not_emp = .data$hb)

    hb_total <- idata$hb %>%
        dplyr::filter(.data$hb_status == "Total") %>%
        dplyr::select(
            .data$gid,
            .data$date,
            hb_total = .data$hb,
            .data$hb_int)

    esa <- idata$esa %>%
        dplyr::filter(.data$esa_payment_type == "Total")%>%
        dplyr::select(
            .data$gid,
            .data$date,
            .data$esa,
            .data$esa_int)

    is <- idata$is %>%
        dplyr::select(
            .data$gid,
            .data$date,
            .data$is,
            .data$is_int)

    jsa <- idata$jsa %>%
        dplyr::group_by(
            .data$gid,
            .data$date) %>%
        dplyr::summarise(
            jsa = sum(.data$jsa),
            jsa_int = mean(.data$jsa_int)) %>%
        dplyr::ungroup() %>%
        dplyr::select(
            .data$gid,
            .data$date,
            .data$jsa,
            .data$jsa_int)

    hmrc <- idata$hmrc %>% dplyr::select(-geography)

    mdata <- list(
        uch_child = uch_child,
        uch_housing = uch_housing,
        uch_capability = uch_capability,
        uch_total = uch_total,
        ucp_search_work = ucp_search_work,
        ucp_total = ucp_total,
        hb_not_emp = hb_not_emp,
        hb_total = hb_total,
        esa = esa,
        is = is,
        jsa = jsa,
        hmrc = hmrc)

    # Get unique gids and dates
    gid <- unique(unlist(purrr::map(mdata, ~ .$gid), use.names = FALSE))
    date <- unique(as.Date(unlist(purrr::map(mdata, ~ .$date),
                use.names = FALSE), origin = lubridate::origin))

    # Create a tibble with unique combinations of geography and date
    master <- tidyr::crossing(gid, date) %>%
        dplyr::left_join(., GID_GEOGRAPHY_LOOKUP, by = c("gid")) %>%
        dplyr::select(.data$gid, .data$geography, .data$date)

    # Join the columns from each dataset table in turn
    master %>%
        dplyr::left_join(., uch_child, by = c("gid", "date")) %>%
        dplyr::left_join(., uch_housing, by = c("gid", "date")) %>%
        dplyr::left_join(., uch_capability, by = c("gid", "date")) %>%
        dplyr::left_join(., uch_total, by = c("gid", "date")) %>%
        dplyr::left_join(., ucp_search_work, by = c("gid", "date")) %>%
        dplyr::left_join(., ucp_total, by = c("gid", "date")) %>%
        dplyr::left_join(., hb_not_emp, by = c("gid", "date")) %>%
        dplyr::left_join(., hb_total, by = c("gid", "date")) %>%
        dplyr::left_join(., esa, by = c("gid", "date")) %>%
        dplyr::left_join(., is, by = c("gid", "date")) %>%
        dplyr::left_join(., jsa, by = c("gid", "date")) %>%
        dplyr::left_join(., hmrc, by = c("gid", "date")) %>%
        dplyr::arrange(.data$gid, .data$date) %>%
        tidyr::replace_na(list(
            uch_int = 2,
            ucp_int = 2,
            hb_int = 2,
            esa_int = 2,
            is_int = 2,
            jsa_int = 2,
            hmrc_int =2)) %>%
        tidyr::fill(-dplyr::ends_with("_int"))
}

#' Run the pipeline
#'
#' \code{pipeline} runs the full pipeline.
#'
#' @return A list containing the data, the interpolated data, and the master
#'   dataframe of results.
#' @export

pipeline <- function() {

    fdata <- fetch_data()
    idata <- interpolate_data(fdata)
    master <- create_master(idata)

    data <- list(
        fdata = fdata,
        idata = idata,
        master = master)

    report("Writing master benefits data file")
    filename <- file.path(OUTPUT_DIR, "master-benefits-data.csv")
    readr::write_csv(data$master, filename)

    report("Writing all data as RDS")
    saveRDS(data, file.path(OUTPUT_DIR, "data.rds"))
}
