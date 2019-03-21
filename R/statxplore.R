### Functions for downloading and processing data from Stat-Xplore

# Constants -------------------------------------------------------------------

SX_DATE_ID_TOKEN <- "<date_id>"

# Generic function to fetch Stat-Xplore tables --------------------------------

fetch_sx_table <- function(query, filename = NULL, custom = NULL) {
    if(! statxplorer::has_api_key()) {
        stop(stringr::str_c(
            "No Stat-Xplore API key provided: use ",
            "clbenefits::set_sx_api_key() or clbenefits::load_sx_api_key() ",
            "to provide your key"))
    }
    statxplorer::fetch_table(query, filename = filename, custom = custom)
}

# Functions for parsing dates -------------------------------------------------

parse_sx_date_id <- function(date_id) {
    year <- lubridate::year(date_id)
    month <- stringr::str_pad(lubridate::month(date_id), 2, "left", "0")
    as.character(stringr::str_glue("{year}{month}"))
}

parse_sx_date_esa <- function(quarters) {
    quarters <- stringr::str_glue("1-{quarters}")
    lubridate::dmy(quarters)
}

parse_sx_date_hb <- function(months) {
    months <- sapply(stringr::str_split(months, " "), "[[", 1)
    months <- stringr::str_glue("{months}01")
    lubridate::ymd(months)
}

parse_sx_date_uc <- function(months) {
    months <- stringr::str_glue("1 {months}")
    lubridate::dmy(months)
}

# Fetch dates for each dataset ------------------------------------------------

fetch_sx_dates <- function(query, date_func) {
    results <- fetch_sx_table(query)
    dates <- results$dfs[[1]]
    colnames(dates) <- c("date", "value")
    dates$date <- date_func(dates$date)
    dates$date_id <- parse_sx_date_id(dates$date)
    dates %>% dplyr::select(
        .data$date_id,
        .data$date,
        .data$value)
}

fetch_sx_dates_esa <- function() {
    fetch_sx_dates(SX_ESA_DATES_QUERY, parse_sx_date_esa)
}

fetch_sx_dates_hb <- function() {
    fetch_sx_dates(SX_HB_DATES_QUERY, parse_sx_date_hb)
}

fetch_sx_dates_ucp <- function() {
    fetch_sx_dates(SX_UCP_DATES_QUERY, parse_sx_date_uc)
}

fetch_sx_dates_uch <- function() {
    fetch_sx_dates(SX_UCH_DATES_QUERY, parse_sx_date_uc)
}


# Fetch data for a given date for each dataset --------------------------------

fetch_sx_esa_in <- function(date_id) {

    query <- stringr::str_replace(SX_ESA_QUERY, SX_DATE_ID_TOKEN, date_id)

    results <- query %>%
        fetch_sx_table() %>%
        statxplorer::add_codes_for_field(
            field = "Westminster Parliamentary Constituencies",
            colname = "pconid")

    esa <- results$dfs[[1]] %>%
        dplyr::select(.data$pconid, dplyr::everything())

    colnames(esa) <- c(
        "gid",
        "geography",
        "date",
        "payment_type",
        "caseload")

    esa$date <- parse_sx_date_esa(esa$date)
    esa
}

fetch_sx_hb_in <- function(date_id) {

    query <- stringr::str_replace(SX_HB_QUERY, SX_DATE_ID_TOKEN, date_id)
    custom <- list("Age of Claimant (bands only)" = c("16-64"))

    results <- query %>%
        fetch_sx_table(custom = custom) %>%
        statxplorer::add_codes_for_field(
            field = "Westminster Parliamentary Constituencies",
            colname = "pconid")

    hb <- results$df[[1]] %>%
        dplyr::select(.data$pconid, dplyr::everything())

    colnames(hb) <- c(
        "gid",
        "geography",
        "date",
        "status",
        "age",
        "claimants")

    hb$date <- parse_sx_date_hb(hb$date)
    hb
}

fetch_sx_ucp_in <- function(date_id) {

    query <- stringr::str_replace(SX_UCP_QUERY, SX_DATE_ID_TOKEN, date_id)

    results <- query %>%
        fetch_sx_table()  %>%
        statxplorer::add_codes_for_field(
            field = "Westminster Parliamentary Constituencies",
            colname = "pconid")

    ucp <- results$df[[1]] %>%
        dplyr::select(.data$pconid, dplyr::everything())

    colnames(ucp) <- c(
        "gid",
        "geography",
        "date",
        "gender",
        "conditionality",
        "people")

    ucp$date <- parse_sx_date_uc(ucp$date)
    ucp
}

fetch_sx_uch_in <- function(date_id) {

    query <- stringr::str_replace(SX_UCH_QUERY, SX_DATE_ID_TOKEN, date_id)

    results <- query %>%
        fetch_sx_table()  %>%
        statxplorer::add_codes_for_field(
            field = "Westminster Parliamentary Constituencies",
            colname = "pconid")

    uch <- results$df[[1]] %>%
        dplyr::select(.data$pconid, dplyr::everything())

    colnames(uch) <- c(
        "gid",
        "geography",
        "date",
        "ent_child",
        "ent_housing",
        "ent_capability",
        "households")

    uch$date <- parse_sx_date_uc(uch$date)
    uch
}

# Fetch data for all dates for each dataset -----------------------------------

fetch_sx_dataset <- function(date_func, dataset_func, verbose = TRUE) {

    dates <- date_func()
    fname <- deparse(substitute(dataset_func))

    purrr::map_dfr(dates$date_id, function(date_id) {
        if (verbose) {
            report(stringr::str_glue("Fetching Stat-Xplore dataset: {fname} for {date_id}"))
        }
        dataset_func(date_id)
    })
}

fetch_sx_esa <- function() {
    fetch_sx_dataset(fetch_sx_dates_esa, fetch_sx_esa_in)
}

fetch_sx_hb <- function() {
    fetch_sx_dataset(fetch_sx_dates_hb, fetch_sx_hb_in)
}

fetch_sx_ucp <- function() {
    fetch_sx_dataset(fetch_sx_dates_ucp, fetch_sx_ucp_in)
}

fetch_sx_uch <- function() {
    fetch_sx_dataset(fetch_sx_dates_uch, fetch_sx_uch_in)
}

# Fetch all data for all datasets ---------------------------------------------

#' Fetch all data from Stat-Xplore and return as a list of dataframes
#'
#' \code{fetch_sx} fetches all data from from Stat-Xplore and returns the
#' datasets as a list of dataframes.
#'
#' @param verbose A boolean indicating whether the progress of the function
#'   should be printed to the console.
#' @return A list of the results sets as dataframes.
#' @export

fetch_sx <- function(verbose = TRUE) {

    if (verbose) report("Fetching Stat-Xplore data on UC Households")
    uch <- fetch_sx_uch()

    uch_child <- uch %>%
        dplyr::filter(
            .data$ent_child == "Yes",
            .data$ent_housing == "Total",
            .data$ent_capability =="Total")

    uch_housing <- uch %>%
        dplyr::filter(
            .data$ent_child == "Total",
            .data$ent_housing == "Yes",
            .data$ent_capability =="Total")

    uch_capability <- uch %>%
        dplyr::filter(
            .data$ent_child == "Total",
            .data$ent_housing == "Total",
            .data$ent_capability =="Yes")

    uch_total <- uch %>%
        dplyr::filter(
            .data$ent_child == "Total",
            .data$ent_housing == "Total",
            .data$ent_capability =="Total")

    if (verbose) report("Fetching Stat-Xplore data on UC People")
    ucp <- fetch_sx_ucp()

    if (verbose) report("Fetching Stat-Xplore data on ESA")
    esa <- fetch_sx_esa()

    if (verbose) report("Fetching Stat-Xplore data on Housing Benefit")
    hb <- fetch_sx_hb()

    list(
        esa = esa,
        hb = hb,
        ucp = ucp,
        uch = uch,
        uch_child = uch_child,
        uch_housing = uch_housing,
        uch_capability = uch_capability,
        uch_total = uch_total)
}



