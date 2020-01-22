### Functions for downloading and processing data from Stat-Xplore

# Generic function to fetch Stat-Xplore tables --------------------------------

fetch_sx_table <- function(query, filename = NULL, custom = NULL, retry = 10) {

    # Check an api key exists
    if (! statxplorer::has_api_key()) {
        stop(stringr::str_c(
            "No Stat-Xplore API key provided: use ",
            "clbenefits::set_sx_api_key() or clbenefits::load_sx_api_key() ",
            "to provide your key"))
    }

    # Define a function to handle an error and retry
    handle_error <- function(condition) {
        if (retry > 0) {
            fetch_sx_table(query, filename, custom, retry - 1)
        } else {
            stop(condition$message)
        }
    }

    # Try to fetch the table and call the error handler on failure
    tryCatch({
        statxplorer::fetch_table(query, filename = filename, custom = custom)
    },
    error = handle_error)
}

# Functions for parsing dates -------------------------------------------------

parse_sx_date_id <- function(date_id) {
    year <- lubridate::year(date_id)
    month <- stringr::str_pad(lubridate::month(date_id), 2, "left", "0")
    as.character(stringr::str_glue("{year}{month}"))
}

parse_sx_date_uc <- function(months) {
    months <- stringr::str_glue("1 {months}")
    lubridate::dmy(months)
}

parse_sx_date_hb <- function(months) {
    months <- sapply(stringr::str_split(months, " "), "[[", 1)
    months <- stringr::str_glue("{months}01")
    lubridate::ymd(months)
}

parse_sx_date_esa <- function(quarters) {
    quarters <- stringr::str_glue("1-{quarters}")
    lubridate::dmy(quarters)
}

parse_sx_date_is <- function(quarters) {
    quarters <- stringr::str_glue("1-{quarters}")
    lubridate::dmy(quarters)
}

parse_sx_date_jsa <- function(quarters) {
    quarters <- stringr::str_glue("1-{quarters}")
    lubridate::dmy(quarters)
}

# Fetch dates for each dataset ------------------------------------------------

fetch_sx_dates <- function(query, date_func) {
    results <- fetch_sx_table(query)
    dates <- results$dfs[[1]]
    colnames(dates) <- c("date", "value")
    dates$date <- date_func(dates$date)
    dates$date_id <- parse_sx_date_id(dates$date)
    dates %>%
        dplyr::filter(date >= SX_START_DATE) %>%
        dplyr::select(
            .data$date_id,
            .data$date,
            .data$value)
}

fetch_sx_dates_uch <- function() {
    fetch_sx_dates(SX_UCH_DATES_QUERY, parse_sx_date_uc)
}

fetch_sx_dates_ucp <- function() {
    fetch_sx_dates(SX_UCP_DATES_QUERY, parse_sx_date_uc)
}

fetch_sx_dates_hb_1 <- function() {
    fetch_sx_dates(SX_HB_1_DATES_QUERY, parse_sx_date_hb)
}

fetch_sx_dates_hb_2 <- function() {
    fetch_sx_dates(SX_HB_2_DATES_QUERY, parse_sx_date_hb)
}

fetch_sx_dates_esa_1 <- function() {
    fetch_sx_dates(SX_ESA_1_DATES_QUERY, parse_sx_date_esa)
}

fetch_sx_dates_esa_2 <- function() {
    fetch_sx_dates(SX_ESA_2_DATES_QUERY, parse_sx_date_esa)
}

fetch_sx_dates_is <- function() {
    fetch_sx_dates(SX_IS_DATES_QUERY, parse_sx_date_is)
}

fetch_sx_dates_jsa <- function() {
    fetch_sx_dates(SX_JSA_DATES_QUERY, parse_sx_date_jsa)
}

# Fetch data for a given date for each dataset --------------------------------

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
        "uch_child",
        "uch_housing",
        "uch_capability",
        "uch")

    uch$date <- parse_sx_date_uc(uch$date)
    uch
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
        "ucp_gender",
        "ucp_conditionality",
        "ucp")

    ucp$date <- parse_sx_date_uc(ucp$date)
    ucp
}

fetch_sx_hb_1_x_in <- function(query, date_id, geography_field) {

    query <- stringr::str_replace(query, SX_DATE_ID_TOKEN, date_id)
    custom <- list("Age of Claimant (bands only)" = c("16-64"))

    results <- query %>%
        fetch_sx_table(custom = custom) %>%
        statxplorer::add_codes_for_field(
            field = geography_field,
            colname = "pconid")

    hb_1 <- results$dfs[[1]] %>%
        dplyr::select(
            .data$pconid,
            .data[[geography_field]],
            .data$Month,
            .data$`Employment Status`,
            dplyr::everything())

    colnames(hb_1) <- c(
        "gid",
        "geography",
        "date",
        "hb_status",
        "hb_age",
        "hb")

    hb_1$date <- parse_sx_date_hb(hb_1$date)
    hb_1
}

fetch_sx_hb_1_in <- function(date_id) {

    hb_1_1 <- fetch_sx_hb_1_x_in(
        SX_HB_1_1_QUERY,
        date_id,
        "Westminster Parliamentary Constituencies")

    hb_1_2 <- fetch_sx_hb_1_x_in(
        SX_HB_1_2_QUERY,
        date_id,
        "National - Regional - LA - OAs")

    dplyr::bind_rows(hb_1_1, hb_1_2)
}

fetch_sx_hb_2_in <- function(date_id) {

    query <- stringr::str_replace(SX_HB_2_QUERY, SX_DATE_ID_TOKEN, date_id)
    custom <- list("Age of Claimant (bands only)" = c("16-64"))

    results <- query %>%
        fetch_sx_table(custom = custom) %>%
        statxplorer::add_codes_for_field(
            field = "Westminster Parliamentary Constituencies",
            colname = "pconid")

    hb_2 <- results$df[[1]] %>%
        dplyr::select(.data$pconid, dplyr::everything())

    colnames(hb_2) <- c(
        "gid",
        "geography",
        "date",
        "hb_status",
        "hb_age",
        "hb")

    hb_2$date <- parse_sx_date_hb(hb_2$date)
    hb_2
}

fetch_sx_esa_1_x_in <- function(query, date_id, geography_field) {

    query <- stringr::str_replace(query, SX_DATE_ID_TOKEN, date_id)

    results <- query %>%
        fetch_sx_table() %>%
        statxplorer::add_codes_for_field(
            field = geography_field,
            colname = "pconid")

    esa_1 <- results$dfs[[1]] %>%
        dplyr::select(
            .data$pconid,
            .data[[geography_field]],
            .data$Quarter,
            dplyr::everything())

    colnames(esa_1) <- c(
        "gid",
        "geography",
        "date",
        "esa_payment_type",
        "esa")

    esa_1$date <- parse_sx_date_esa(esa_1$date)
    esa_1
}

fetch_sx_esa_1_in <- function(date_id) {

    esa_1_1 <- fetch_sx_esa_1_x_in(
        SX_ESA_1_1_QUERY,
        date_id,
        "Westminster Parliamentary Constituencies")

    esa_1_2 <- fetch_sx_esa_1_x_in(
        SX_ESA_1_2_QUERY,
        date_id,
        "National - Regional - LA - OAs")

    dplyr::bind_rows(esa_1_1, esa_1_2)
}

fetch_sx_esa_2_in <- function(date_id) {

    query <- stringr::str_replace(SX_ESA_2_QUERY, SX_DATE_ID_TOKEN, date_id)

    results <- query %>%
        fetch_sx_table() %>%
        statxplorer::add_codes_for_field(
            field = "Westminster Parliamentary Constituencies",
            colname = "pconid")

    esa_2 <- results$dfs[[1]] %>%
        dplyr::select(.data$pconid, dplyr::everything())

    colnames(esa_2) <- c(
        "gid",
        "geography",
        "date",
        "esa_payment_type",
        "esa")

    esa_2$date <- parse_sx_date_esa(esa_2$date)
    esa_2
}

fetch_sx_is_in <- function(date_id) {

    query <- stringr::str_replace(SX_IS_QUERY, SX_DATE_ID_TOKEN, date_id)

    results <- query %>%
        fetch_sx_table()  %>%
        statxplorer::add_codes_for_field(
            field = "Westminster Parliamentary Constituencies",
            colname = "pconid")

    is <- results$df[[1]] %>%
        dplyr::select(.data$pconid, dplyr::everything())

    colnames(is) <- c(
        "gid",
        "geography",
        "date",
        "is")

    is$date <- parse_sx_date_uc(is$date)
    is
}

fetch_sx_jsa_in <- function(date_id) {

    query <- stringr::str_replace(SX_JSA_QUERY, SX_DATE_ID_TOKEN, date_id)
    custom <- list("Grouped Amount of Benefit" = c("Receiving payment"))

    results <- query %>%
        fetch_sx_table(custom = custom)  %>%
        statxplorer::add_codes_for_field(
            field = "Westminster Parliamentary Constituencies",
            colname = "pconid")

    jsa <- results$df[[1]] %>%
        dplyr::select(.data$pconid, dplyr::everything())

    colnames(jsa) <- c(
        "gid",
        "geography",
        "amount",
        "date",
        "jsa")

    jsa$date <- parse_sx_date_jsa(jsa$date)
    jsa
}

# Fetch data for all dates for each dataset -----------------------------------

fetch_sx_dataset <- function(date_func, dataset_func, verbose = TRUE) {

    dates <- date_func()
    fname <- deparse(substitute(dataset_func))

    purrr::map_dfr(dates$date_id, function(date_id) {
        if (verbose) {
            report(stringr::str_glue(
                "Fetching Stat-Xplore dataset: {fname} for {date_id}"))
        }
        dataset_func(date_id)
    })
}

fetch_sx_uch <- function(verbose = TRUE) {
    fetch_sx_dataset(fetch_sx_dates_uch, fetch_sx_uch_in, verbose)
}

fetch_sx_ucp <- function(verbose = TRUE) {
    fetch_sx_dataset(fetch_sx_dates_ucp, fetch_sx_ucp_in, verbose)
}

fetch_sx_hb <- function(verbose = TRUE) {
    hb_1 <- fetch_sx_dataset(fetch_sx_dates_hb_1, fetch_sx_hb_1_in, verbose)
    hb_2 <- fetch_sx_dataset(fetch_sx_dates_hb_2, fetch_sx_hb_2_in, verbose)
    dplyr::bind_rows(hb_1, hb_2)
}

fetch_sx_esa <- function(verbose = TRUE) {
    esa_1 <- fetch_sx_dataset(fetch_sx_dates_esa_1, fetch_sx_esa_1_in, verbose)
    esa_2 <- fetch_sx_dataset(fetch_sx_dates_esa_2, fetch_sx_esa_2_in, verbose)
    dplyr::bind_rows(esa_1, esa_2)
}

fetch_sx_is <- function(verbose = TRUE) {
    fetch_sx_dataset(fetch_sx_dates_is, fetch_sx_is_in, verbose)
}

fetch_sx_jsa <- function(verbose = TRUE) {
    fetch_sx_dataset(fetch_sx_dates_jsa, fetch_sx_jsa_in, verbose)
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

    uch <- fetch_sx_uch(verbose) %>%
        dplyr::filter(.data$gid != "ZZXXXXXXX") %>%
        dplyr::arrange(
            .data$gid,
            .data$uch_child,
            .data$uch_housing,
            .data$uch_capability,
            .data$date)

    # uch <- readr::read_csv(file.path(SX_ARCHIVE_DIR, "uch.csv"))

    if (verbose) report("Fetching Stat-Xplore data on UC People")
    ucp <- fetch_sx_ucp(verbose) %>%
        dplyr::filter(.data$gid != "ZZXXXXXXX") %>%
        dplyr::arrange(
            .data$gid,
            .data$ucp_gender,
            .data$ucp_conditionality,
            .data$date)

    if (verbose) report("Fetching Stat-Xplore data on Housing Benefit")
    hb <- fetch_sx_hb(verbose) %>%
        dplyr::filter(.data$gid != "ZZXXXXXXX") %>%
        dplyr::arrange(
            .data$gid,
            .data$hb_status,
            .data$date)

    if (verbose) report("Fetching Stat-Xplore data on ESA")
    esa <- fetch_sx_esa(verbose) %>%
        dplyr::filter(.data$gid != "ZZXXXXXXX") %>%
        dplyr::arrange(
            .data$gid,
            .data$esa_payment_type,
            .data$date)

    if (verbose) report("Fetching Stat-Xplore data on Incapacity Benefit")
    is <- fetch_sx_is(verbose) %>%
        dplyr::filter(.data$gid != "ZZXXXXXXX") %>%
        dplyr::arrange(
            .data$gid,
            .data$date)

    if (verbose) report("Fetching Stat-Xplore data on JSA")
    jsa <- fetch_sx_jsa(verbose) %>%
        dplyr::filter(.data$gid != "ZZXXXXXXX") %>%
        dplyr::arrange(
            .data$gid,
            .data$date)

    list(
        uch = uch,
        ucp = ucp,
        hb = hb,
        esa = esa,
        is = is,
        jsa = jsa)
}



