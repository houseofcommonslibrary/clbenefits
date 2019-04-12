### Functions for downloading and processing data from Nomis

# Constants -------------------------------------------------------------------

NM_DATE_RANGE_TOKEN <- "<date_range>"
NM_API_KEY_TOKEN <- "<api_key>"
NM_UC_START_DATE <- as.Date("2013-02-01")

# Generic function to fetch Nomis tables --------------------------------------

fetch_nm_table <- function(url) {
    headers <- httr::add_headers("accept" = "application/json")
    response <- httr::GET(url, headers)
    response_text <- httr::content(response, as = "text", encoding = "utf-8")
    tibble::as_tibble(rjstat::fromJSONstat(response_text))
}

# Functions for parsing dates -------------------------------------------------

parse_nm_months <- function(m) {
    m <- stringr::str_glue("1 {m}")
    lubridate::dmy(m)
}

# Fetch the latest date for each dataset --------------------------------------

fetch_nm_latest_quarter <- function(date_query_url) {
    results <- fetch_nm_table(date_query_url)
    if(length(results$date) != 1) stop("Query error: too many months")
    parse_nm_months(results$date)
}

fetch_nm_latest_is <- function() {
    fetch_nm_latest_quarter(NM_IS_DATE_URL)
}

fetch_nm_latest_jsa <- function() {
    fetch_nm_latest_quarter(NM_JSA_DATE_URL)
}

# Fetch the date range for each dataset ---------------------------------------

fetch_nm_url <- function(start_date, fetch_date_func, interval, url) {
    api_key <- get_nm_api_key()
    latest_date <- fetch_date_func()
    delta <- lubridate::interval(start_date, latest_date)
    num_months <- suppressMessages(delta %/% months(interval))
    if (num_months %% 1 != 0) stop("Date range error: fractional months")
    date_range <- stringr::str_glue("latestMINUS{num_months}-latest")
    url <- stringr::str_replace(url, NM_DATE_RANGE_TOKEN, date_range)
    stringr::str_replace(url, NM_API_KEY_TOKEN, api_key)
}

# Fetch datasets --------------------------------------------------------------

fetch_nm_is <- function() {

    url <- fetch_nm_url(NM_UC_START_DATE, fetch_nm_latest_is, 3, NM_IS_URL)
    data <- fetch_nm_table(url)
    data$date <- parse_nm_months(data$date)

    data <- data %>% dplyr::left_join(
        NM_GEOGRAPHY_LOOKUP,
        by = "geography")

    data <- data %>%
        dplyr::filter(.data$gid != "ZZXXXXXXX") %>%
        dplyr::select(
            .data$gid,
            .data$geography,
            .data$date,
            .data$value) %>%
        dplyr::arrange(
            .data$gid,
            .data$date)

    colnames(data) <- c(
        "gid",
        "geography",
        "date",
        "is")

    data
}

fetch_nm_jsa <- function() {

    url <- fetch_nm_url(NM_UC_START_DATE, fetch_nm_latest_jsa, 3, NM_JSA_URL)
    data <- fetch_nm_table(url)
    data$date <- parse_nm_months(data$date)

    data <- data %>% dplyr::left_join(
        NM_GEOGRAPHY_LOOKUP,
        by = "geography")

    data <- data %>%
        dplyr::select(
            .data$gid,
            .data$geography,
            .data$date,
            .data$item,
            .data$value)%>%
        dplyr::arrange(
            .data$gid,
            .data$item,
            .data$date)

    colnames(data) <- c(
        "gid",
        "geography",
        "date",
        "jsa_item",
        "jsa")

    # Replace NAs with zeros excpet where month is August 2016
    exclude_months <- as.Date(c("2016-08-01"))

    data$jsa <- purrr::map2_dbl(
        data$date, data$jsa, function(d, c) {
        if((! d %in% exclude_months) && (is.na(c))) return(0)
        return(c)
    })

    data
}

# Fetch all datasets ----------------------------------------------------------

#' Fetch all data from Nomis and return as a list of dataframes
#'
#' \code{fetch_nm} fetches all data from from Nomis and returns the
#' datasets as a list of dataframes.
#'
#' @param verbose A boolean indicating whether the progress of the function
#'   should be printed to the console.
#' @return A list of the results sets as dataframes.
#' @export


fetch_nm <- function(verbose = TRUE) {

    if (verbose) report("Fetching Nomis data on Income Support")
    is <- fetch_nm_is()

    if (verbose) report("Fetching Nomis data on JSA")
    jsa <- fetch_nm_jsa()

    list(
        is = is,
        jsa = jsa)
}
