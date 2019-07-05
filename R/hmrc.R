### Functions for downloading and processing data from hmrc on gov.uk

# Constants -------------------------------------------------------------------

HMRC_TC_URL <- stringr::str_c(
    "https://www.gov.uk/government/statistics/child-and-working-tax-credits-",
    "statistics-provisional-awards-geographical-analyses-december-2013")

HMRC_TC_FILENAME <- "tax-credits"

# Parse date string for filename ----------------------------------------------

parse_hmrc_date_str <- function(date_str) {
    year <- lubridate::year(date_str)
    month <- stringr::str_pad(lubridate::month(date_str), 2, "left", "0")
    day <- stringr::str_pad(lubridate::day(date_str), 2, "left", "0")
    stringr::str_glue("{year}-{month}-{day}")
}

# Get tax credit filename -----------------------------------------------------

get_tax_credits_excelname <- function(date_str) {
    stringr::str_glue("{HMRC_TC_FILENAME}-{date_str}.xlsx")
}

get_tax_credits_csvname <- function(date_str) {
    stringr::str_glue("{HMRC_TC_FILENAME}-{date_str}.csv")
}

# Fetch hmrc excel file from gov.uk -------------------------------------------

#' Fetch the latest hmrc tax credits data
#'
#' \code{fetch_hmrc_excel} attempts to download the latest tax credits excel
#' file from gov.uk.
#'
#' @param url The url for hrmc geographical tax credits data.
#' @param excelname A filepath for the excel file download.
#' @export

fetch_hmrc_excel <- function(url, excelname) {
    tryCatch({
        html <- xml2::read_html(url)
        href <- html %>%
            rvest::html_nodes("h2.title a") %>%
            rvest::html_attr("href") %>%
            .[[1]]
        excel <- readr::read_file_raw(href)
        readr::write_file(excel, excelname)},
        error = function(c) {
            stop("Could not download the tax credits spreadsheet from HMRC")
    })
}

# Import an hmrc excel file ---------------------------------------------------

#' Import an excel file of hmrc tax credit data and convert to a dataframe
#'
#' \code{import_hmrc_excel} takes the path to an excel file of hmrc tax credits
#' data and attempts to convert it to a csv. You must provide the function with
#' the information it needs to extract the correct data.
#'
#' @param excelname A filepath for an excel file of hmrc tax credits data.
#' @param sheet A string specifying the name of the worrksheet that contains
#'   data on Parliamentary constituencies.
#' @param skip An integer specifying the number of rows to skip before the
#'   lowest header row.
#' @param cols A vector of integers specifying the numbers of the columns that
#'   contain the expected data, in the expected sequence.
#' @param date A date representing the date of the dataset. This is typically
#'   the first of the month for which the dataset is published.
#' @return A dataframe of the extracted tax credits data.
#' @export

import_hmrc_excel <- function(excelname, sheet, skip, cols, date) {

    # Read the excel file, select the columns, and remove blank rows
    df <- readxl::read_excel(excelname, sheet = sheet, skip = skip)
    df <- df %>% dplyr::select(cols)
    df <- df %>% dplyr::filter(! is.na(df[[length(df)]]))

    # Set the column names
    colnames(df) <- c(
        "gid",
        "geog_1",
        "geog_2",
        "hrmc_wc_oow_f",
        "hrmc_wc_oow_c",
        "hrmc_wc_wtc_ctc_f",
        "hrmc_wc_wtc_ctc_c",
        "hrmc_wc_ctc_only_f",
        "hrmc_wc_ctc_only_c",
        "hrmc_wc_childcare_f",
        "hrmc_wc_wnc_f",
        "hrmc_total_number",
        "hrmc_total_range")

    # Combine the two geography columns
    df$geography <- ifelse(is.na(df$geog_2), df$geog_1, df$geog_2)

    # Create a date column
    df$date <- date

    # Remove the old geography columns and reorder
    df <- df %>%
        dplyr::select(
            -.data$geog_1,
            -.data$geog_2) %>%
        dplyr::select(
            .data$gid,
            .data$geography,
            .data$date,
            dplyr::everything())

    # Find the row with a foreign geography and set the code
    df$gid <- purrr::map2_chr(df$geography, df$gid, function(geography, gid) {
        if (stringr::str_detect(geography, "FOREIGN")) return("ZZXXXXXXX")
        gid
    })

    # Replace missing values in data columns with zeros and convert to numeric
    dfn <- purrr::map_dfc(df[4:length(df)], function(x) {
        x[is.na(x)] <- "0"
        as.double(stringr::str_replace_all(x, "-", "0"))
    })

    # Rejoin the number columns with the character columns and return
    dplyr::bind_cols(df[1:3], dfn)
}

# Convert spreadsheet to csvs -------------------------------------------------

#' Convert an excel file of hmrc tax credit data to a csv
#'
#' \code{convert_hmrc_excel} takes the path to an excel file of hmrc tax credits
#' data and attempts to convert it to a csv. You must provide the function with
#' the information it needs to extract the correct data.
#'
#' @param excelname A filepath for an excel file of hmrc tax credits data.
#' @param sheet A string specifying the name of the worrksheet that contains
#'   data on Parliamentary constituencies.
#' @param skip An integer specifying the number of rows to skip before the
#'   lowest header row.
#' @param cols A vector of integers specifying the numbers of the columns that
#'   contain the expected data, in the expected sequence.
#' @param date A date representing the date of the dataset. This is typically
#'   the first of the month for which the dataset is published.
#' @param csvname A filepath for the output csv file.
#' @return A dataframe of the extracted tax credits data.
#' @export

convert_hmrc_excel <- function(excelname, sheet, skip, cols, date, csvname) {
    df <- import_hmrc_excel(excelname, sheet, skip, cols, date)
    readr::write_csv(df, csvname)
    df
}

# Read HMRC data file from inputs ---------------------------------------------

read_hmrc_csv <- function(csvname) {
    filename <- file.path(HMRC_CSV_INPUT_DIR, csvname)
    readr::read_csv(
        filename,
        col_types = readr::cols(
            gid = readr::col_character(),
            geography = readr::col_character(),
            date = readr::col_date(),
            hmrc_wc_oow_f = readr::col_double(),
            hmrc_wc_oow_c = readr::col_double(),
            hmrc_wc_wtc_ctc_f = readr::col_double(),
            hmrc_wc_wtc_ctc_c = readr::col_double(),
            hmrc_wc_ctc_only_f = readr::col_double(),
            hmrc_wc_ctc_only_c = readr::col_double(),
            hmrc_wc_childcare_f = readr::col_double(),
            hmrc_wnc_total = readr::col_double(),
            hmrc_total_number = readr::col_double(),
            hmrc_total_range = readr::col_double()))
}

# Read in all hmrc data -------------------------------------------------------

#' Read all hmrc data on tax credits and return it as a single dataframe
#'
#' \code{read_hmrc} reads all the hmrc data on tax credits stored in the
#' archive of csvs in the input directory and returns the data as a single
#' dataframe.
#'
#' @param verbose A boolean indicating whether the progress of the function
#'   should be printed to the console.
#' @return A list of the results sets as dataframes.
#' @export

read_hmrc <- function(verbose = TRUE) {

    if (verbose) report("Reading HMRC data on Tax Credits")
    hmrc <- purrr::map_dfr(list.files(HMRC_CSV_INPUT_DIR), ~ read_hmrc_csv(.))
    hmrc %>%
        dplyr::filter(! .data$gid %in% c(
            "ZZXXXXXXX", "K02000001", "K04000001", "N92000002", "N06000001",
            "N06000002", "N06000003", "N06000004", "N06000005", "N06000006",
            "N06000007", "N06000008", "N06000009", "N06000010", "N06000011",
            "N06000012", "N06000013", "N06000014", "N06000015", "N06000016",
            "N06000017", "N06000018")) %>%
        dplyr::arrange(.data$gid, .data$date) %>%
        dplyr::mutate(
            hmrc_wc_oow_f = hmrc_wc_oow_f * 1000,
            hmrc_wc_oow_c = hmrc_wc_oow_c * 1000,
            hmrc_wc_wtc_ctc_f = hmrc_wc_wtc_ctc_f * 1000,
            hmrc_wc_wtc_ctc_c = hmrc_wc_wtc_ctc_c * 1000,
            hmrc_wc_ctc_only_f = hmrc_wc_ctc_only_f * 1000,
            hmrc_wc_ctc_only_c = hmrc_wc_ctc_only_c * 1000,
            hmrc_wc_childcare_f = hmrc_wc_childcare_f * 1000,
            hmrc_wnc_total = hmrc_wnc_total * 1000,
            hmrc_total_number = hmrc_total_number * 1000,
            hmrc_total_range = hmrc_total_range * 1000,
            hmrc_in_work_total = hmrc_total_number - hmrc_wc_oow_f,
            hmrc_wc_total = hmrc_total_number - hmrc_wnc_total)
}
