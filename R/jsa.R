### Functions for processing data on JSA

# Read historic JSA data ------------------------------------------------------

#' Read historic data on tax JSA and return it as a dataframe
#'
#' \code{read_historic_jsa} reads the data on historic JSA from a csv in the
#' input directory and returns the data as a dataframe.
#'
#' @param verbose A boolean indicating whether the progress of the function
#'   should be printed to the console.
#'
#' @return Historic JSA data as a dataframe.
#' @export

read_historic_jsa <- function(verbose = TRUE) {
    if (verbose) report("Reading data on historic JSA")
    readr::read_csv(
        JSA_INPUT_FILE,
        col_types = readr::cols(
            gid = readr::col_character(),
            date = readr::col_date(),
            jsa = readr::col_double()))
}

#  Merge JSA data -------------------------------------------------------------

#' Combine historic JSA data with the Stat-Xplore JSA data and estimate the
#' income-based element from February 2019.
#'
#' \code{merge_jsa} combines the historic JSA figures in the input directory
#' with more recent figrues from Stat-Xplore. The historic data covers the
#' period from August 2015 to February 2019 inclusive. Data is taken from
#' Stat-Xplore for months since February 2019. To estimate the proportion of
#' JSA claimants that are income based in the Stat-Xplore data, a fixed
#' proportion has been calculated for February 2019 and this is reduced by
#' linear interpolation to the assumed end of the UC rollout period.
#'
#' @param sx_jsa A dataframe of JSA data returned from \code{fetch_sx}.
#' @param verbose A boolean indicating whether the progress of the function
#'   should be printed to the console.
#'
#' @return Historic and current JSA data merged into a single dataframe.
#' @export

merge_jsa <- function(sx_jsa, verbose = TRUE) {

    # Load the historic data to Feb 2019
    historic_jsa <- read_historic_jsa(verbose)
    if (verbose) report("Merging historic and recent JSA data")

    # From Stat-Xplore get just the data since Feb 2019
    sx_jsa <- sx_jsa %>%
        dplyr::filter(.data$date > JSA_CROSSOVER_DATE) %>%
        dplyr::select(-.data$amount)

    # Calculate the number of months from Feb 2019 to the end of the rollout
    rollout_interval <- lubridate::interval(JSA_CROSSOVER_DATE, JSA_END_DATE)
    rollout_delta <- rollout_interval / months(1)

    # For each row reduce the JSA figure to the proportion that is income based
    sx_jsa$jsa <- purrr::map2_dbl(sx_jsa$date, sx_jsa$jsa, function(d, jsa) {
        delta <-  lubridate::interval(d, JSA_END_DATE) / months(1)
        proportion <- delta / rollout_delta * JSA_PROPORTION_FEB19
        proportion * jsa
    })

    dplyr::bind_rows(historic_jsa, sx_jsa) %>%
        dplyr::arrange(.data$gid, .data$date)
}
