### Functions for tapering legacy benefit numbers after the last observation

# Generic functions -----------------------------------------------------------

taper_benefit <- function(master, benefit_cols, int_col, taper_end_date) {

    # Split the master into rows with and without interpolation status 2
    excluded <- master %>%
        dplyr::filter(.data[[int_col]] != 2)

    target <- master %>%
        dplyr::filter(.data[[int_col]] == 2) %>%
        dplyr::arrange(.data$gid, .data$date)

    # If there are no rows to interpolate return the master table unmodified
    if (nrow(target) == 0) return(master)

    # Otherwise taper the target rows for each gid
    tapered <- target %>%
        dplyr::group_by(.data$gid) %>%
        dplyr::group_modify(function(df, group) {

            # Calculate the number of months
            start_date <- min(df$date)
            taper_interval <- lubridate::interval(start_date, taper_end_date)
            taper_delta <- taper_interval / months(1)

            # Taper each target column
            for (col in benefit_cols) {
                iterator <- get_taper_iterator(taper_delta)
                df[[col]] <- purrr::map_dbl(df[[col]], iterator)
            }

            df[[int_col]] <- 3
            df
    })

    # Combine the tapered and excluded rows and re-arrange
    dplyr::bind_rows(excluded, tapered) %>%
        dplyr::arrange(.data$gid, .data$date)
}

get_taper_iterator <- function(taper_delta) {
    i <- 1
    function(benefit_val) {
        decrement <- benefit_val / taper_delta
        new_benefit_val <- benefit_val - (i * decrement)
        i <<- i + 1
        new_benefit_val
    }
}

# Dataset specific taper functions --------------------------------------------

taper_hb <- function(master) {

    benefit_cols <- c("hb_not_emp", "hb_total")
    int_col <- "hb_int"
    taper_end_date <- ROLLOUT_END_DATE
    taper_benefit(master, benefit_cols, int_col, taper_end_date)
}

taper_esa <- function(master) {

    benefit_cols <- "esa"
    int_col <- "esa_int"
    taper_end_date <- ROLLOUT_END_DATE
    taper_benefit(master, benefit_cols, int_col, taper_end_date)
}

taper_is <- function(master) {

    benefit_cols <- "is"
    int_col <- "is_int"
    taper_end_date <- ROLLOUT_END_DATE
    taper_benefit(master, benefit_cols, int_col, taper_end_date)
}

taper_jsa <- function(master) {

    benefit_cols <- "jsa"
    int_col <- "jsa_int"
    taper_end_date <- ROLLOUT_END_DATE
    taper_benefit(master, benefit_cols, int_col, taper_end_date)
}

taper_hmrc <- function(master) {

    benefit_cols <- c(
        "hmrc_wc_oow_f",
        "hmrc_wc_oow_c",
        "hmrc_wc_wtc_ctc_f",
        "hmrc_wc_wtc_ctc_c",
        "hmrc_wc_ctc_only_f",
        "hmrc_wc_ctc_only_c",
        "hmrc_wc_childcare_f",
        "hmrc_wnc_total",
        "hmrc_total_number",
        "hmrc_total_range",
        "hmrc_in_work_total",
        "hmrc_wc_total")

    int_col <- "hmrc_int"
    taper_end_date <- ROLLOUT_END_DATE
    taper_benefit(master, benefit_cols, int_col, taper_end_date)
}

