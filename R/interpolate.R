### Functions for interpolating the data into monthly time series

# Generic functions -----------------------------------------------------------

get_months_sequence <- function(start_date, end_date) {
    seq(as.Date(start_date),
        as.Date(end_date),
        by = "months")
}

# Genereic interpolation functions --------------------------------------------------------------

get_months_df <- function(data, catcols, ...) {
    start_date <- min(data$date)
    end_date <- max(data$date)
    date <- get_months_sequence(start_date, end_date)
    df <- tidyr::crossing(..., date)
    colnames(df) <- catcols
    df
}

interpolate_dataset <- function(data, months_df, catcols, valcols, label) {
    idata <- months_df %>% dplyr::left_join(data, by = catcols)
    idata <- tidyr::fill(idata, colnames(data[-valcols]))
    idata[[label]] <- ifelse(is.na(idata[[valcols[[1]]]]), 1, 0)
    idata %>%
        dplyr::select(valcols) %>%
        purrr::map_dfc(~ zoo::na.approx(.)) %>%
        dplyr::bind_cols(idata %>% dplyr::select(-valcols), .) %>%
        dplyr::select(
            colnames(data[-valcols]),
            colnames(data[valcols]),
            (!! label))
}

# Dataset specific interpolation functions ------------------------------------

interpolate_esa <- function(esa) {
    valcols <- 5
    catcols <- c("gid", "esa_payment_type", "date")
    months_df <- get_months_df(esa, catcols, esa$gid, esa$esa_payment_type)
    interpolate_dataset(esa, months_df, catcols, valcols, "esa_int")
}

interpolate_is <- function(is) {
    valcols <- 4
    catcols <- c("gid", "date")
    months_df <- get_months_df(is, catcols, is$gid)
    interpolate_dataset(is, months_df, catcols, valcols, "is_int")
}

interpolate_jsa <- function(jsa) {
    valcols <- 5
    catcols <- c("gid", "jsa_item", "date")
    months_df <- get_months_df(jsa, catcols, jsa$gid, jsa$jsa_item)
    interpolate_dataset(jsa, months_df, catcols, valcols, "jsa_int")
}

interpolate_hmrc <- function(hmrc) {
    valcols <- 4:13
    catcols <- c("gid", "date")
    months_df <- get_months_df(hmrc, catcols, hmrc$gid)
    interpolate_dataset(hmrc, months_df, catcols, valcols, "hmrc_int")
}

