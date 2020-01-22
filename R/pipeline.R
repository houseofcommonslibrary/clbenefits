### Functions for running and managing the pipeline

# Config ----------------------------------------------------------------------

#' Load the config file
#'
#' \code{load_config} loads the config file from the pipeline directory.
#'
#' @export

load_config <- function() {

    config_str <- readr::read_file(CONFIG_FILE)
    config <- jsonlite::fromJSON(config_str)

    if (! CONFIG_NAME_SX_KEY %in% names(config)) {
        stop("No Stat-Xplore API key provided in key configuration file")
    }

    set_sx_api_key(config[[CONFIG_NAME_SX_KEY]])
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

    sx <- fetch_sx(verbose)
    hmrc <- read_hmrc(verbose)
    jsa <- merge_jsa(sx$jsa, verbose)

    report("Writing Stat-Xplore data")
    purrr::map(names(sx), function(name) {
        filename <- file.path(SX_OUTPUT_DIR, stringr::str_glue("{name}.csv"))
        readr::write_csv(sx[[name]], filename)
    })

    report("Writing HMRC data")
    filename <- file.path(HMRC_OUTPUT_DIR, "hmrc.csv")
    readr::write_csv(hmrc, filename)

    report("Writing JSA data")
    filename <- file.path(JSA_OUTPUT_DIR, "jsa.csv")
    readr::write_csv(jsa, filename)

    list(
        uch = sx$uch,
        ucp = sx$ucp,
        hb = sx$hb,
        esa = sx$esa,
        is = sx$is,
        jsa = jsa,
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

    is <- interpolate_is(fdata$is) %>%
        dplyr::filter(.data$date >= MASTER_START_DATE)

    jsa <- interpolate_jsa(fdata$jsa) %>%
        dplyr::filter(.data$date >= MASTER_START_DATE)

    hmrc <- interpolate_hmrc(fdata$hmrc) %>%
        dplyr::filter(.data$date >= MASTER_START_DATE)

    list(
        uch = uch,
        ucp = ucp,
        hb = hb,
        esa = esa,
        is = is,
        jsa = jsa,
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

    hmrc <- idata$hmrc %>% dplyr::select(-.data$geography)

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
        # Add interpolation status 2 for row values that will be rolled forward
        tidyr::replace_na(list(
            uch_int = 2,
            ucp_int = 2,
            hb_int = 2,
            esa_int = 2,
            is_int = 2,
            jsa_int = 2,
            hmrc_int =2)) %>%
        # Fill down the interpolated columns to roll forward last value
        tidyr::fill(-dplyr::ends_with("_int")) %>%
        # Add column for legacy benefits total
        dplyr::mutate(
            legacy_total = .data$hb_not_emp +
                .data$esa +
                .data$is +
                .data$jsa +
                .data$hmrc_in_work_total)
}

#' Convert the master table into a table for Power BI
#'
#' @param master output table created in the pipeline.
#' @return A table with the data structured for Power BI.
#' @export

create_powerbi <- function(master) {

    con_legacy <- master %>%
        dplyr::filter(.data$gid %in% CON_REGION_LOOKUP$gid) %>%
        dplyr::left_join(CON_REGION_LOOKUP, by = "gid") %>%
        dplyr::mutate(
            country_id = "K03000001",
            country_name = "Great Britain") %>%
        dplyr::select(
            .data$date,
            constituency_id = .data$gid,
            constituency_name = .data$geography,
            .data$region_id,
            .data$region_name,
            .data$country_id,
            .data$country_name,
            .data$hb_total,
            .data$hmrc_wc_total,
            .data$esa,
            .data$jsa,
            .data$legacy_total) %>%
        tidyr::gather(
            key = "legacy_measure",
            value = "legacy_value",
            -c(.data$date,
                .data$constituency_id,
                .data$constituency_name,
                .data$region_id,
                .data$region_name,
                .data$country_id,
                .data$country_name)) %>%
        dplyr::mutate(
            benefit_type = unname(LEGACY_BENEFIT_TYPES[.data$legacy_measure]))

    con_uc <- master %>%
        dplyr::filter(.data$gid %in% CON_REGION_LOOKUP$gid) %>%
        dplyr::left_join(CON_REGION_LOOKUP, by = "gid") %>%
        dplyr::mutate(
            country_id = "K03000001",
            country_name = "Great Britain") %>%
        dplyr::select(
            .data$date,
            constituency_id = .data$gid,
            constituency_name = .data$geography,
            .data$region_id,
            .data$region_name,
            .data$country_id,
            .data$country_name,
            .data$uch_int,
            .data$uch_housing,
            .data$uch_child,
            .data$uch_capability,
            .data$ucp_search_work,
            .data$uch_total) %>%
        tidyr::gather(
            key = "uc_measure",
            value = "uc_value",
            -c(.data$date,
                .data$constituency_id,
                .data$constituency_name,
                .data$region_id,
                .data$region_name,
                .data$country_id,
                .data$country_name,
                .data$uch_int)) %>%
        dplyr::mutate(
            benefit_type = unname(UC_BENEFIT_TYPES[.data$uc_measure]))

    con <- dplyr::left_join(
        con_legacy,
        con_uc,
        by = c(
            "date",
            "constituency_id",
            "constituency_name",
            "region_id",
            "region_name",
            "country_id",
            "country_name",
            "benefit_type")) %>%
        dplyr::mutate(
            pc_uc = ifelse(
                .data$legacy_value == 0 & .data$uc_value == 0, 0,
                .data$uc_value / (.data$legacy_value + .data$uc_value))) %>%
        dplyr::group_by(date, .data$region_id, .data$benefit_type) %>%
        dplyr::mutate(rank_region = rank(-.data$pc_uc, ties.method = "min")) %>%
        dplyr::ungroup() %>%
        dplyr::group_by(.data$date, .data$benefit_type) %>%
        dplyr::mutate(rank_country = rank(-.data$pc_uc, ties.method = "min")) %>%
        dplyr::ungroup()

    con_child <-  dplyr::left_join(
        con %>%
            dplyr::filter(.data$uc_measure == "uch_child") %>%
            dplyr::select(
                .data$date,
                .data$constituency_id,
                uch_child = .data$uc_value),
        con %>%
            dplyr::filter(.data$uc_measure == "uch_total") %>%
            dplyr::select(
                .data$date,
                .data$constituency_id,
                uch_total = .data$uc_value),
        by = c("date", "constituency_id")) %>%
        dplyr::mutate(
            pc_uc_child = ifelse(
                .data$uch_child == 0, 0,
                .data$uch_child / .data$uch_total))

    con <- dplyr::left_join(
        con,
        con_child %>%
            dplyr::select(
                .data$date,
                .data$constituency_id,
                .data$pc_uc_child),
        by = c("date", "constituency_id")) %>%
        dplyr::select(
            .data$date,
            .data$constituency_id,
            .data$constituency_name,
            .data$region_id,
            .data$region_name,
            .data$country_id,
            .data$country_name,
            .data$benefit_type,
            dplyr::everything()) %>%
        dplyr::mutate(area_type = "constituency")

    reg_legacy <- master %>%
        dplyr::filter(
            ! .data$gid %in% CON_REGION_LOOKUP$gid,
            .data$gid != "K03000001") %>%
        dplyr::select(
            .data$date,
            region_id = .data$gid,
            region_name = .data$geography,
            .data$hb_total,
            .data$hmrc_wc_total,
            .data$esa,
            .data$jsa,
            .data$legacy_total) %>%
        tidyr::gather(
            key = "legacy_measure",
            value = "legacy_value",
            -c(.data$date,
               .data$region_id,
               .data$region_name)) %>%
        dplyr::mutate(
            benefit_type = unname(LEGACY_BENEFIT_TYPES[.data$legacy_measure]))

    reg_uc <- master %>%
        dplyr::filter(
            ! .data$gid %in% CON_REGION_LOOKUP$gid,
            .data$gid != "K03000001") %>%
        dplyr::select(
            .data$date,
            region_id = .data$gid,
            region_name = .data$geography,
            .data$uch_int,
            .data$uch_housing,
            .data$uch_child,
            .data$uch_capability,
            .data$ucp_search_work,
            .data$uch_total) %>%
        tidyr::gather(
            key = "uc_measure",
            value = "uc_value",
            -c(.data$date,
               .data$region_id,
               .data$region_name,
               .data$uch_int)) %>%
        dplyr::mutate(
            benefit_type = unname(UC_BENEFIT_TYPES[.data$uc_measure]))

     reg <- dplyr::left_join(
        reg_legacy,
        reg_uc,
        by = c(
            "date",
            "region_id",
            "region_name",
            "benefit_type")) %>%
        dplyr::mutate(
            pc_uc = ifelse(
                .data$legacy_value == 0 & .data$uc_value == 0, 0,
                .data$uc_value / (.data$legacy_value + .data$uc_value))) %>%
         dplyr::select(
            .data$date,
            .data$region_id,
            .data$region_name,
            .data$benefit_type,
            dplyr::everything())

    gb_legacy <- master %>%
        dplyr::filter(.data$gid == "K03000001") %>%
        dplyr::select(
            .data$date,
            country_id = .data$gid,
            country_name = .data$geography,
            .data$hb_total,
            .data$hmrc_wc_total,
            .data$esa,
            .data$jsa,
            .data$legacy_total) %>%
        tidyr::gather(
            key = "legacy_measure",
            value = "legacy_value",
            -c(.data$date,
               .data$country_id,
               .data$country_name)) %>%
        dplyr::mutate(
            benefit_type = unname(LEGACY_BENEFIT_TYPES[.data$legacy_measure]))

    gb_uc <- master %>%
        dplyr::filter(.data$gid == "K03000001") %>%
        dplyr::select(
            .data$date,
            country_id = .data$gid,
            country_name = .data$geography,
            .data$uch_int,
            .data$uch_housing,
            .data$uch_child,
            .data$uch_capability,
            .data$ucp_search_work,
            .data$uch_total) %>%
        tidyr::gather(
            key = "uc_measure",
            value = "uc_value",
            -c(.data$date,
               .data$country_id,
               .data$country_name,
               .data$uch_int)) %>%
        dplyr::mutate(
            benefit_type = unname(UC_BENEFIT_TYPES[.data$uc_measure]))

    gb <- dplyr::left_join(
        gb_legacy,
        gb_uc,
        by = c(
            "date",
            "country_id",
            "country_name",
            "benefit_type")) %>%
        dplyr::mutate(
            pc_uc = ifelse(
                .data$legacy_value == 0 & .data$uc_value == 0, 0,
                .data$uc_value / (.data$legacy_value + .data$uc_value))) %>%
        dplyr::select(
            .data$date,
            .data$country_id,
            .data$country_name,
            .data$benefit_type,
            dplyr::everything())

     con_frame <- con %>%
         dplyr::select(
            .data$date,
            .data$constituency_id,
            .data$constituency_name,
            .data$region_id,
            .data$region_name,
            .data$country_id,
            .data$country_name,
            .data$benefit_type)

    con_reg <- dplyr::left_join(
        con_frame,
        reg,
        by = c(
            "date",
            "region_id",
            "region_name",
            "benefit_type")) %>%
        dplyr::mutate(
            rank_region = -1,
            rank_country = -1,
            pc_uc_child = -1,
            area_type = "region")

    con_gb <- dplyr::left_join(
        con_frame,
        gb,
        by = c(
            "date",
            "country_id",
            "country_name",
            "benefit_type")) %>%
        dplyr::mutate(
            rank_region = -1,
            rank_country = -1,
            pc_uc_child = -1,
            area_type = "country")

    dplyr::bind_rows(con, con_reg, con_gb) %>%
        add_powerbi_labels() %>%
        dplyr::select(
            .data$date,
            .data$constituency_id,
            .data$constituency_name,
            .data$region_id,
            .data$region_name,
            .data$country_id,
            .data$country_name,
            .data$area_type,
            .data$area_name,
            .data$benefit_type,
            .data$benefit_name,
            .data$legacy_measure,
            .data$legacy_value,
            .data$uc_measure,
            .data$uc_value,
            dplyr::everything()) %>%
        dplyr::arrange(
            .data$constituency_id,
            .data$date,
            .data$benefit_type)
}

#' Add additional columns of labels to the Power BI table
#'
#' @param powerbi The table of core data structured for Power BI.
#' @return A table structured for Power BI with additional label columns.
#' @keywords internal

add_powerbi_labels <- function(powerbi) {


    # Add area name labels based on area type
    area_cols = list(
        powerbi$constituency_name,
        powerbi$region_name,
        powerbi$country_name,
        powerbi$area_type)

    powerbi$area_name <- purrr::pmap_chr(
        area_cols,
        function(constituency_name, region_name, country_name, area_type) {

            area_type_lookup <- list(
                "constituency" = constituency_name,
                "region" = region_name,
                "country" = country_name)

            area_type_lookup[[area_type]]
    })


    # Add benefit labels based on benefit_type
    benefit_type_lookup <- list(
        "children" = "Children (no. of households)",
        "housing" = "Housing costs (no. of households)",
        "incapacity" = "Incapacity (no. of households)",
        "unemployment" = "Jobseekers (no. of individuals)",
        "total" = "Total: all households")

    powerbi$benefit_name <- purrr::map_chr(
        powerbi$benefit_type,
        function(benefit_type) {
            benefit_type_lookup[[benefit_type]]
    })

    # Add row number indicators based on benefit type and area type
    row_number_lookup <- list(
    "children" = list(
        "constituency" = 7,
        "region" = 8,
        "country" = 9),
    "housing" = list(
        "constituency" = 4,
        "region" = 5,
        "country" = 6),
    "incapacity" = list(
        "constituency" = 10,
        "region" = 11,
        "country" = 12),
    "unemployment" = list(
        "constituency" = 13,
        "region" = 14,
        "country" = 15),
    "total" = list(
        "constituency" = 1,
        "region" = 2,
        "country" = 3))

    powerbi$row_number <- as.integer(purrr::pmap_chr(
        list(powerbi$benefit_type, powerbi$area_type),
        function(benefit_type, area_type) {
            row_number_lookup[[benefit_type]][[area_type]]
    }))

    # Add constituency count based on region
    const_count_lookup <- list(
        "E12000001" = 29,
        "E12000002" = 75,
        "E12000003" = 54,
        "E12000004" = 46,
        "E12000005" = 59,
        "E12000006" = 58,
        "E12000007" = 73,
        "E12000008" = 84,
        "E12000009" = 55,
        "S92000003" = 59,
        "W92000004" = 40)

    powerbi$region_const_count <- as.integer(purrr::map_chr(
        powerbi$region_id,
        function(region_id) {
            const_count_lookup[[region_id]]
    }))

    powerbi
}

#' Run the pipeline
#'
#' \code{pipeline} runs the full pipeline.
#'
#' @return A list containing the data, the interpolated data, and the master
#'   dataframe of results.
#' @export

pipeline <- function() {

    load_config()

    fdata <- fetch_data()
    idata <- interpolate_data(fdata)
    master <- create_master(idata)
    powerbi <- create_powerbi(master)

    data <- list(
        fdata = fdata,
        idata = idata,
        master = master,
        powerbi = powerbi)

    report("Writing master benefits data file")
    filename <- file.path(OUTPUT_DIR, "universal-credit-master-data.csv")
    readr::write_csv(data$master, filename)

    report("Writing Power BI data file")
    filename <- file.path(OUTPUT_DIR, "universal-credit-power-bi.csv")
    readr::write_csv(data$powerbi, filename)

    report("Writing all data as RDS")
    saveRDS(data, file.path(OUTPUT_DIR, "data.rds"))
    data
}

