#' clbenefits: Download and process a range of benefits data
#'
#' The clbenefits package implements a pipeline for dowloading and integrating
#' a collection of data on benefits claimed within Parliamentary
#' constituencies.
#'
#' @docType package
#' @name clbenefits
#' @importFrom magrittr %>%
#' @importFrom rlang .data
NULL

# Tell R CMD check about new operators
if(getRversion() >= "2.15.1") {
    utils::globalVariables(c(".", ":="))
}
