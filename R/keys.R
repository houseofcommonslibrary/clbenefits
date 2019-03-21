### Functions for handling API keys

# Cache -----------------------------------------------------------------------

cache <- new.env(parent = emptyenv())

# Constants -------------------------------------------------------------------

CACHE_NM_API_KEY = "nm_api_key"

# Load Stat-Xplore API key ----------------------------------------------------

set_sx_api_key <- function(api_key) {
    statxplorer::set_api_key(api_key)
}

load_sx_api_key <- function(filename) {
    statxplorer::load_api_key(filename)
}

# Load Nomis API key ----------------------------------------------------------

set_nm_api_key <- function(api_key) {
    assign(CACHE_NM_API_KEY, api_key, envir = cache)
}

load_nm_api_key <- function(filename) {
    api_key = stringr::str_trim(readr::read_file(filename))
    set_nm_api_key(api_key)
}

get_nm_api_key <- function() {
    if (! exists(CACHE_NM_API_KEY, envir = cache)) {
        stop(stringr::str_c(
            "No Nomis API key provided: use ",
            "clbenefits::set_nm_api_key() or clbenefits::load_nm_api_key() ",
            "to provide your key"))
    }
    get(CACHE_NM_API_KEY, envir = cache)
}


