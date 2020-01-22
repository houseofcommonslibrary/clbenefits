### Functions for handling API keys

# Cache -----------------------------------------------------------------------

cache <- new.env(parent = emptyenv())

# Load Stat-Xplore API key ----------------------------------------------------

set_sx_api_key <- function(api_key) {
    statxplorer::set_api_key(api_key)
}

load_sx_api_key <- function(filename) {
    statxplorer::load_api_key(filename)
}
