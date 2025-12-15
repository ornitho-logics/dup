#' @import methods utils stats RMariaDB RSQLite data.table
#' @import dbo apis
#' @import magrittr stringr glue pushoverr  anytime crayon forcats
#' @import foreach future doFuture
#' @import fs ssh
#' @import webp
#' @importFrom  imager width  height resize  imresize load.image save.image crop.borders
#' @importFrom jpeg readJPEG
#' @importFrom config get
#' @importFrom geodist geodist
#' @importFrom lubridate force_tz with_tz ymd_hms
#'
NULL


.onLoad <- function(libname, pkgname) {
  dcf <- read.dcf(file = system.file("DESCRIPTION", package = pkgname))
  packageStartupMessage(paste(pkgname, 'v.', dcf[, "Version"]))
}
