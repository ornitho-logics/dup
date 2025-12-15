#' ARGOS2 pipeline
#' @export
ARGOS2.downloadNew <- function() {
  crd = config::get(config = "argos_api")
  login = argos_login(un = crd$un, pwd = crd$pwd, wsdl_server = crd$wsdl_server)

  ids = argos_devlist(login)
  ids[, platformId := as.integer(platformId)]

  ldt = dbq(
    q = "SELECT  platformId,
          MAX(locationDate) AS last_locationDate
          FROM ARGOS2.locations
            GROUP BY platformId",
    db = "ARGOS2",
    server = "scidb"
  )
  ldt[, last_locationDate := force_tz(last_locationDate, tz = "UTC")]

  ldt = merge(ids, ldt, by = "platformId", all.x = TRUE)

  setorder(ldt, -last_locationDate)

  ten_days_ago = force_tz(Sys.Date() - 10, tz = "UTC") |> as.POSIXct()

  ldt[is.na(last_locationDate), last_locationDate := ten_days_ago]

  # to download after longer interruptions
  ldt[last_locationDate < ten_days_ago, last_locationDate := ten_days_ago]

  # Get data
  o = foreach(i = 1:nrow(ldt), .errorhandling = "pass") %do%
    {
      print(i)
      argos_data(
        login,
        platformId = ldt[i, platformId],
        startDate = ldt[i, last_locationDate] |> to_timestamp()
      )
    }

  o = o[!sapply(o, inherits, what = "error")]

  o = rbindlist(o, fill = TRUE, use.names = TRUE)

  setnames(o, make.unique(names(o)))

  o
}

#' @export
ARGOS2.prepare_locations <- function(X) {
  X[, .(
    programNumber,
    platformId,
    platformType,
    platformModel,
    locationDate,
    bestMsgDate,
    latitude,
    longitude,
    locationClass,
    errorRadius,
    semiMajor,
    semiMinor,
    orientation,
    satellite,
    duration,
    bestLevel,
    gpsSpeed,
    gpsHeading,
    hdop,
    compression,
    nopc,
    nbMessage,
    frequency,
    altitude
  )]
}

#' ARGOS2 pipeline
#' @export
ARGOS2.prepare_sensors <- function(X) {
  x = X[
    !formatName %in% c("ZE", "SPECIAL INFO FRAME #2"),
    .(
      platformId,
      timestamp = bestMsgDate,
      name,
      value,
      name.1,
      value.1,
      name.2,
      value.2,
      name.3,
      value.3,
      name.4,
      value.4,
      name.5,
      value.5,
      name.6,
      value.6,
      name.7,
      value.7
    )
  ]

  # long format
  x = melt(
    x,
    id.vars = c("platformId", "timestamp"),
    measure = patterns("^name", "^value"),
    variable.name = "sensor_id",
    value.name = c("sensor_info", "sensor_value")
  )

  x[!is.na(sensor_value)]
}

#' ARGOS2 pipeline
#' @export
ARGOS2.update <- function(x, what = c("locations", "sensors")) {
  con = dbcon(db = "ARGOS2", server = "scidb")
  ok = DBI::dbWriteTable(con, what, x, append = TRUE, row.names = FALSE)
  DBI::dbDisconnect(con)
  ok
}
