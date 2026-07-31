#' Update the GM database from Movebank
#'
#' Downloads GPS events directly from
#' the Movebank event endpoint. This includes events collected both during and
#' outside defined deployments. Only GPS measurements are written to the GM
#' `locations` table; deployment and other Movebank reference data are not
#' stored.
#'
#' Events are downloaded in one-day windows. An existing database resumes two
#' days before its latest timestamp.
#' The overlap and the `event_id` primary key make daily runs
#' idempotent and allow recently changed records to be refreshed.
#'
#' @param start_datetime Initial UTC datetime used when `locations` is empty.
#' @param overlap Amount of existing data to download again. Defaults to two
#'   days.
#' @param window Maximum duration of each Movebank request. Defaults to one
#'   day.
#' @param verbose Show download and insertion progress. Defaults to
#'   [interactive()].
#'
#' @return Invisibly, a data table summarising each downloaded window.
#' @export
GM_update <- function(
  start_datetime = "2024-05-01T00:00:00Z",
  overlap = lubridate::days(2),
  window = lubridate::days(1),
  verbose = interactive()
) {
  handle <- .gm_handle()
  study_id <- .gm_study_id(handle)
  last_timestamp <- .gm_watermark()
  target <- .gm_current_time()

  from <- if (is.na(last_timestamp)) {
    ymd_hms(start_datetime, tz = "UTC")
  } else {
    ymd_hms(last_timestamp, tz = "UTC") - overlap
  }

  results <- list()

  while (from < target) {
    to <- min(from + window, target)

    .gm_inform(
      verbose,
      glue(
        "GM [{.gm_display_time(from)} -> {.gm_display_time(to)}]: ",
        "downloading."
      )
    )

    downloaded <- .gm_download(
      handle,
      study_id,
      timestamp_start = from,
      timestamp_end = to
    )
    locations <- .gm_prepare_locations(downloaded)
    affected <- .gm_insert_locations(locations)

    result <- data.table(
      window_start = .gm_display_time(from),
      window_end = .gm_display_time(to),
      downloaded = nrow(downloaded),
      locations = nrow(locations),
      affected = affected
    )
    results[[length(results) + 1]] <- result

    .gm_inform(
      verbose,
      glue(
        "GM [{result$window_start} -> {result$window_end}]: ",
        "{result$downloaded} downloaded, ",
        "{result$locations} location(s) prepared."
      )
    )

    from <- to
  }

  .gm_inform(verbose, "GM: update complete.")
  invisible(rbindlist(results))
}

.gm_credentials <- function() {
  config::get("host_movebank")
}

.gm_handle <- function(credentials = .gm_credentials()) {
  move2::movebank_handle(
    username = credentials$user,
    password = credentials$pwd
  )
}

.gm_study_id <- function(handle) {
  move2::movebank_get_study_id(
    "Nomadic shorebirds: GM",
    handle = handle
  )
}

.gm_attributes <- function() {
  c(
    "event_id",
    "sensor_type_id",
    "deployment_id",
    "timestamp",
    "location_long",
    "location_lat",
    "activity_count",
    "external_temperature",
    "gps_hdop",
    "gps_satellite_count",
    "gps_vdop",
    "ground_speed",
    "heading",
    "height_above_msl",
    "tag_voltage"
  )
}

.gm_retrieve <- function(...) {
  move2::movebank_retrieve(...)
}

.gm_download <- function(
  handle,
  study_id,
  timestamp_start,
  timestamp_end
) {
  .gm_retrieve(
    entity_type = "event",
    study_id = study_id,
    sensor_type_id = "gps",
    timestamp_start = timestamp_start,
    timestamp_end = timestamp_end,
    attributes = .gm_attributes(),
    handle = handle,
    convert_spatial_columns = FALSE,
    progress = FALSE
  )
}

.gm_prepare_locations <- function(events) {
  locations <- data.table(
    event_id = events$event_id,
    sensor_type_id = events$sensor_type_id,
    timestamp = .gm_sql_time(events$timestamp),
    location_long = as.numeric(events$location_long),
    location_lat = as.numeric(events$location_lat),
    activity_count = as.integer(events$activity_count),
    external_temperature = as.numeric(events$external_temperature),
    gps_hdop = as.numeric(events$gps_hdop),
    gps_satellite_count = as.integer(events$gps_satellite_count),
    gps_vdop = as.numeric(events$gps_vdop),
    ground_speed = as.numeric(events$ground_speed),
    heading = as.numeric(events$heading),
    height_above_msl = as.numeric(events$height_above_msl),
    tag_voltage = as.numeric(events$tag_voltage)
  )

  locations[
    !is.na(locations[["event_id"]]) &
      !is.na(locations[["timestamp"]]) &
      !is.na(locations[["location_long"]]) &
      !is.na(locations[["location_lat"]])
  ]
}

.gm_watermark <- function() {
  connection <- dbcon(db = "GM", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbGetQuery(
    connection,
    "
    SELECT DATE_FORMAT(
      MAX(timestamp),
      '%Y-%m-%dT%H:%i:%s.%fZ'
    ) AS last_timestamp
    FROM locations
    "
  )$last_timestamp[1]
}

.gm_insert_locations <- function(locations) {
  if (nrow(locations) == 0) {
    return(0)
  }

  connection <- dbcon(db = "GM", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbWriteTable(
    connection,
    "gm_locations_stage",
    locations,
    temporary = TRUE,
    row.names = FALSE
  )

  statement <- "
    INSERT INTO locations (
      event_id,
      sensor_type_id,
      timestamp,
      location_long,
      location_lat,
      activity_count,
      external_temperature,
      gps_hdop,
      gps_satellite_count,
      gps_vdop,
      ground_speed,
      heading,
      height_above_msl,
      tag_voltage
    )
    SELECT
      event_id,
      sensor_type_id,
      timestamp,
      location_long,
      location_lat,
      activity_count,
      external_temperature,
      gps_hdop,
      gps_satellite_count,
      gps_vdop,
      ground_speed,
      heading,
      height_above_msl,
      tag_voltage
    FROM gm_locations_stage
    ON DUPLICATE KEY UPDATE
      sensor_type_id = VALUES(sensor_type_id),
      timestamp = VALUES(timestamp),
      location_long = VALUES(location_long),
      location_lat = VALUES(location_lat),
      activity_count = VALUES(activity_count),
      external_temperature = VALUES(external_temperature),
      gps_hdop = VALUES(gps_hdop),
      gps_satellite_count = VALUES(gps_satellite_count),
      gps_vdop = VALUES(gps_vdop),
      ground_speed = VALUES(ground_speed),
      heading = VALUES(heading),
      height_above_msl = VALUES(height_above_msl),
      tag_voltage = VALUES(tag_voltage)
  "

  DBI::dbExecute(connection, statement)
}

.gm_display_time <- function(x) {
  format(
    x,
    format = "%Y-%m-%dT%H:%M:%SZ",
    tz = "UTC"
  )
}

.gm_sql_time <- function(x) {
  missing <- is.na(x)
  milliseconds <- round(as.numeric(x) * 1000)
  seconds <- milliseconds %/% 1000
  fraction <- milliseconds %% 1000

  output <- glue(
    "{format(
      as.POSIXct(seconds, origin = '1970-01-01', tz = 'UTC'),
      format = '%Y-%m-%d %H:%M:%S',
      tz = 'UTC'
    )}.{sprintf('%03d', fraction)}"
  ) |>
    as.character()
  output[missing] <- NA_character_
  output
}

.gm_current_time <- function() {
  Sys.time() |>
    lubridate::with_tz("UTC")
}

.gm_inform <- function(verbose, text) {
  if (verbose) {
    message(text)
  }

  invisible(NULL)
}
