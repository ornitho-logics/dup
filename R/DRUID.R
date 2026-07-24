#' Run the DRUID database update
#'
#' Logs in to Ecotopia using the `druid_api` configuration, updates the device list, and downloads new and recently transmitted GPS records. Each device is updated independently, so a failure for one device does not discard data downloaded for the others.
#'
#' Devices without GPS rows are downloaded from 2000-01-01. Existing devices are resumed two days before their latest stored collection time, and rows whose `(id, timestamp)` key is already present are left unchanged.
#'
#' @param verbose Show stage and per-device progress. Defaults to
#'   [interactive()].
#'
#' @return Invisibly, a list containing the number of devices added and a GPS update summary by device.
#' @export
DRUID_update <- function(verbose = interactive()) {
  .druid_inform(verbose, "DRUID: authenticating with Ecotopia.")
  credentials <- .druid_credentials()

  logstring <- ecotopia_login(
    credentials$generic$un,
    credentials$generic$pwd,
    credentials$kw1,
    credentials$kw2,
    verbose = FALSE
  )

  .druid_inform(verbose, "DRUID: updating the device list.")
  devices_added <- .druid_update_device_list(logstring)
  .druid_inform(
    verbose,
    glue("DRUID: device list complete; {devices_added} new device(s).")
  )

  gps <- .druid_update_gps(logstring, verbose = verbose)
  .druid_inform(verbose, "DRUID: update complete.")

  invisible(list(
    devices_added = devices_added,
    gps = gps
  ))
}


.druid_inform <- function(verbose, text) {
  if (verbose) {
    message(text)
  }

  invisible(NULL)
}


.druid_credentials <- function() {
  config::get(config = "druid_api")
}


.druid_update_device_list <- function(logstring) {
  devices <- ecotopia_devlist(logstring, verbose = FALSE)
  devices <- devices[, .(
    id,
    uuid,
    sn,
    device_number = mark,
    mac,
    device_type
  )]

  connection <- dbcon(db = "DRUID", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  existing <- DBI::dbGetQuery(
    connection,
    "SELECT id FROM device_list"
  ) |>
    as.data.table()

  new_devices <- devices[!existing, on = "id"]

  if (nrow(new_devices) > 0) {
    DBI::dbAppendTable(
      connection,
      "device_list",
      new_devices
    )
  }

  invisible(nrow(new_devices))
}


.druid_gps_field <- function(x, name) {
  if (name %in% names(x)) {
    x[[name]]
  } else {
    rep(NA, nrow(x))
  }
}


.druid_gps_sample_type <- function(x) {
  if (!is.list(x)) {
    return(as.integer(x))
  }

  vapply(
    x,
    function(value) {
      if (length(value) == 0 || all(is.na(value))) {
        return(as.integer(NA))
      }

      as.integer(sum(value, na.rm = TRUE))
    },
    integer(1)
  )
}


.druid_api_sql_time <- function(x) {
  x |>
    ymd_hms(tz = "UTC", quiet = TRUE) |>
    format(
      format = "%Y-%m-%d %H:%M:%OS6",
      tz = "UTC"
    )
}


.druid_prepare_gps <- function(gps) {
  gps <- as.data.table(gps)

  if (nrow(gps) == 0) {
    return(data.table())
  }

  field <- function(name) .druid_gps_field(gps, name)

  output <- data.table(
    id = field("device_id"),
    uuid = field("uuid"),
    updated_at = .druid_api_sql_time(field("updated_at")),
    timestamp = .druid_api_sql_time(field("timestamp")),
    longitude = field("longitude"),
    latitude = field("latitude"),
    altitude = field("altitude"),
    geoid_altitude = field("geoid_altitude"),
    relative_altitude = field("relative_altitude"),
    ground_altitude = field("ground_altitude"),
    speed = field("speed"),
    course = field("course"),
    used_star = field("used_star"),
    view_star = field("view_star"),
    fix_time = field("fix_time"),
    horizontal = field("horizontal"),
    vertical = field("vertical"),
    hdop = field("hdop"),
    vdop = field("vdop"),
    pdop = field("pdop"),
    quality = field("quality"),
    sample_type = .druid_gps_sample_type(field("sample_type"))
  )

  column_order <- c(names(output))
  output[, let(transmission_known = !is.na(updated_at))]

  data.table::setorderv(
    output,
    c("id", "timestamp", "transmission_known", "updated_at"),
    c(1, 1, -1, -1)
  )

  value_columns <- column_order[
    !column_order %in% c("id", "timestamp")
  ]

  output <- output[,
    lapply(.SD, function(value) {
      value[match(FALSE, is.na(value), nomatch = 1)]
    }),
    by = .(id, timestamp),
    .SDcols = value_columns
  ]

  data.table::setcolorder(output, column_order)
  output
}


.druid_gps_watermark_query <- function() {
  glue(
    "
    SELECT
      d.id,
      DATE_FORMAT(
        MAX(g.timestamp),
        '%Y-%m-%dT%H:%i:%s.%fZ'
      ) AS last_timestamp
    FROM device_list AS d
    LEFT JOIN GPS AS g ON g.id = d.id
    GROUP BY d.id
    ORDER BY
      MAX(g.timestamp) IS NOT NULL,
      MAX(g.timestamp)
    "
  ) |>
    as.character()
}


.druid_gps_watermarks <- function() {
  connection <- dbcon(db = "DRUID", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbGetQuery(
    connection,
    .druid_gps_watermark_query()
  ) |>
    as.data.table()
}


.druid_insert_gps <- function(gps) {
  if (nrow(gps) == 0) {
    return(0)
  }

  connection <- dbcon(db = "DRUID", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbWriteTable(
    connection,
    "druid_gps_stage",
    gps,
    temporary = TRUE,
    row.names = FALSE
  )

  statement <- glue(
    "
    INSERT INTO GPS (
      id,
      uuid,
      updated_at,
      timestamp,
      longitude,
      latitude,
      altitude,
      geoid_altitude,
      relative_altitude,
      ground_altitude,
      speed,
      course,
      used_star,
      view_star,
      fix_time,
      horizontal,
      vertical,
      hdop,
      vdop,
      pdop,
      quality,
      sample_type
    )
    SELECT
      id,
      uuid,
      updated_at,
      timestamp,
      longitude,
      latitude,
      altitude,
      geoid_altitude,
      relative_altitude,
      ground_altitude,
      speed,
      course,
      used_star,
      view_star,
      fix_time,
      horizontal,
      vertical,
      hdop,
      vdop,
      pdop,
      quality,
      sample_type
    FROM druid_gps_stage
    ON DUPLICATE KEY UPDATE
      id = VALUES(id)
    "
  ) |>
    as.character()

  DBI::dbExecute(connection, statement)
}


.druid_update_gps <- function(
  logstring,
  overlap = lubridate::days(2),
  initial_datetime = "2000-01-01T00:00:00Z",
  verbose = interactive()
) {
  devices <- .druid_gps_watermarks()
  total_devices <- nrow(devices)

  .druid_inform(
    verbose,
    glue("GPS: {total_devices} device(s) to update.")
  )

  results <- lapply(seq_len(total_devices), function(i) {
    device_id <- devices$id[i]
    last_timestamp <- devices$last_timestamp[i]

    from <- if (is.na(last_timestamp)) {
      initial_datetime
    } else {
      last_time <- ymd_hms(last_timestamp, tz = "UTC")

      format(
        last_time - overlap,
        format = "%Y-%m-%dT%H:%M:%SZ",
        tz = "UTC"
      )
    }

    .druid_inform(
      verbose,
      glue(
        "GPS [{i}/{total_devices}] {device_id}: ",
        "downloading from {from}."
      )
    )

    result <- tryCatch(
      {
        downloaded <- ecotopia_data(
          logstring,
          device_id,
          datetime = from,
          what = "gps",
          verbose = FALSE
        )
        gps <- .druid_prepare_gps(downloaded)
        affected <- .druid_insert_gps(gps)

        data.table(
          id = device_id,
          from = from,
          downloaded = nrow(downloaded),
          affected = affected,
          success = TRUE,
          error = NA_character_
        )
      },
      error = function(e) {
        data.table(
          id = device_id,
          from = from,
          downloaded = NA,
          affected = NA,
          success = FALSE,
          error = conditionMessage(e)
        )
      }
    )

    if (result$success) {
      .druid_inform(
        verbose,
        glue(
          "GPS [{i}/{total_devices}] {device_id}: ",
          "{result$downloaded} downloaded, {result$affected} inserted."
        )
      )
    } else {
      .druid_inform(
        verbose,
        glue(
          "GPS [{i}/{total_devices}] {device_id}: ",
          "failed: {result$error}"
        )
      )
    }

    result
  })

  if (length(results) == 0) {
    return(data.table())
  }

  result <- rbindlist(results, use.names = TRUE, fill = TRUE)
  failed <- result[success == FALSE]

  if (nrow(failed) > 0) {
    warning(
      glue(
        "GPS update failed for {nrow(failed)} device(s): ",
        "{toString(failed$id)}"
      ),
      call. = FALSE
    )
  }

  result
}
