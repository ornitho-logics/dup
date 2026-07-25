#' Run the Kineis database update
#'
#' Logs in using the `kineis_api` configuration, retrieves the devices
#' available to that profile, and updates the MariaDB `sensors` and `doppler`
#' tables. Each device and data layer is updated independently.
#'
#' Devices without stored rows are downloaded from 2000-01-01. Existing devices
#' are resumed two days before their latest stored message time, and rows whose
#' database key is already present are left unchanged.
#'
#' The public Kinéis authentication and telemetry endpoints are used when
#' `auth_url` or `api_telemetry_url` is absent from the configuration. The
#' `kineis_api` configuration must provide non-empty `un` and `pwd` values.
#'
#' @param verbose Show stage and per-device progress. Defaults to
#'   [interactive()].
#'
#' @return Invisibly, a list containing the device count and sensor and Doppler
#'   update summaries by device.
#' @export
KINEIS_update <- function(verbose = interactive()) {
  .kineis_inform(verbose, "KINEIS: authenticating.")
  credentials <- .kineis_credentials()

  token <- kineis_login(
    un = credentials$un,
    pwd = credentials$pwd,
    auth_url = credentials$auth_url,
    verbose = FALSE
  )

  .kineis_inform(verbose, "KINEIS: retrieving the device list.")
  devices <- kineis_devlist(
    token,
    api_telemetry_url = credentials$api_telemetry_url,
    verbose = FALSE
  ) |>
    .kineis_prepare_devices()
  .kineis_inform(
    verbose,
    glue("KINEIS: device list complete; {nrow(devices)} device(s).")
  )

  end_datetime <- .kineis_current_datetime()
  sensors <- .kineis_update_sensors(
    token,
    api_telemetry_url = credentials$api_telemetry_url,
    devices = devices,
    end_datetime = end_datetime,
    verbose = verbose
  )
  doppler <- .kineis_update_doppler(
    token,
    api_telemetry_url = credentials$api_telemetry_url,
    devices = devices,
    end_datetime = end_datetime,
    verbose = verbose
  )
  .kineis_inform(verbose, "KINEIS: update complete.")

  invisible(list(
    devices = nrow(devices),
    sensors = sensors,
    doppler = doppler
  ))
}

.kineis_inform <- function(verbose, text) {
  if (verbose) {
    message(text)
  }

  invisible(NULL)
}

.kineis_credentials <- function(
  credentials = config::get(config = "kineis_api")
) {
  if (!is.list(credentials)) {
    stop(
      "The `kineis_api` configuration must be a list.",
      call. = FALSE
    )
  }

  defaults <- list(
    auth_url = paste0(
      "https://account.groupcls.com/auth/realms/cls/",
      "protocol/openid-connect/token"
    ),
    api_telemetry_url = "https://api.groupcls.com/telemetry/api/v1"
  )

  for (field in names(defaults)) {
    if (!.kineis_nonempty_string(credentials[[field]])) {
      credentials[[field]] <- defaults[[field]]
    }
  }

  required <- c("un", "pwd", "auth_url", "api_telemetry_url")
  missing_fields <- required[
    !vapply(
      credentials[required],
      .kineis_nonempty_string,
      logical(1)
    )
  ]

  if (length(missing_fields) > 0) {
    stop(
      glue(
        "The `kineis_api` configuration is missing non-empty fields: ",
        "{toString(missing_fields)}"
      ),
      call. = FALSE
    )
  }

  credentials
}


.kineis_nonempty_string <- function(x) {
  is.character(x) &&
    length(x) == 1 &&
    !is.na(x) &&
    nzchar(x)
}

.kineis_current_datetime <- function() {
  Sys.time() |>
    lubridate::with_tz("UTC") |>
    format(
      format = "%Y-%m-%dT%H:%M:%OS3Z",
      tz = "UTC"
    )
}

.kineis_prepare_devices <- function(devices) {
  devices <- data.table::copy(as.data.table(devices))
  required <- c("deviceUid", "deviceRef")
  missing_columns <- setdiff(required, names(devices))

  if (length(missing_columns) > 0) {
    stop(
      glue(
        "Kineis device list is missing columns: ",
        "{toString(missing_columns)}"
      ),
      call. = FALSE
    )
  }

  devices <- devices[, .(
    deviceUid = as.character(deviceUid),
    deviceRef = as.character(deviceRef)
  )]
  devices <- devices[
    !is.na(deviceUid) &
      nzchar(deviceUid) &
      !is.na(deviceRef) &
      nzchar(deviceRef)
  ]

  unique(devices, by = c("deviceUid", "deviceRef"))
}

.kineis_field <- function(x, name) {
  if (name %in% names(x)) {
    x[[name]]
  } else {
    rep(NA, nrow(x))
  }
}

.kineis_api_sql_time <- function(x) {
  time <- ymd_hms(x, tz = "UTC", quiet = TRUE)
  format(
    time + 0.0000005,
    format = "%Y-%m-%d %H:%M:%OS6",
    tz = "UTC"
  )
}

.kineis_from_datetime <- function(
  last_timestamp,
  overlap,
  initial_datetime
) {
  if (
    length(last_timestamp) == 0 ||
      is.na(last_timestamp) ||
      !nzchar(last_timestamp)
  ) {
    return(initial_datetime)
  }

  last_time <- ymd_hms(last_timestamp, tz = "UTC", quiet = TRUE)

  if (is.na(last_time)) {
    stop(
      glue("Invalid Kineis database timestamp: {last_timestamp}"),
      call. = FALSE
    )
  }

  format(
    last_time - overlap,
    format = "%Y-%m-%dT%H:%M:%OS3Z",
    tz = "UTC"
  )
}

.kineis_parse_sensors <- function(value) {
  if (is.null(value) || length(value) == 0) {
    return(list())
  }

  if (
    is.atomic(value) &&
      length(value) == 1 &&
      is.na(value)
  ) {
    return(list())
  }

  if (is.list(value) && !is.data.frame(value)) {
    return(value)
  }

  if (!is.character(value) || length(value) != 1) {
    stop("Unsupported Kineis sensor payload.", call. = FALSE)
  }

  value <- trimws(value)

  if (!nzchar(value)) {
    return(list())
  }

  if (!startsWith(value, "{")) {
    value <- paste0("{", value, "}")
  }

  tryCatch(
    jsonlite::fromJSON(value, simplifyVector = TRUE) |>
      as.list(),
    error = function(e) {
      stop(
        glue("Invalid Kineis sensor payload: {conditionMessage(e)}"),
        call. = FALSE
      )
    }
  )
}

.kineis_sensor_id <- function(x) {
  id <- stringr::str_extract(x, "[0-9]+$")
  suppressWarnings(as.integer(id))
}

.kineis_prepare_sensors <- function(telemetry) {
  telemetry <- data.table::copy(as.data.table(telemetry))

  if (nrow(telemetry) == 0) {
    return(data.table())
  }

  required <- c("deviceUid", "msgDatetime")
  missing_columns <- setdiff(required, names(telemetry))

  if (length(missing_columns) > 0) {
    stop(
      glue(
        "Kineis telemetry is missing columns: ",
        "{toString(missing_columns)}"
      ),
      call. = FALSE
    )
  }

  parts <- list()
  sensor_columns <- grep(
    "^sensors\\.",
    names(telemetry),
    value = TRUE
  )

  if (length(sensor_columns) > 0) {
    for (column in sensor_columns) {
      data.table::set(
        telemetry,
        j = column,
        value = as.character(telemetry[[column]])
      )
    }

    wide <- telemetry[,
      c(required, sensor_columns),
      with = FALSE
    ]
    flattened <- melt(
      wide,
      id.vars = required,
      measure.vars = sensor_columns,
      variable.name = "sensor_name",
      value.name = "value",
      variable.factor = FALSE,
      na.rm = TRUE
    )
    flattened[,
      sensor_name := sub("^sensors\\.", "", sensor_name)
    ]
    parts[[length(parts) + 1]] <- flattened
  }

  if ("sensors" %in% names(telemetry)) {
    legacy <- lapply(seq_len(nrow(telemetry)), function(i) {
      sensors <- .kineis_parse_sensors(telemetry[["sensors"]][[i]])

      if (length(sensors) == 0) {
        return(NULL)
      }

      sensor_names <- names(sensors)

      if (is.null(sensor_names) || any(!nzchar(sensor_names))) {
        stop(
          "Kineis sensor payload contains unnamed values.",
          call. = FALSE
        )
      }

      data.table(
        deviceUid = as.character(telemetry[["deviceUid"]][i]),
        msgDatetime = telemetry[["msgDatetime"]][i],
        sensor_name = sensor_names,
        value = as.character(unlist(sensors, use.names = FALSE))
      )
    })

    legacy <- Filter(Negate(is.null), legacy)

    if (length(legacy) > 0) {
      parts[[length(parts) + 1]] <- rbindlist(
        legacy,
        use.names = TRUE,
        fill = TRUE
      )
    }
  }

  if (length(parts) == 0) {
    return(data.table())
  }

  output <- rbindlist(parts, use.names = TRUE, fill = TRUE)
  output[, sensor := .kineis_sensor_id(sensor_name)]
  invalid <- output[is.na(sensor), unique(sensor_name)]

  if (length(invalid) > 0) {
    stop(
      glue(
        "Kineis sensor names do not end in numeric IDs: ",
        "{toString(invalid)}"
      ),
      call. = FALSE
    )
  }

  output[, let(
    deviceUid = as.character(deviceUid),
    msgDatetime = .kineis_api_sql_time(msgDatetime),
    value = as.character(value)
  )]
  output <- output[
    !is.na(deviceUid) &
      !is.na(msgDatetime) &
      !is.na(value),
    .(deviceUid, msgDatetime, sensor, value)
  ]

  unique(output, by = c("deviceUid", "msgDatetime", "sensor"))
}

.kineis_prepare_doppler <- function(telemetry) {
  telemetry <- data.table::copy(as.data.table(telemetry))

  if (nrow(telemetry) == 0) {
    return(data.table())
  }

  required <- c("deviceUid", "msgDatetime")
  missing_columns <- setdiff(required, names(telemetry))

  if (length(missing_columns) > 0) {
    stop(
      glue(
        "Kineis telemetry is missing columns: ",
        "{toString(missing_columns)}"
      ),
      call. = FALSE
    )
  }

  field <- function(name) .kineis_field(telemetry, name)
  output <- data.table(
    deviceUid = as.character(field("deviceUid")),
    deviceRef = as.character(field("deviceRef")),
    msgDatetime = .kineis_api_sql_time(field("msgDatetime")),
    acqDatetime = .kineis_api_sql_time(field("acqDatetime")),
    dopplerDatetime = .kineis_api_sql_time(field("dopplerDatetime")),
    dopplerLocLon = field("dopplerLocLon"),
    dopplerLocLat = field("dopplerLocLat"),
    dopplerLocAlt = field("dopplerLocAlt"),
    dopplerLocErrorRadius = field("dopplerLocErrorRadius"),
    dopplerLocClass = as.character(field("dopplerLocClass"))
  )
  output <- output[
    !is.na(deviceUid) &
      !is.na(msgDatetime) &
      !is.na(dopplerLocLon)
  ]

  unique(output, by = c("deviceUid", "msgDatetime"))
}

.kineis_watermark_query <- function(table) {
  table <- match.arg(table, c("sensors", "doppler"))

  glue(
    "
    SELECT
      deviceUid,
      DATE_FORMAT(
        MAX(msgDatetime),
        '%Y-%m-%dT%H:%i:%s.%fZ'
      ) AS last_timestamp
    FROM {`table`}
    GROUP BY deviceUid
    "
  ) |>
    as.character()
}

.kineis_watermarks <- function(devices, table) {
  devices <- data.table::copy(as.data.table(devices))
  connection <- dbcon(db = "KINEIS", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  stored <- DBI::dbGetQuery(
    connection,
    .kineis_watermark_query(table)
  ) |>
    as.data.table()

  devices[, last_timestamp := NA_character_]

  if (nrow(stored) > 0) {
    stored[, deviceUid := as.character(deviceUid)]
    devices[
      stored,
      on = "deviceUid",
      last_timestamp := i.last_timestamp
    ]
  }

  devices[, has_data := !is.na(last_timestamp)]
  data.table::setorderv(
    devices,
    c("has_data", "last_timestamp"),
    c(1, 1)
  )
  devices[, has_data := NULL]
  devices
}

.kineis_insert_sensors <- function(sensors) {
  if (nrow(sensors) == 0) {
    return(0)
  }

  connection <- dbcon(db = "KINEIS", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbWriteTable(
    connection,
    "kineis_sensors_stage",
    sensors,
    temporary = TRUE,
    row.names = FALSE
  )

  statement <- "
    INSERT INTO sensors (
      deviceUid,
      msgDatetime,
      sensor,
      value
    )
    SELECT
      deviceUid,
      msgDatetime,
      sensor,
      value
    FROM kineis_sensors_stage
    ON DUPLICATE KEY UPDATE
      deviceUid = VALUES(deviceUid)
  "

  DBI::dbExecute(connection, statement)
}

.kineis_insert_doppler <- function(doppler) {
  if (nrow(doppler) == 0) {
    return(0)
  }

  connection <- dbcon(db = "KINEIS", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbWriteTable(
    connection,
    "kineis_doppler_stage",
    doppler,
    temporary = TRUE,
    row.names = FALSE
  )

  statement <- "
    INSERT INTO doppler (
      deviceUid,
      deviceRef,
      msgDatetime,
      acqDatetime,
      dopplerDatetime,
      dopplerLocLon,
      dopplerLocLat,
      dopplerLocAlt,
      dopplerLocErrorRadius,
      dopplerLocClass
    )
    SELECT
      deviceUid,
      deviceRef,
      msgDatetime,
      acqDatetime,
      dopplerDatetime,
      dopplerLocLon,
      dopplerLocLat,
      dopplerLocAlt,
      dopplerLocErrorRadius,
      dopplerLocClass
    FROM kineis_doppler_stage
    ON DUPLICATE KEY UPDATE
      deviceUid = VALUES(deviceUid)
  "

  DBI::dbExecute(connection, statement)
}

.kineis_update_layer <- function(
  layer,
  token,
  api_telemetry_url,
  devices,
  end_datetime,
  overlap = lubridate::days(2),
  initial_datetime = "2000-01-01T00:00:00.000Z",
  verbose = interactive()
) {
  layer <- match.arg(layer, c("sensors", "doppler"))
  label <- toupper(layer)
  watermarks <- .kineis_watermarks(devices, layer)
  total_devices <- nrow(watermarks)

  .kineis_inform(
    verbose,
    glue("{label}: {total_devices} device(s) to update.")
  )

  results <- lapply(seq_len(total_devices), function(i) {
    device_uid <- watermarks$deviceUid[i]
    device_ref <- watermarks$deviceRef[i]
    from <- .kineis_from_datetime(
      watermarks$last_timestamp[i],
      overlap = overlap,
      initial_datetime = initial_datetime
    )

    .kineis_inform(
      verbose,
      glue(
        "{label} [{i}/{total_devices}] {device_ref}: ",
        "downloading from {from}."
      )
    )

    result <- tryCatch(
      {
        downloaded <- kineis_data(
          token,
          api_telemetry_url = api_telemetry_url,
          datetime = from,
          end_datetime = end_datetime,
          device_refs = device_ref,
          retrieve_metadata = FALSE,
          retrieve_raw_data = FALSE,
          retrieve_doppler = identical(layer, "doppler"),
          retrieve_gps_loc = FALSE,
          retrieve_sensors = identical(layer, "sensors"),
          retrieve_additional_properties = FALSE,
          verbose = FALSE
        )
        prepared <- if (identical(layer, "sensors")) {
          .kineis_prepare_sensors(downloaded)
        } else {
          .kineis_prepare_doppler(downloaded)
        }
        affected <- if (identical(layer, "sensors")) {
          .kineis_insert_sensors(prepared)
        } else {
          .kineis_insert_doppler(prepared)
        }

        data.table(
          deviceUid = device_uid,
          deviceRef = device_ref,
          from = from,
          to = end_datetime,
          downloaded = nrow(downloaded),
          affected = affected,
          success = TRUE,
          error = NA_character_
        )
      },
      error = function(e) {
        if (inherits(e, "httr2_http_429")) {
          stop(
            paste(
              "Kineis API rate limiting remained active after automatic",
              "retries; stopping this update to avoid repeated HTTP 429",
              "requests."
            ),
            call. = FALSE
          )
        }

        data.table(
          deviceUid = device_uid,
          deviceRef = device_ref,
          from = from,
          to = end_datetime,
          downloaded = NA_integer_,
          affected = NA_integer_,
          success = FALSE,
          error = conditionMessage(e)
        )
      }
    )

    if (result$success) {
      .kineis_inform(
        verbose,
        glue(
          "{label} [{i}/{total_devices}] {device_ref}: ",
          "{result$downloaded} downloaded, ",
          "{result$affected} inserted."
        )
      )
    } else {
      .kineis_inform(
        verbose,
        glue(
          "{label} [{i}/{total_devices}] {device_ref}: ",
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
  failed <- result[result[["success"]] == FALSE]

  if (nrow(failed) > 0) {
    warning(
      glue(
        "{label} update failed for {nrow(failed)} device(s): ",
        "{toString(failed$deviceRef)}"
      ),
      call. = FALSE
    )
  }

  result
}

.kineis_update_sensors <- function(
  token,
  api_telemetry_url,
  devices,
  end_datetime = .kineis_current_datetime(),
  overlap = lubridate::days(2),
  initial_datetime = "2000-01-01T00:00:00.000Z",
  verbose = interactive()
) {
  .kineis_update_layer(
    layer = "sensors",
    token = token,
    api_telemetry_url = api_telemetry_url,
    devices = devices,
    end_datetime = end_datetime,
    overlap = overlap,
    initial_datetime = initial_datetime,
    verbose = verbose
  )
}

.kineis_update_doppler <- function(
  token,
  api_telemetry_url,
  devices,
  end_datetime = .kineis_current_datetime(),
  overlap = lubridate::days(2),
  initial_datetime = "2000-01-01T00:00:00.000Z",
  verbose = interactive()
) {
  .kineis_update_layer(
    layer = "doppler",
    token = token,
    api_telemetry_url = api_telemetry_url,
    devices = devices,
    end_datetime = end_datetime,
    overlap = overlap,
    initial_datetime = initial_datetime,
    verbose = verbose
  )
}
