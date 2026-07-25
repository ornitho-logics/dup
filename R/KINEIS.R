#' Run the Kineis database update
#'
#' Logs in using the `kineis_api` configuration, retrieves the devices
#' available to that profile, and updates the MariaDB `sensors` and `doppler`
#' tables. Each device and data layer is updated independently.
#'
#' Devices without stored rows are downloaded from 2000-01-01. Existing devices
#' are resumed two days before their latest stored message time, and rows whose
#' database key is already present are left unchanged.
#' Each chronologically ordered API page is written immediately, so an
#' interrupted initial backfill resumes from the latest persisted page.
#' Authentication tokens are renewed before expiry and after an unexpected
#' HTTP 401 response. If HTTP 429 rate limiting is encountered (after automatic
#' retries where configured), the update returns normally with a deferred
#' status. The next scheduled run resumes from the latest persisted page.
#'
#' The public Kinéis authentication and telemetry endpoints are used when
#' `auth_url` or `api_telemetry_url` is absent from the configuration. The
#' `kineis_api` configuration must provide non-empty `un` and `pwd` values.
#'
#' @param verbose Show stage and per-device progress. Defaults to
#'   [interactive()].
#'
#' @return Invisibly, a list containing `status`, `deferred`,
#'   `deferred_stage`, the device count, and sensor and Doppler update summaries
#'   by device. `status` is `"complete"` or `"deferred"`.
#' @export
KINEIS_update <- function(verbose = interactive()) {
  .kineis_require_streaming_api()
  .kineis_inform(verbose, "KINEIS: authenticating.")
  credentials <- .kineis_credentials()

  token <- .kineis_token_provider(credentials)
  authentication <- tryCatch(
    {
      token()
      NULL
    },
    httr2_http_429 = identity
  )

  if (inherits(authentication, "httr2_http_429")) {
    return(invisible(.kineis_deferred_update(
      stage = "authentication",
      error = conditionMessage(authentication)
    )))
  }

  .kineis_inform(verbose, "KINEIS: retrieving the device list.")
  devices <- tryCatch(
    kineis_devlist(
      token,
      api_telemetry_url = credentials$api_telemetry_url,
      verbose = FALSE
    ) |>
      .kineis_prepare_devices(),
    httr2_http_429 = identity
  )

  if (inherits(devices, "httr2_http_429")) {
    return(invisible(.kineis_deferred_update(
      stage = "device list",
      error = conditionMessage(devices)
    )))
  }

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

  if (.kineis_was_deferred(sensors)) {
    return(invisible(.kineis_deferred_update(
      stage = "sensors",
      devices = nrow(devices),
      sensors = sensors,
      error = .kineis_deferred_error(sensors)
    )))
  }

  doppler <- .kineis_update_doppler(
    token,
    api_telemetry_url = credentials$api_telemetry_url,
    devices = devices,
    end_datetime = end_datetime,
    verbose = verbose
  )

  if (.kineis_was_deferred(doppler)) {
    return(invisible(.kineis_deferred_update(
      stage = "doppler",
      devices = nrow(devices),
      sensors = sensors,
      doppler = doppler,
      error = .kineis_deferred_error(doppler)
    )))
  }

  .kineis_inform(verbose, "KINEIS: update complete.")

  invisible(list(
    status = "complete",
    deferred = FALSE,
    deferred_stage = NA_character_,
    devices = nrow(devices),
    sensors = sensors,
    doppler = doppler,
    error = NA_character_
  ))
}


.kineis_was_deferred <- function(x) {
  is.data.frame(x) &&
    "deferred" %in% names(x) &&
    any(x[["deferred"]] %in% TRUE)
}


.kineis_deferred_error <- function(x) {
  x[x[["deferred"]] %in% TRUE][["error"]][1]
}


.kineis_deferred_update <- function(
  stage,
  devices = NA_integer_,
  sensors = data.table(),
  doppler = data.table(),
  error = NA_character_
) {
  message(
    glue(
      "KINEIS: API rate limiting is active during {stage}; update deferred. ",
      "The next scheduled run will resume ",
      "from the latest persisted page."
    )
  )

  list(
    status = "deferred",
    deferred = TRUE,
    deferred_stage = stage,
    devices = devices,
    sensors = sensors,
    doppler = doppler,
    error = error
  )
}


.kineis_require_streaming_api <- function() {
  required_arguments <- c("page_handler", "collect")
  available_arguments <- names(formals(kineis_data))
  missing_arguments <- setdiff(
    required_arguments,
    available_arguments
  )

  if (length(missing_arguments) > 0) {
    stop(
      paste(
        "KINEIS_update() requires apis >= 0.0.5.",
        "Restart R to unload the older apis namespace, then try again."
      ),
      call. = FALSE
    )
  }

  invisible(TRUE)
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


.kineis_token_expired <- function(token, safety_seconds = 60) {
  if (is.null(token)) {
    return(TRUE)
  }

  if (
    !is.list(token) ||
      !.kineis_nonempty_string(token$access_token)
  ) {
    return(TRUE)
  }

  expires_in <- suppressWarnings(as.numeric(token$expires_in))
  obtained_at <- suppressWarnings(as.POSIXct(
    token$obtained_at,
    tz = "UTC"
  ))

  if (
    length(expires_in) != 1 ||
      is.na(expires_in) ||
      !is.finite(expires_in) ||
      length(obtained_at) != 1 ||
      is.na(obtained_at)
  ) {
    return(FALSE)
  }

  refresh_after <- max(expires_in - safety_seconds, 0)
  Sys.time() >= obtained_at + refresh_after
}


.kineis_token_provider <- function(credentials) {
  token <- NULL

  function(force = FALSE) {
    if (!isTRUE(force) && !.kineis_token_expired(token)) {
      return(token)
    }

    token <<- kineis_login(
      un = credentials$un,
      pwd = credentials$pwd,
      auth_url = credentials$auth_url,
      verbose = FALSE
    )
    token
  }
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
  output[, sensor := trimws(as.character(sensor_name))]
  invalid <- output[
    is.na(sensor) | !nzchar(sensor),
    unique(sensor_name)
  ]

  if (length(invalid) > 0) {
    stop("Kineis sensor payload contains empty names.", call. = FALSE)
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

  results <- vector("list", total_devices)
  result_count <- 0L

  for (i in seq_len(total_devices)) {
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

    downloaded_count <- 0L
    affected_count <- 0L
    persist_page <- function(downloaded) {
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

      downloaded_count <<- downloaded_count + nrow(downloaded)
      affected_count <<- affected_count + affected
      .kineis_inform(
        verbose,
        glue(
          "{label} [{i}/{total_devices}] {device_ref}: ",
          "{downloaded_count} downloaded, ",
          "{affected_count} inserted so far."
        )
      )

      invisible(NULL)
    }

    result <- tryCatch(
      {
        kineis_data(
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
          verbose = FALSE,
          page_handler = persist_page,
          collect = FALSE
        )

        data.table(
          deviceUid = device_uid,
          deviceRef = device_ref,
          from = from,
          to = end_datetime,
          downloaded = downloaded_count,
          affected = affected_count,
          success = TRUE,
          deferred = FALSE,
          error = NA_character_
        )
      },
      error = function(e) {
        if (inherits(e, "httr2_http_401")) {
          stop(
            paste(
              "Kineis authentication failed after automatic token",
              "renewal; stopping this update."
            ),
            call. = FALSE
          )
        }

        if (inherits(e, "httr2_http_429")) {
          return(data.table(
            deviceUid = device_uid,
            deviceRef = device_ref,
            from = from,
            to = end_datetime,
            downloaded = downloaded_count,
            affected = affected_count,
            success = FALSE,
            deferred = TRUE,
            error = conditionMessage(e)
          ))
        }

        data.table(
          deviceUid = device_uid,
          deviceRef = device_ref,
          from = from,
          to = end_datetime,
          downloaded = NA_integer_,
          affected = NA_integer_,
          success = FALSE,
          deferred = FALSE,
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
    } else if (result$deferred) {
      .kineis_inform(
        verbose,
        glue(
          "{label} [{i}/{total_devices}] {device_ref}: ",
          "rate limited after {result$downloaded} downloaded and ",
          "{result$affected} inserted; deferring remaining requests."
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

    result_count <- result_count + 1L
    results[[result_count]] <- result

    if (result$deferred) {
      break
    }
  }

  if (result_count == 0L) {
    return(data.table())
  }

  results <- results[seq_len(result_count)]
  result <- rbindlist(results, use.names = TRUE, fill = TRUE)
  failed <- result[
    result[["success"]] == FALSE &
      result[["deferred"]] == FALSE
  ]

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
