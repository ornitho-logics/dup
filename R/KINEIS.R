#' Update the Kineis database
#'
#' Retrieves sensors and Doppler locations together from the Kinéis bulk API
#' for every device available to the login profile. Time is processed in fixed
#' one-day windows. On a fresh database, retrieval begins at `start_date`.
#' Each page and its next pagination cursor are committed together, so a
#' deferred run resumes at the next page.
#'
#' Authentication tokens are renewed before expiry and after an unexpected
#' HTTP 401 response. If the API remains rate limited or temporarily
#' unavailable after automatic retries, the update returns normally with a
#' deferred status. The next daily run resumes the stored window and cursor.
#'
#' The public Kinéis authentication and telemetry endpoints are used when
#' `auth_url` or `api_telemetry_url` is absent from the configuration. The
#' `kineis_api` configuration must provide non-empty `un` and `pwd` values.
#'
#' @param start_date Initial retrieval date in UTC. It may be a `"YYYY-MM-DD"`
#'   string, an RFC 3339 datetime, a [base::Date], or a POSIX date-time. This
#'   is used only when `telemetry_progress` has no checkpoint. Defaults to
#'   `"2024-01-01"`.
#' @param verbose Show stage and window progress. Defaults to [interactive()].
#'
#' @return Invisibly, a list containing `status`, `deferred`,
#'   `deferred_stage`, and a window summary. `status` is `"complete"` or
#'   `"deferred"`.
#' @export
KINEIS_update <- function(
  start_date = "2024-01-01",
  verbose = interactive()
) {
  .kineis_require_api()
  start_datetime <- .kineis_rfc3339(start_date)
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

  target_datetime <- .kineis_current_datetime()
  windows <- .kineis_update_telemetry(
    token,
    api_telemetry_url = credentials$api_telemetry_url,
    start_datetime = start_datetime,
    target_datetime = target_datetime,
    verbose = verbose
  )

  if (.kineis_was_deferred(windows)) {
    return(invisible(.kineis_deferred_update(
      stage = "telemetry",
      windows = windows,
      error = .kineis_deferred_error(windows)
    )))
  }

  .kineis_inform(verbose, "KINEIS: update complete.")

  invisible(list(
    status = "complete",
    deferred = FALSE,
    deferred_stage = NA_character_,
    windows = windows,
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
  windows = data.table(),
  error = NA_character_
) {
  message(
    glue(
      "KINEIS: API access is temporarily unavailable during {stage}; ",
      "update deferred. The next run will resume the stored window and cursor."
    )
  )

  list(
    status = "deferred",
    deferred = TRUE,
    deferred_stage = stage,
    windows = windows,
    error = error
  )
}


.kineis_require_api <- function() {
  required_arguments <- c("page_handler", "collect", "after_cursor")
  available_arguments <- names(formals(kineis_data))
  missing_arguments <- setdiff(
    required_arguments,
    available_arguments
  )

  if (length(missing_arguments) > 0) {
    stop(
      paste(
        "KINEIS_update() requires apis >= 0.0.8.",
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


.kineis_rfc3339 <- function(x) {
  time <- .kineis_time(x)

  format(
    time,
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

.kineis_progress_query <- function() {
  "
  SELECT
    DATE_FORMAT(
      windowStart,
      '%Y-%m-%dT%H:%i:%s.%fZ'
    ) AS window_start,
    DATE_FORMAT(
      windowEnd,
      '%Y-%m-%dT%H:%i:%s.%fZ'
    ) AS window_end,
    afterCursor AS after_cursor
  FROM telemetry_progress
  WHERE id = 1
  "
}


.kineis_progress <- function(start_datetime) {
  connection <- dbcon(db = "KINEIS", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  start_datetime <- .kineis_api_sql_time(
    .kineis_rfc3339(start_datetime)
  )
  DBI::dbExecute(
    connection,
    "
    INSERT IGNORE INTO telemetry_progress (
      id,
      windowStart,
      windowEnd,
      afterCursor
    )
    VALUES (1, ?, ?, NULL)
    ",
    params = list(start_datetime, start_datetime)
  )

  stored <- DBI::dbGetQuery(
    connection,
    .kineis_progress_query()
  ) |>
    as.data.table()

  if (nrow(stored) != 1) {
    stop(
      "KINEIS.telemetry_progress must contain its id = 1 row.",
      call. = FALSE
    )
  }

  cursor <- stored$after_cursor[1]

  list(
    window_start = .kineis_rfc3339(stored$window_start[1]),
    window_end = .kineis_rfc3339(stored$window_end[1]),
    after_cursor = if (is.na(cursor) || !nzchar(cursor)) NULL else cursor
  )
}

.kineis_insert_sensors <- function(sensors, connection = NULL) {
  if (nrow(sensors) == 0) {
    return(0)
  }

  own_connection <- is.null(connection)

  if (own_connection) {
    connection <- dbcon(db = "KINEIS", server = "scidb")
    on.exit(DBI::dbDisconnect(connection))
  }

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
      value = VALUES(value)
  "

  DBI::dbExecute(connection, statement)
}

.kineis_insert_doppler <- function(doppler, connection = NULL) {
  if (nrow(doppler) == 0) {
    return(0)
  }

  own_connection <- is.null(connection)

  if (own_connection) {
    connection <- dbcon(db = "KINEIS", server = "scidb")
    on.exit(DBI::dbDisconnect(connection))
  }

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
      deviceRef = VALUES(deviceRef),
      acqDatetime = VALUES(acqDatetime),
      dopplerDatetime = VALUES(dopplerDatetime),
      dopplerLocLon = VALUES(dopplerLocLon),
      dopplerLocLat = VALUES(dopplerLocLat),
      dopplerLocAlt = VALUES(dopplerLocAlt),
      dopplerLocErrorRadius = VALUES(dopplerLocErrorRadius),
      dopplerLocClass = VALUES(dopplerLocClass)
  "

  DBI::dbExecute(connection, statement)
}

.kineis_set_progress <- function(
  window_start,
  window_end,
  after_cursor = NULL,
  connection = NULL
) {
  own_connection <- is.null(connection)

  if (own_connection) {
    connection <- dbcon(db = "KINEIS", server = "scidb")
    on.exit(DBI::dbDisconnect(connection))
  }

  statement <- "
    UPDATE telemetry_progress
    SET
      windowStart = ?,
      windowEnd = ?,
      afterCursor = ?
    WHERE id = 1
  "

  affected <- DBI::dbExecute(
    connection,
    statement,
    params = list(
      .kineis_api_sql_time(window_start),
      .kineis_api_sql_time(window_end),
      if (is.null(after_cursor)) NA_character_ else as.character(after_cursor)
    )
  )

  affected
}


.kineis_page_progress <- function(page_info) {
  if (is.null(page_info) || !isTRUE(page_info$hasNextPage)) {
    return(list(complete = TRUE, after_cursor = NULL))
  }

  cursor <- page_info$endCursor

  if (
    is.null(cursor) ||
      length(cursor) != 1 ||
      is.na(cursor) ||
      !nzchar(cursor)
  ) {
    stop(
      "Kineis page indicated another page but returned no cursor.",
      call. = FALSE
    )
  }

  list(complete = FALSE, after_cursor = as.character(cursor))
}


.kineis_persist_page <- function(
  telemetry,
  window_start,
  window_end,
  page_info
) {
  sensors <- .kineis_prepare_sensors(telemetry)
  doppler <- .kineis_prepare_doppler(telemetry)
  progress <- .kineis_page_progress(page_info)
  connection <- dbcon(db = "KINEIS", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  affected <- DBI::dbWithTransaction(connection, {
    sensor_affected <- .kineis_insert_sensors(
      sensors,
      connection = connection
    )
    doppler_affected <- .kineis_insert_doppler(
      doppler,
      connection = connection
    )

    if (progress$complete) {
      .kineis_set_progress(
        window_start = window_end,
        window_end = window_end,
        connection = connection
      )
    } else {
      .kineis_set_progress(
        window_start = window_start,
        window_end = window_end,
        after_cursor = progress$after_cursor,
        connection = connection
      )
    }

    list(
      sensor_rows = nrow(sensors),
      sensor_affected = sensor_affected,
      doppler_rows = nrow(doppler),
      doppler_affected = doppler_affected,
      after_cursor = progress$after_cursor,
      complete = progress$complete
    )
  })

  affected
}


.kineis_time <- function(x) {
  if (inherits(x, "Date")) {
    if (length(x) != 1 || is.na(x)) {
      stop("Invalid Kineis UTC datetime.", call. = FALSE)
    }

    return(as.POSIXct(x, tz = "UTC"))
  }

  if (inherits(x, "POSIXt")) {
    time <- lubridate::with_tz(x, "UTC")
  } else if (
    is.character(x) &&
      length(x) == 1 &&
      !is.na(x) &&
      grepl("^\\d{4}-\\d{2}-\\d{2}$", x)
  ) {
    time <- as.POSIXct(x, format = "%Y-%m-%d", tz = "UTC")
  } else {
    time <- ymd_hms(x, tz = "UTC", quiet = TRUE)
  }

  if (length(time) != 1 || is.na(time)) {
    stop("Invalid Kineis UTC datetime.", call. = FALSE)
  }

  lubridate::with_tz(time, "UTC")
}


.kineis_window_end <- function(start, target) {
  start <- .kineis_time(start)
  target <- .kineis_time(target)
  candidate <- start + lubridate::days(1)

  .kineis_rfc3339(min(candidate, target))
}


.kineis_is_temporary_api_error <- function(error) {
  any(vapply(
    c("httr2_http_429", "httr2_http_503", "httr2_http_504"),
    function(class) inherits(error, class),
    logical(1)
  ))
}


.kineis_result <- function(
  window_start,
  window_end,
  downloaded = 0L,
  sensor_rows = 0L,
  sensor_affected = 0L,
  doppler_rows = 0L,
  doppler_affected = 0L,
  success,
  deferred,
  error = NA_character_
) {
  data.table(
    window_start = window_start,
    window_end = window_end,
    downloaded = as.integer(downloaded),
    sensor_rows = as.integer(sensor_rows),
    sensor_affected = as.integer(sensor_affected),
    doppler_rows = as.integer(doppler_rows),
    doppler_affected = as.integer(doppler_affected),
    success = success,
    deferred = deferred,
    error = error
  )
}


.kineis_update_telemetry <- function(
  token,
  api_telemetry_url,
  start_datetime = "2024-01-01",
  target_datetime,
  verbose = interactive()
) {
  start_datetime <- .kineis_rfc3339(start_datetime)
  state <- .kineis_progress(start_datetime)
  target_datetime <- .kineis_rfc3339(target_datetime)
  results <- list()

  if (.kineis_time(state$window_start) > .kineis_time(target_datetime)) {
    stop(
      "Kineis checkpoint is later than the update target.",
      call. = FALSE
    )
  }

  while (.kineis_time(state$window_start) < .kineis_time(target_datetime)) {
    if (
      .kineis_time(state$window_start) >=
        .kineis_time(state$window_end)
    ) {
      state$window_end <- .kineis_window_end(
        state$window_start,
        target_datetime
      )
      state$after_cursor <- NULL
      .kineis_set_progress(
        state$window_start,
        state$window_end
      )
    }

    .kineis_inform(
      verbose,
      glue(
        "KINEIS [{state$window_start} \u2192 {state$window_end}]: ",
        if (is.null(state$after_cursor)) {
          "downloading."
        } else {
          glue("resuming after cursor {state$after_cursor}.")
        }
      )
    )

    downloaded_count <- 0L
    sensor_rows <- 0L
    sensor_affected <- 0L
    doppler_rows <- 0L
    doppler_affected <- 0L

    persist_page <- function(downloaded, page_info) {
      persisted <- .kineis_persist_page(
        downloaded,
        window_start = state$window_start,
        window_end = state$window_end,
        page_info = page_info
      )
      downloaded_count <<- downloaded_count + nrow(downloaded)
      sensor_rows <<- sensor_rows + persisted$sensor_rows
      sensor_affected <<- sensor_affected + persisted$sensor_affected
      doppler_rows <<- doppler_rows + persisted$doppler_rows
      doppler_affected <<- doppler_affected + persisted$doppler_affected

      .kineis_inform(
        verbose,
        glue(
          "KINEIS [{state$window_start} \u2192 {state$window_end}]: ",
          "{downloaded_count} downloaded, ",
          "{sensor_rows} sensor and {doppler_rows} Doppler rows prepared."
        )
      )

      invisible(NULL)
    }

    retrieval <- tryCatch(
      {
        kineis_data(
          token,
          api_telemetry_url = api_telemetry_url,
          datetime = state$window_start,
          end_datetime = state$window_end,
          device_refs = character(),
          retrieve_metadata = FALSE,
          retrieve_raw_data = FALSE,
          retrieve_doppler = TRUE,
          retrieve_gps_loc = FALSE,
          retrieve_sensors = TRUE,
          retrieve_additional_properties = FALSE,
          verbose = FALSE,
          page_handler = persist_page,
          collect = FALSE,
          after_cursor = state$after_cursor
        )

        .kineis_set_progress(
          state$window_end,
          state$window_end
        )

        .kineis_result(
          state$window_start,
          state$window_end,
          downloaded = downloaded_count,
          sensor_rows = sensor_rows,
          sensor_affected = sensor_affected,
          doppler_rows = doppler_rows,
          doppler_affected = doppler_affected,
          success = TRUE,
          deferred = FALSE
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

        if (.kineis_is_temporary_api_error(e)) {
          return(.kineis_result(
            state$window_start,
            state$window_end,
            downloaded = downloaded_count,
            sensor_rows = sensor_rows,
            sensor_affected = sensor_affected,
            doppler_rows = doppler_rows,
            doppler_affected = doppler_affected,
            success = FALSE,
            deferred = TRUE,
            error = conditionMessage(e)
          ))
        }

        stop(e)
      }
    )

    results[[length(results) + 1]] <- retrieval

    if (retrieval$deferred) {
      .kineis_inform(
        verbose,
        glue(
          "KINEIS [{state$window_start} \u2192 {state$window_end}]: ",
          "temporarily unavailable after {downloaded_count} downloaded; ",
          "deferring remaining pages."
        )
      )
      break
    }

    .kineis_inform(
      verbose,
      glue(
        "KINEIS [{state$window_start} \u2192 {state$window_end}]: ",
        "{downloaded_count} downloaded, ",
        "{sensor_affected} sensor and {doppler_affected} Doppler rows ",
        "affected; window complete."
      )
    )
    state$window_start <- state$window_end
    state$after_cursor <- NULL
  }

  if (length(results) == 0) {
    return(data.table())
  }

  rbindlist(results, use.names = TRUE, fill = TRUE)
}
