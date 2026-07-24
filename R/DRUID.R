#' Run the DRUID database update
#'
#' Logs in to Ecotopia using the `druid_api` configuration, updates the device
#' list, and downloads new and recently transmitted GPS, ODBA, ENV, and raw ML
#' behaviour records. Each device and data type is updated independently.
#'
#' Devices without stored rows are downloaded from 2000-01-01. Existing devices
#' are resumed two days before their latest stored collection time, and rows
#' whose `(id, timestamp)` key is already present are left unchanged.
#' Structured ML behaviour payloads are stored as raw base64 and are not passed
#' through `ecotopia_postprocess_structured()`.
#'
#' @param verbose Show stage and per-device progress. Defaults to
#'   [interactive()].
#'
#' @return Invisibly, a list containing the number of devices added and GPS,
#'   ODBA, ENV, and behaviour update summaries by device.
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
  odba <- .druid_update_odba(logstring, verbose = verbose)
  env <- .druid_update_env(logstring, verbose = verbose)
  behaviour <- .druid_update_behaviour(logstring, verbose = verbose)
  .druid_inform(verbose, "DRUID: update complete.")

  invisible(list(
    devices_added = devices_added,
    gps = gps,
    odba = odba,
    env = env,
    behaviour = behaviour
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


.druid_field <- function(x, name) {
  if (name %in% names(x)) {
    x[[name]]
  } else {
    rep(NA, nrow(x))
  }
}


.druid_sample_type <- function(x) {
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


.druid_consolidate_records <- function(output) {
  column_order <- c(names(output))
  output[, let(
    transmission_known = !is.na(output[["updated_at"]])
  )]

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
    by = c("id", "timestamp"),
    .SDcols = value_columns
  ]

  data.table::setcolorder(output, column_order)
  output
}


.druid_prepare_gps <- function(gps) {
  gps <- as.data.table(gps)

  if (nrow(gps) == 0) {
    return(data.table())
  }

  field <- function(name) .druid_field(gps, name)

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
    sample_type = .druid_sample_type(field("sample_type"))
  )

  .druid_consolidate_records(output)
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
  failed <- result[result[["success"]] == FALSE]

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


.druid_prepare_odba <- function(odba) {
  odba <- as.data.table(odba)

  if (nrow(odba) == 0) {
    return(data.table())
  }

  field <- function(name) .druid_field(odba, name)

  output <- data.table(
    id = field("device_id"),
    uuid = field("uuid"),
    updated_at = .druid_api_sql_time(field("updated_at")),
    timestamp = .druid_api_sql_time(field("timestamp")),
    odba = field("odba"),
    odba_x = field("odba_x"),
    odba_y = field("odba_y"),
    odba_z = field("odba_z"),
    meandl_x = field("meandl_x"),
    meandl_y = field("meandl_y"),
    meandl_z = field("meandl_z"),
    sample_type = .druid_sample_type(field("sample_type"))
  )

  .druid_consolidate_records(output)
}


.druid_odba_watermark_query <- function() {
  glue(
    "
    SELECT
      d.id,
      DATE_FORMAT(
        MAX(o.timestamp),
        '%Y-%m-%dT%H:%i:%s.%fZ'
      ) AS last_timestamp
    FROM device_list AS d
    LEFT JOIN ODBA AS o ON o.id = d.id
    GROUP BY d.id
    ORDER BY
      MAX(o.timestamp) IS NOT NULL,
      MAX(o.timestamp)
    "
  ) |>
    as.character()
}


.druid_odba_watermarks <- function() {
  connection <- dbcon(db = "DRUID", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbGetQuery(
    connection,
    .druid_odba_watermark_query()
  ) |>
    as.data.table()
}


.druid_insert_odba <- function(odba) {
  if (nrow(odba) == 0) {
    return(0)
  }

  connection <- dbcon(db = "DRUID", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbWriteTable(
    connection,
    "druid_odba_stage",
    odba,
    temporary = TRUE,
    row.names = FALSE
  )

  statement <- glue(
    "
    INSERT INTO ODBA (
      id,
      uuid,
      updated_at,
      timestamp,
      odba,
      odba_x,
      odba_y,
      odba_z,
      meandl_x,
      meandl_y,
      meandl_z,
      sample_type
    )
    SELECT
      id,
      uuid,
      updated_at,
      timestamp,
      odba,
      odba_x,
      odba_y,
      odba_z,
      meandl_x,
      meandl_y,
      meandl_z,
      sample_type
    FROM druid_odba_stage
    ON DUPLICATE KEY UPDATE
      id = VALUES(id)
    "
  ) |>
    as.character()

  DBI::dbExecute(connection, statement)
}


.druid_update_odba <- function(
  logstring,
  overlap = lubridate::days(2),
  initial_datetime = "2000-01-01T00:00:00Z",
  verbose = interactive()
) {
  devices <- .druid_odba_watermarks()
  total_devices <- nrow(devices)

  .druid_inform(
    verbose,
    glue("ODBA: {total_devices} device(s) to update.")
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
        "ODBA [{i}/{total_devices}] {device_id}: ",
        "downloading from {from}."
      )
    )

    result <- tryCatch(
      {
        downloaded <- ecotopia_data(
          logstring,
          device_id,
          datetime = from,
          what = "odba",
          verbose = FALSE
        )
        odba <- .druid_prepare_odba(downloaded)
        affected <- .druid_insert_odba(odba)

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
          "ODBA [{i}/{total_devices}] {device_id}: ",
          "{result$downloaded} downloaded, {result$affected} inserted."
        )
      )
    } else {
      .druid_inform(
        verbose,
        glue(
          "ODBA [{i}/{total_devices}] {device_id}: ",
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
        "ODBA update failed for {nrow(failed)} device(s): ",
        "{toString(failed$id)}"
      ),
      call. = FALSE
    )
  }

  result
}


.druid_prepare_env <- function(environment) {
  environment <- as.data.table(environment)

  if (nrow(environment) == 0) {
    return(data.table())
  }

  field <- function(name) .druid_field(environment, name)

  output <- data.table(
    id = field("device_id"),
    uuid = field("uuid"),
    updated_at = .druid_api_sql_time(field("updated_at")),
    timestamp = .druid_api_sql_time(field("timestamp")),
    inner_temperature = field("inner_temperature"),
    inner_humidity = field("inner_humidity"),
    ambient_light = field("ambient_light"),
    inner_light = field("inner_light"),
    inner_pressure = field("inner_pressure"),
    battery_power = field("battery_power"),
    battery_voltage = field("battery_voltage"),
    charge_voltage = field("charge_voltage"),
    charge_current = field("charge_current"),
    sample_type = .druid_sample_type(field("sample_type"))
  )

  .druid_consolidate_records(output)
}


.druid_env_watermark_query <- function() {
  glue(
    "
    SELECT
      d.id,
      DATE_FORMAT(
        MAX(e.timestamp),
        '%Y-%m-%dT%H:%i:%s.%fZ'
      ) AS last_timestamp
    FROM device_list AS d
    LEFT JOIN ENV AS e ON e.id = d.id
    GROUP BY d.id
    ORDER BY
      MAX(e.timestamp) IS NOT NULL,
      MAX(e.timestamp)
    "
  ) |>
    as.character()
}


.druid_env_watermarks <- function() {
  connection <- dbcon(db = "DRUID", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbGetQuery(
    connection,
    .druid_env_watermark_query()
  ) |>
    as.data.table()
}


.druid_insert_env <- function(environment) {
  if (nrow(environment) == 0) {
    return(0)
  }

  connection <- dbcon(db = "DRUID", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbWriteTable(
    connection,
    "druid_env_stage",
    environment,
    temporary = TRUE,
    row.names = FALSE
  )

  statement <- glue(
    "
    INSERT INTO ENV (
      id,
      uuid,
      updated_at,
      timestamp,
      inner_temperature,
      inner_humidity,
      ambient_light,
      inner_light,
      inner_pressure,
      battery_power,
      battery_voltage,
      charge_voltage,
      charge_current,
      sample_type
    )
    SELECT
      id,
      uuid,
      updated_at,
      timestamp,
      inner_temperature,
      inner_humidity,
      ambient_light,
      inner_light,
      inner_pressure,
      battery_power,
      battery_voltage,
      charge_voltage,
      charge_current,
      sample_type
    FROM druid_env_stage
    ON DUPLICATE KEY UPDATE
      id = VALUES(id)
    "
  ) |>
    as.character()

  DBI::dbExecute(connection, statement)
}


.druid_update_env <- function(
  logstring,
  overlap = lubridate::days(2),
  initial_datetime = "2000-01-01T00:00:00Z",
  verbose = interactive()
) {
  devices <- .druid_env_watermarks()
  total_devices <- nrow(devices)

  .druid_inform(
    verbose,
    glue("ENV: {total_devices} device(s) to update.")
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
        "ENV [{i}/{total_devices}] {device_id}: ",
        "downloading from {from}."
      )
    )

    result <- tryCatch(
      {
        downloaded <- ecotopia_data(
          logstring,
          device_id,
          datetime = from,
          what = "env",
          verbose = FALSE
        )
        environment <- .druid_prepare_env(downloaded)
        affected <- .druid_insert_env(environment)

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
          "ENV [{i}/{total_devices}] {device_id}: ",
          "{result$downloaded} downloaded, {result$affected} inserted."
        )
      )
    } else {
      .druid_inform(
        verbose,
        glue(
          "ENV [{i}/{total_devices}] {device_id}: ",
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
        "ENV update failed for {nrow(failed)} device(s): ",
        "{toString(failed$id)}"
      ),
      call. = FALSE
    )
  }

  result
}


.druid_prepare_behaviour <- function(behaviour) {
  behaviour <- as.data.table(behaviour)

  if (nrow(behaviour) == 0) {
    return(data.table())
  }

  field <- function(name) .druid_field(behaviour, name)

  output <- data.table(
    id = field("device_id"),
    uuid = field("uuid"),
    updated_at = .druid_api_sql_time(field("updated_at")),
    timestamp = .druid_api_sql_time(field("timestamp")),
    version = field("version"),
    type = field("type"),
    behaviour = field("data")
  )

  .druid_consolidate_records(output)
}


.druid_behaviour_watermark_query <- function() {
  glue(
    "
    SELECT
      d.id,
      DATE_FORMAT(
        MAX(b.timestamp),
        '%Y-%m-%dT%H:%i:%s.%fZ'
      ) AS last_timestamp
    FROM device_list AS d
    LEFT JOIN behaviour AS b ON b.id = d.id
    GROUP BY d.id
    ORDER BY
      MAX(b.timestamp) IS NOT NULL,
      MAX(b.timestamp)
    "
  ) |>
    as.character()
}


.druid_behaviour_watermarks <- function() {
  connection <- dbcon(db = "DRUID", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbGetQuery(
    connection,
    .druid_behaviour_watermark_query()
  ) |>
    as.data.table()
}


.druid_insert_behaviour <- function(behaviour) {
  if (nrow(behaviour) == 0) {
    return(0)
  }

  connection <- dbcon(db = "DRUID", server = "scidb")
  on.exit(DBI::dbDisconnect(connection))

  DBI::dbWriteTable(
    connection,
    "druid_behaviour_stage",
    behaviour,
    temporary = TRUE,
    row.names = FALSE
  )

  statement <- glue(
    "
    INSERT INTO behaviour (
      id,
      uuid,
      updated_at,
      timestamp,
      version,
      type,
      behaviour
    )
    SELECT
      id,
      uuid,
      updated_at,
      timestamp,
      version,
      type,
      behaviour
    FROM druid_behaviour_stage
    ON DUPLICATE KEY UPDATE
      id = VALUES(id)
    "
  ) |>
    as.character()

  DBI::dbExecute(connection, statement)
}


.druid_update_behaviour <- function(
  logstring,
  overlap = lubridate::days(2),
  initial_datetime = "2000-01-01T00:00:00Z",
  verbose = interactive()
) {
  devices <- .druid_behaviour_watermarks()
  total_devices <- nrow(devices)

  .druid_inform(
    verbose,
    glue("BEHAVIOUR: {total_devices} device(s) to update.")
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
        "BEHAVIOUR [{i}/{total_devices}] {device_id}: ",
        "downloading from {from}."
      )
    )

    result <- tryCatch(
      {
        downloaded <- ecotopia_data(
          logstring,
          device_id,
          datetime = from,
          what = "structured",
          verbose = FALSE
        )
        behaviour <- .druid_prepare_behaviour(downloaded)
        affected <- .druid_insert_behaviour(behaviour)

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
          "BEHAVIOUR [{i}/{total_devices}] {device_id}: ",
          "{result$downloaded} downloaded, {result$affected} inserted."
        )
      )
    } else {
      .druid_inform(
        verbose,
        glue(
          "BEHAVIOUR [{i}/{total_devices}] {device_id}: ",
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
        "BEHAVIOUR update failed for {nrow(failed)} device(s): ",
        "{toString(failed$id)}"
      ),
      call. = FALSE
    )
  }

  result
}
