kineis_rate_limit <- function() {
  structure(
    list(
      message = "HTTP 429 Too Many Requests.",
      call = NULL
    ),
    class = c(
      "httr2_http_429",
      "httr2_http",
      "httr2_error",
      "error",
      "condition"
    )
  )
}


test_that("Kineis credentials default the public API endpoints", {
  credentials <- .kineis_credentials(list(
    un = "username",
    pwd = "password"
  ))

  expect_equal(
    credentials$auth_url,
    paste0(
      "https://account.groupcls.com/auth/realms/cls/",
      "protocol/openid-connect/token"
    )
  )
  expect_equal(
    credentials$api_telemetry_url,
    "https://api.groupcls.com/telemetry/api/v1"
  )
})


test_that("Kineis bulk update requires the resumable API interface", {
  local_mocked_bindings(
    kineis_data = function(token, api_telemetry_url) {
      NULL
    },
    kineis_data_count = function(...) 0
  )

  expect_error(
    .kineis_require_bulk_api(),
    "requires apis >= 0.0.6",
    fixed = TRUE
  )
})


test_that("Kineis token provider caches and renews login tokens", {
  calls <- new.env(parent = emptyenv())
  calls$logins <- 0L

  local_mocked_bindings(
    kineis_login = function(un, pwd, auth_url, verbose) {
      calls$logins <- calls$logins + 1L
      expect_equal(un, "username")
      expect_equal(pwd, "password")
      expect_equal(auth_url, "https://auth.example")
      expect_false(verbose)
      list(
        access_token = paste0("token-", calls$logins),
        expires_in = 3600,
        obtained_at = Sys.time()
      )
    }
  )

  provider <- .kineis_token_provider(list(
    un = "username",
    pwd = "password",
    auth_url = "https://auth.example"
  ))

  expect_equal(provider()$access_token, "token-1")
  expect_equal(provider()$access_token, "token-1")
  expect_equal(provider(force = TRUE)$access_token, "token-2")
  expect_equal(calls$logins, 2L)
})


test_that("Kineis credentials fail before HTTP when secrets are missing", {
  expect_error(
    .kineis_credentials(list()),
    "missing non-empty fields: un, pwd",
    fixed = TRUE
  )
})


test_that("Kineis sensor preparation supports flattened API fields", {
  telemetry <- data.table(
    deviceUid = c("123", "123"),
    msgDatetime = c(
      "2026-07-01T00:00:00.123Z",
      "2026-07-01T00:01:00.456Z"
    ),
    `sensors.BATTERY VOLTS` = c("7.6", NA_character_),
    sensors.TEMPERATURE = c("5.2", "5.1")
  )

  result <- .kineis_prepare_sensors(telemetry)

  expect_named(
    result,
    c("deviceUid", "msgDatetime", "sensor", "value")
  )
  expect_equal(result$deviceUid, rep("123", 3))
  expect_equal(
    result$sensor,
    c("BATTERY VOLTS", "TEMPERATURE", "TEMPERATURE")
  )
  expect_equal(result$value, c("7.6", "5.2", "5.1"))
  expect_equal(
    result$msgDatetime,
    c(
      "2026-07-01 00:00:00.123000",
      "2026-07-01 00:00:00.123000",
      "2026-07-01 00:01:00.456000"
    )
  )
})


test_that("Kineis sensor preparation supports legacy JSON fragments", {
  telemetry <- data.table(
    deviceUid = "123",
    msgDatetime = "2026-07-01T00:00:00.123Z",
    sensors = '"SENSOR1":"7.6","SENSOR32":"5.2"'
  )

  result <- .kineis_prepare_sensors(telemetry)

  expect_equal(result$sensor, c("SENSOR1", "SENSOR32"))
  expect_equal(result$value, c("7.6", "5.2"))
})


test_that("Kineis sensor preparation supports live API list columns", {
  telemetry <- data.table(
    deviceUid = c("123", "123"),
    msgDatetime = c(
      "2026-07-01T00:00:00.123Z",
      "2026-07-01T00:01:00.456Z"
    ),
    sensors = list(
      list(`BATTERY VOLTS` = "7.6", TEMPERATURE = "5.2"),
      list(TEMPERATURE = "5.1")
    )
  )

  result <- .kineis_prepare_sensors(telemetry)

  expect_equal(
    result$sensor,
    c("BATTERY VOLTS", "TEMPERATURE", "TEMPERATURE")
  )
  expect_equal(result$value, c("7.6", "5.2", "5.1"))
})


test_that("Kineis Doppler preparation follows the MariaDB schema", {
  telemetry <- data.table(
    deviceUid = "123",
    deviceRef = "device-a",
    msgDatetime = "2026-07-01T00:00:00.123Z",
    acqDatetime = "2026-07-01T00:02:00.123Z",
    dopplerDatetime = "2026-07-01T00:03:00.123Z",
    dopplerLocLon = 8.3,
    dopplerLocLat = 52.5,
    dopplerLocAlt = 100,
    dopplerLocErrorRadius = 1200,
    dopplerLocClass = "A"
  )

  result <- .kineis_prepare_doppler(telemetry)

  expect_named(
    result,
    c(
      "deviceUid",
      "deviceRef",
      "msgDatetime",
      "acqDatetime",
      "dopplerDatetime",
      "dopplerLocLon",
      "dopplerLocLat",
      "dopplerLocAlt",
      "dopplerLocErrorRadius",
      "dopplerLocClass"
    )
  )
  expect_equal(result$deviceUid, "123")
  expect_equal(result$msgDatetime, "2026-07-01 00:00:00.123000")
  expect_equal(result$dopplerLocLon, 8.3)
})


test_that("Kineis sensor insertion stages data and ignores stored keys", {
  calls <- new.env(parent = emptyenv())
  calls$disconnected <- FALSE

  local_mocked_bindings(
    dbcon = function(db, server) {
      expect_equal(db, "KINEIS")
      expect_equal(server, "scidb")
      structure(list(), class = "test_connection")
    }
  )
  local_mocked_bindings(
    dbWriteTable = function(
      connection,
      name,
      value,
      temporary,
      row.names
    ) {
      calls$stage_name <- name
      calls$stage <- value
      calls$temporary <- temporary
      calls$row_names <- row.names
      TRUE
    },
    dbExecute = function(connection, statement) {
      calls$statement <- statement
      1
    },
    dbDisconnect = function(connection) {
      calls$disconnected <- TRUE
      TRUE
    },
    .package = "DBI"
  )

  sensors <- data.table(
    deviceUid = "123",
    msgDatetime = "2026-07-01 00:00:00.123000",
    sensor = "TEMPERATURE",
    value = "7.6"
  )

  expect_equal(.kineis_insert_sensors(sensors), 1)
  expect_equal(calls$stage_name, "kineis_sensors_stage")
  expect_equal(calls$stage, sensors)
  expect_true(calls$temporary)
  expect_false(calls$row_names)
  expect_match(
    calls$statement,
    "ON DUPLICATE KEY UPDATE\\s+deviceUid = VALUES\\(deviceUid\\)"
  )
  expect_true(calls$disconnected)
})


test_that("Kineis Doppler insertion stages data and ignores stored keys", {
  calls <- new.env(parent = emptyenv())
  connection <- structure(list(), class = "test_connection")

  local_mocked_bindings(
    dbWriteTable = function(
      connection,
      name,
      value,
      temporary,
      row.names
    ) {
      calls$stage_name <- name
      calls$stage <- value
      calls$temporary <- temporary
      calls$row_names <- row.names
      TRUE
    },
    dbExecute = function(connection, statement) {
      calls$statement <- statement
      1
    },
    .package = "DBI"
  )

  doppler <- data.table(
    deviceUid = "123",
    deviceRef = "device-a",
    msgDatetime = "2026-07-01 00:00:00.123000",
    acqDatetime = "2026-07-01 00:02:00.123000",
    dopplerDatetime = "2026-07-01 00:03:00.123000",
    dopplerLocLon = 8.3,
    dopplerLocLat = 52.5,
    dopplerLocAlt = 100,
    dopplerLocErrorRadius = 1200,
    dopplerLocClass = "A"
  )

  expect_equal(
    .kineis_insert_doppler(doppler, connection = connection),
    1
  )
  expect_equal(calls$stage_name, "kineis_doppler_stage")
  expect_equal(calls$stage, doppler)
  expect_true(calls$temporary)
  expect_false(calls$row_names)
  expect_match(calls$statement, "INSERT INTO doppler")
  expect_match(
    calls$statement,
    "ON DUPLICATE KEY UPDATE\\s+deviceUid = VALUES\\(deviceUid\\)"
  )
})


test_that("Kineis bulk page persistence writes both outputs and its cursor", {
  calls <- new.env(parent = emptyenv())
  connection <- structure(list(), class = "test_connection")

  local_mocked_bindings(
    dbcon = function(db, server) connection,
    .kineis_ensure_bulk_progress = function(connection) TRUE,
    .kineis_insert_sensors = function(sensors, connection) {
      calls$sensors <- sensors
      nrow(sensors)
    },
    .kineis_insert_doppler = function(doppler, connection) {
      calls$doppler <- doppler
      nrow(doppler)
    },
    .kineis_set_bulk_progress = function(
      window_start,
      window_end,
      after_cursor = NULL,
      message_count = NULL,
      connection
    ) {
      calls$progress <- list(
        window_start = window_start,
        window_end = window_end,
        after_cursor = after_cursor,
        message_count = message_count,
        connection = connection
      )
      1
    }
  )
  local_mocked_bindings(
    dbWithTransaction = function(connection, code, ...) force(code),
    dbDisconnect = function(connection) TRUE,
    .package = "DBI"
  )

  page <- data.table(
    deviceUid = c("123", "123"),
    deviceRef = c("device-a", "device-a"),
    msgDatetime = c(
      "2026-07-01T00:00:00.123Z",
      "2026-07-01T00:01:00.456Z"
    ),
    acqDatetime = c(
      "2026-07-01T00:02:00.123Z",
      "2026-07-01T00:03:00.456Z"
    ),
    sensors.TEMPERATURE = c("5.2", "5.1"),
    dopplerDatetime = c("2026-07-01T00:03:00.123Z", NA_character_),
    dopplerLocLon = c(8.3, NA_real_),
    dopplerLocLat = c(52.5, NA_real_),
    dopplerLocAlt = c(100, NA_real_),
    dopplerLocErrorRadius = c(1200, NA_real_),
    dopplerLocClass = c("A", NA_character_)
  )

  result <- .kineis_persist_bulk_page(
    page,
    window_start = "2026-07-01T00:00:00.000Z",
    window_end = "2026-07-02T00:00:00.000Z",
    page_info = list(hasNextPage = TRUE, endCursor = "99"),
    message_count = 200
  )

  expect_equal(result$sensor_rows, 2L)
  expect_equal(result$sensor_affected, 2L)
  expect_equal(result$doppler_rows, 1L)
  expect_equal(result$doppler_affected, 1L)
  expect_equal(nrow(calls$sensors), 2L)
  expect_equal(nrow(calls$doppler), 1L)
  expect_equal(
    calls$progress$window_start,
    "2026-07-01T00:00:00.000Z"
  )
  expect_equal(
    calls$progress$window_end,
    "2026-07-02T00:00:00.000Z"
  )
  expect_equal(calls$progress$after_cursor, "99")
  expect_equal(calls$progress$message_count, 200)
  expect_false(result$complete)
  expect_identical(calls$progress$connection, connection)
})


test_that("Kineis bulk progress stores the active window and cursor", {
  calls <- new.env(parent = emptyenv())
  connection <- structure(list(), class = "test_connection")

  local_mocked_bindings(
    dbExecute = function(connection, statement, params) {
      calls$statement <- statement
      calls$params <- params
      1
    },
    .package = "DBI"
  )

  expect_equal(
    .kineis_set_bulk_progress(
      "2026-07-01T00:00:00.000Z",
      "2026-07-02T00:00:00.000Z",
      after_cursor = "99",
      message_count = 200,
      connection = connection
    ),
    1
  )
  expect_match(calls$statement, "INSERT INTO bulk_progress")
  expect_match(calls$statement, "afterCursor = VALUES")
  expect_equal(
    calls$params,
    list(
      "2026-07-01 00:00:00.000000",
      "2026-07-02 00:00:00.000000",
      "99",
      200
    )
  )
})


test_that("Kineis bulk telemetry resumes one account-wide window", {
  calls <- new.env(parent = emptyenv())
  calls$progress <- list()

  local_mocked_bindings(
    .kineis_bulk_progress = function(initial_datetime) {
      list(
        window_start = "2026-07-01T00:00:00.000Z",
        window_end = "2026-07-02T00:00:00.000Z",
        after_cursor = "99",
        message_count = 150
      )
    },
    kineis_data_count = function(...) {
      stop("bulk count must not repeat after a saved cursor")
    },
    kineis_data = function(
      token,
      api_telemetry_url,
      datetime,
      end_datetime,
      device_refs,
      retrieve_metadata,
      retrieve_raw_data,
      retrieve_doppler,
      retrieve_gps_loc,
      retrieve_sensors,
      retrieve_additional_properties,
      verbose,
      page_handler,
      collect,
      after_cursor
    ) {
      calls$download <- as.list(environment())
      page_handler(
        data.table(
          deviceUid = "1",
          deviceRef = "device-a",
          msgDatetime = "2026-07-01T12:00:00.000Z"
        ),
        list(hasNextPage = FALSE, endCursor = "149")
      )
      data.table()
    },
    .kineis_persist_bulk_page = function(...) {
      list(
        sensor_rows = 2L,
        sensor_affected = 2L,
        doppler_rows = 1L,
        doppler_affected = 1L
      )
    },
    .kineis_set_bulk_progress = function(...) {
      calls$progress[[length(calls$progress) + 1]] <- list(...)
      1
    }
  )

  result <- .kineis_update_bulk(
    token = "token",
    api_telemetry_url = "https://api.example",
    target_datetime = "2026-07-02T00:00:00.000Z",
    initial_datetime = "2000-01-01T00:00:00.000Z",
    max_window_days = 365,
    min_window_hours = 24,
    target_messages = 1000,
    verbose = FALSE
  )

  expect_length(calls$download$device_refs, 0)
  expect_equal(calls$download$after_cursor, "99")
  expect_true(calls$download$retrieve_sensors)
  expect_true(calls$download$retrieve_doppler)
  expect_false(calls$download$collect)
  expect_true(result$success)
  expect_equal(result$downloaded, 1L)
  expect_equal(result$sensor_affected, 2L)
  expect_equal(result$doppler_affected, 1L)
  expect_equal(
    calls$progress[[1]][[1]],
    "2026-07-02T00:00:00.000Z"
  )
})


test_that("Kineis bulk telemetry defers with its exact page cursor saved", {
  calls <- new.env(parent = emptyenv())
  calls$persisted <- 0L

  local_mocked_bindings(
    .kineis_bulk_progress = function(initial_datetime) {
      list(
        window_start = "2026-07-01T00:00:00.000Z",
        window_end = "2026-07-02T00:00:00.000Z",
        after_cursor = "99",
        message_count = 200
      )
    },
    kineis_data = function(...) {
      arguments <- list(...)
      expect_equal(arguments$after_cursor, "99")
      arguments$page_handler(
        data.table(
          deviceUid = "1",
          deviceRef = "device-a",
          msgDatetime = "2026-07-01T12:00:00.000Z"
        ),
        list(hasNextPage = TRUE, endCursor = "199")
      )
      stop(kineis_rate_limit())
    },
    .kineis_persist_bulk_page = function(...) {
      calls$persisted <- calls$persisted + 1L
      list(
        sensor_rows = 2L,
        sensor_affected = 2L,
        doppler_rows = 1L,
        doppler_affected = 1L
      )
    }
  )

  result <- .kineis_update_bulk(
    token = "token",
    api_telemetry_url = "https://api.example",
    target_datetime = "2026-07-02T00:00:00.000Z",
    initial_datetime = "2000-01-01T00:00:00.000Z",
    max_window_days = 365,
    min_window_hours = 24,
    target_messages = 1000,
    verbose = FALSE
  )

  expect_equal(calls$persisted, 1L)
  expect_false(result$success)
  expect_true(result$deferred)
  expect_equal(result$downloaded, 1L)
  expect_equal(result$sensor_affected, 2L)
  expect_equal(result$doppler_affected, 1L)
  expect_match(result$error, "HTTP 429")
})


test_that("Kineis bulk windows shrink toward the configured minimum", {
  expect_equal(
    .kineis_shrunk_window_end(
      "2026-07-01T00:00:00.000Z",
      "2026-07-05T00:00:00.000Z",
      min_window_hours = 24
    ),
    "2026-07-03T00:00:00.000Z"
  )
  expect_null(.kineis_shrunk_window_end(
    "2026-07-01T00:00:00.000Z",
    "2026-07-02T00:00:00.000Z",
    min_window_hours = 24
  ))
})


test_that("KINEIS bulk update returns a deferred window summary", {
  local_mocked_bindings(
    .kineis_require_bulk_api = function() TRUE,
    .kineis_credentials = function() {
      list(
        un = "username",
        pwd = "password",
        auth_url = "https://auth.example",
        api_telemetry_url = "https://api.example"
      )
    },
    kineis_login = function(un, pwd, auth_url, verbose) {
      list(access_token = "token")
    },
    .kineis_current_datetime = function() {
      "2026-07-25T00:00:00.000Z"
    },
    .kineis_update_bulk = function(...) {
      data.table(
        window_start = "2026-07-01T00:00:00.000Z",
        success = FALSE,
        deferred = TRUE,
        error = "HTTP 429 Too Many Requests."
      )
    }
  )

  expect_message(
    result <- withVisible(KINEIS_update_bulk(verbose = FALSE)),
    "update deferred",
    fixed = TRUE
  )

  expect_false(result$visible)
  expect_equal(result$value$status, "deferred")
  expect_true(result$value$deferred)
  expect_equal(result$value$deferred_stage, "bulk telemetry")
  expect_equal(result$value$error, "HTTP 429 Too Many Requests.")
  expect_true(result$value$windows$deferred)
})


test_that("KINEIS bulk update runs one account-wide bulk pass", {
  calls <- new.env(parent = emptyenv())
  calls$updates <- 0L

  local_mocked_bindings(
    .kineis_require_bulk_api = function() TRUE,
    .kineis_credentials = function() {
      list(
        un = "username",
        pwd = "password",
        auth_url = "https://auth.example",
        api_telemetry_url = "https://api.example"
      )
    },
    kineis_login = function(un, pwd, auth_url, verbose) {
      expect_equal(un, "username")
      expect_equal(pwd, "password")
      expect_equal(auth_url, "https://auth.example")
      expect_false(verbose)
      list(access_token = "token")
    },
    .kineis_current_datetime = function() {
      "2026-07-25T00:00:00.000Z"
    },
    .kineis_update_bulk = function(
      token,
      api_telemetry_url,
      target_datetime,
      initial_datetime,
      max_window_days,
      min_window_hours,
      target_messages,
      verbose
    ) {
      calls$updates <- calls$updates + 1L
      expect_true(is.function(token))
      expect_equal(api_telemetry_url, "https://api.example")
      expect_equal(target_datetime, "2026-07-25T00:00:00.000Z")
      expect_false(verbose)
      data.table(success = TRUE, deferred = FALSE)
    }
  )

  result <- withVisible(KINEIS_update_bulk(verbose = FALSE))

  expect_false(result$visible)
  expect_equal(calls$updates, 1L)
  expect_equal(result$value$status, "complete")
  expect_false(result$value$deferred)
  expect_true(is.na(result$value$deferred_stage))
  expect_true(result$value$windows$success)
})
