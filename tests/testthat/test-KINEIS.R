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


test_that("Kineis update requires the streaming API interface", {
  local_mocked_bindings(
    kineis_data = function(token, api_telemetry_url) {
      NULL
    }
  )

  expect_error(
    .kineis_require_streaming_api(),
    "requires apis >= 0.0.5",
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


test_that("Kineis page persistence writes both outputs and its checkpoint", {
  calls <- new.env(parent = emptyenv())
  connection <- structure(list(), class = "test_connection")

  local_mocked_bindings(
    dbcon = function(db, server) connection,
    .kineis_insert_sensors = function(sensors, connection) {
      calls$sensors <- sensors
      nrow(sensors)
    },
    .kineis_insert_doppler = function(doppler, connection) {
      calls$doppler <- doppler
      nrow(doppler)
    },
    .kineis_set_progress = function(
      device_uid,
      device_ref,
      timestamp,
      connection
    ) {
      calls$progress <- list(
        device_uid = device_uid,
        device_ref = device_ref,
        timestamp = timestamp,
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

  result <- .kineis_persist_page(page, "123", "device-a")

  expect_equal(result$sensor_rows, 2L)
  expect_equal(result$sensor_affected, 2L)
  expect_equal(result$doppler_rows, 1L)
  expect_equal(result$doppler_affected, 1L)
  expect_equal(nrow(calls$sensors), 2L)
  expect_equal(nrow(calls$doppler), 1L)
  expect_equal(calls$progress$device_uid, "123")
  expect_equal(calls$progress$device_ref, "device-a")
  expect_equal(
    calls$progress$timestamp,
    "2026-07-01 00:01:00.456000"
  )
  expect_identical(calls$progress$connection, connection)
})


test_that("Kineis progress update is monotonic", {
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
    .kineis_set_progress(
      "123",
      "device-a",
      "2026-07-01T00:00:00.123Z",
      connection = connection
    ),
    1
  )
  expect_match(calls$statement, "INSERT INTO telemetry_progress")
  expect_match(calls$statement, "GREATEST")
  expect_equal(
    calls$params,
    list("123", "device-a", "2026-07-01 00:00:00.123000")
  )
})


test_that("Kineis telemetry updates both outputs in one API pass", {
  calls <- new.env(parent = emptyenv())
  calls$downloads <- list()
  calls$completed <- list()

  local_mocked_bindings(
    .kineis_watermarks = function(devices) {
      data.table(
        deviceUid = c("1", "2"),
        deviceRef = c("new-device", "existing-device"),
        last_timestamp = c(
          NA_character_,
          "2026-07-20T00:00:00.000000Z"
        )
      )
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
      collect
    ) {
      calls$downloads[[device_refs]] <- as.list(environment())
      page_handler(data.table(
        deviceUid = if (device_refs == "new-device") "1" else "2",
        deviceRef = device_refs,
        msgDatetime = "2026-07-20T00:00:00Z"
      ))
      data.table()
    },
    .kineis_persist_page = function(
      telemetry,
      device_uid,
      device_ref
    ) {
      list(
        sensor_rows = 1L,
        sensor_affected = 1L,
        doppler_rows = 1L,
        doppler_affected = 1L
      )
    },
    .kineis_set_progress = function(
      device_uid,
      device_ref,
      timestamp,
      connection = NULL
    ) {
      calls$completed[[device_ref]] <- timestamp
      1
    }
  )

  result <- .kineis_update_telemetry(
    token = "token",
    api_telemetry_url = "https://api.example",
    devices = data.table(),
    end_datetime = "2026-07-25T00:00:00.000Z",
    verbose = FALSE
  )

  expect_equal(
    calls$downloads[["new-device"]]$datetime,
    "2000-01-01T00:00:00.000Z"
  )
  expect_equal(
    calls$downloads[["existing-device"]]$datetime,
    "2026-07-18T00:00:00.000Z"
  )
  expect_true(calls$downloads[["new-device"]]$retrieve_sensors)
  expect_true(calls$downloads[["new-device"]]$retrieve_doppler)
  expect_false(calls$downloads[["new-device"]]$verbose)
  expect_false(calls$downloads[["new-device"]]$collect)
  expect_equal(
    calls$completed[["new-device"]],
    "2026-07-25T00:00:00.000Z"
  )
  expect_true(all(result$success))
  expect_equal(result$downloaded, c(1L, 1L))
  expect_equal(result$sensor_affected, c(1L, 1L))
  expect_equal(result$doppler_affected, c(1L, 1L))
})


test_that("Kineis telemetry defers after an exhausted rate limit", {
  calls <- new.env(parent = emptyenv())
  calls$downloads <- 0L
  calls$persisted <- 0L

  local_mocked_bindings(
    .kineis_watermarks = function(devices) {
      data.table(
        deviceUid = c("1", "2"),
        deviceRef = c("device-a", "device-b"),
        last_timestamp = c(NA_character_, NA_character_)
      )
    },
    kineis_data = function(...) {
      calls$downloads <- calls$downloads + 1L
      arguments <- list(...)
      arguments$page_handler(data.table(
        deviceUid = "1",
        deviceRef = "device-a",
        msgDatetime = "2026-07-20T00:00:00Z"
      ))
      stop(kineis_rate_limit())
    },
    .kineis_persist_page = function(...) {
      calls$persisted <- calls$persisted + 1L
      list(
        sensor_rows = 2L,
        sensor_affected = 2L,
        doppler_rows = 1L,
        doppler_affected = 1L
      )
    }
  )

  result <- .kineis_update_telemetry(
    token = "token",
    api_telemetry_url = "https://api.example",
    devices = data.table(),
    end_datetime = "2026-07-25T00:00:00.000Z",
    verbose = FALSE
  )

  expect_equal(calls$downloads, 1L)
  expect_equal(calls$persisted, 1L)
  expect_equal(nrow(result), 1L)
  expect_false(result$success)
  expect_true(result$deferred)
  expect_equal(result$downloaded, 1L)
  expect_equal(result$sensor_affected, 2L)
  expect_equal(result$doppler_affected, 1L)
  expect_match(result$error, "HTTP 429")
})


test_that("KINEIS update returns a deferred telemetry summary", {
  local_mocked_bindings(
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
    kineis_devlist = function(token, api_telemetry_url, verbose) {
      data.table(deviceUid = "1", deviceRef = "device-a")
    },
    .kineis_current_datetime = function() {
      "2026-07-25T00:00:00.000Z"
    },
    .kineis_update_telemetry = function(...) {
      data.table(
        deviceRef = "device-a",
        success = FALSE,
        deferred = TRUE,
        error = "HTTP 429 Too Many Requests."
      )
    }
  )

  expect_message(
    result <- withVisible(KINEIS_update(verbose = FALSE)),
    "update deferred",
    fixed = TRUE
  )

  expect_false(result$visible)
  expect_equal(result$value$status, "deferred")
  expect_true(result$value$deferred)
  expect_equal(result$value$deferred_stage, "telemetry")
  expect_equal(result$value$devices, 1L)
  expect_equal(result$value$error, "HTTP 429 Too Many Requests.")
  expect_true(result$value$telemetry$deferred)
})


test_that("KINEIS update defers a rate-limited device list", {
  calls <- new.env(parent = emptyenv())
  calls$telemetry <- 0L

  local_mocked_bindings(
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
    kineis_devlist = function(token, api_telemetry_url, verbose) {
      stop(kineis_rate_limit())
    },
    .kineis_update_telemetry = function(...) {
      calls$telemetry <- calls$telemetry + 1L
    }
  )

  expect_message(
    result <- withVisible(KINEIS_update(verbose = FALSE)),
    "during device list",
    fixed = TRUE
  )

  expect_false(result$visible)
  expect_equal(calls$telemetry, 0L)
  expect_equal(result$value$status, "deferred")
  expect_true(result$value$deferred)
  expect_equal(result$value$deferred_stage, "device list")
  expect_true(is.na(result$value$devices))
  expect_match(result$value$error, "HTTP 429")
})


test_that("KINEIS update runs one combined telemetry pass", {
  calls <- new.env(parent = emptyenv())
  calls$updates <- 0L

  local_mocked_bindings(
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
    kineis_devlist = function(token, api_telemetry_url, verbose) {
      expect_true(is.function(token))
      expect_equal(token()$access_token, "token")
      expect_equal(api_telemetry_url, "https://api.example")
      expect_false(verbose)
      data.table(deviceUid = "1", deviceRef = "device-a")
    },
    .kineis_current_datetime = function() {
      "2026-07-25T00:00:00.000Z"
    },
    .kineis_update_telemetry = function(
      token,
      api_telemetry_url,
      devices,
      end_datetime,
      verbose
    ) {
      calls$updates <- calls$updates + 1L
      expect_false(verbose)
      expect_equal(end_datetime, "2026-07-25T00:00:00.000Z")
      data.table(
        deviceRef = "device-a",
        success = TRUE,
        deferred = FALSE
      )
    }
  )

  result <- withVisible(KINEIS_update(verbose = FALSE))

  expect_false(result$visible)
  expect_equal(calls$updates, 1L)
  expect_equal(result$value$status, "complete")
  expect_false(result$value$deferred)
  expect_true(is.na(result$value$deferred_stage))
  expect_equal(result$value$devices, 1)
  expect_true(result$value$telemetry$success)
})
