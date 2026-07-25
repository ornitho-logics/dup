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
    "requires apis >= 0.0.4",
    fixed = TRUE
  )
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
    sensors.SENSOR1 = c("7.6", NA_character_),
    sensors.SENSOR2 = c("5.2", "5.1")
  )

  result <- .kineis_prepare_sensors(telemetry)

  expect_named(
    result,
    c("deviceUid", "msgDatetime", "sensor", "value")
  )
  expect_equal(result$deviceUid, rep("123", 3))
  expect_equal(result$sensor, c(1L, 2L, 2L))
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

  expect_equal(result$sensor, c(1L, 32L))
  expect_equal(result$value, c("7.6", "5.2"))
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
    sensor = 1L,
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


test_that("Kineis sensor updates overlap existing data", {
  calls <- new.env(parent = emptyenv())
  calls$downloads <- list()

  local_mocked_bindings(
    .kineis_watermarks = function(devices, table) {
      expect_equal(table, "sensors")
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
      page <- data.table(
        deviceUid = if (device_refs == "new-device") "1" else "2",
        msgDatetime = "2026-07-20T00:00:00Z",
        sensors.SENSOR1 = "7.6"
      )
      page_handler(page)
      data.table()
    },
    .kineis_insert_sensors = function(sensors) nrow(sensors)
  )

  result <- .kineis_update_sensors(
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
  expect_false(calls$downloads[["new-device"]]$retrieve_doppler)
  expect_false(calls$downloads[["new-device"]]$verbose)
  expect_false(calls$downloads[["new-device"]]$collect)
  expect_true(all(result$success))
  expect_equal(result$affected, c(1, 1))
})


test_that("Kineis update stops after an exhausted rate limit", {
  calls <- new.env(parent = emptyenv())
  calls$downloads <- 0L
  calls$inserted <- 0L
  rate_limit <- structure(
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

  local_mocked_bindings(
    .kineis_watermarks = function(devices, table) {
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
        msgDatetime = "2026-07-20T00:00:00Z",
        sensors.SENSOR1 = "7.6"
      ))
      stop(rate_limit)
    },
    .kineis_insert_sensors = function(sensors) {
      calls$inserted <- calls$inserted + nrow(sensors)
      nrow(sensors)
    }
  )

  expect_error(
    .kineis_update_sensors(
      token = "token",
      api_telemetry_url = "https://api.example",
      devices = data.table(),
      end_datetime = "2026-07-25T00:00:00.000Z",
      verbose = FALSE
    ),
    "stopping this update to avoid repeated HTTP 429",
    fixed = TRUE
  )
  expect_equal(calls$downloads, 1L)
  expect_equal(calls$inserted, 1L)
})


test_that("KINEIS update runs both data layers in order", {
  calls <- new.env(parent = emptyenv())
  calls$order <- character()

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
      expect_equal(token$access_token, "token")
      expect_equal(api_telemetry_url, "https://api.example")
      expect_false(verbose)
      data.table(deviceUid = "1", deviceRef = "device-a")
    },
    .kineis_current_datetime = function() {
      "2026-07-25T00:00:00.000Z"
    },
    .kineis_update_sensors = function(
      token,
      api_telemetry_url,
      devices,
      end_datetime,
      verbose
    ) {
      calls$order <- c(calls$order, "sensors")
      expect_false(verbose)
      expect_equal(end_datetime, "2026-07-25T00:00:00.000Z")
      data.table(deviceRef = "device-a", success = TRUE)
    },
    .kineis_update_doppler = function(
      token,
      api_telemetry_url,
      devices,
      end_datetime,
      verbose
    ) {
      calls$order <- c(calls$order, "doppler")
      expect_false(verbose)
      expect_equal(end_datetime, "2026-07-25T00:00:00.000Z")
      data.table(deviceRef = "device-a", success = TRUE)
    }
  )

  result <- withVisible(KINEIS_update(verbose = FALSE))

  expect_false(result$visible)
  expect_equal(calls$order, c("sensors", "doppler"))
  expect_equal(result$value$devices, 1)
  expect_true(result$value$sensors$success)
  expect_true(result$value$doppler$success)
})
