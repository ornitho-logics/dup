test_that("GM requests raw deployed and undeployed GPS events", {
  call <- new.env(parent = emptyenv())
  handle <- structure(list(), class = "test_handle")
  start <- ymd_hms("2026-07-20T00:00:00Z", tz = "UTC")
  end <- ymd_hms("2026-07-21T00:00:00Z", tz = "UTC")

  local_mocked_bindings(
    .gm_retrieve = function(...) {
      call$arguments <- list(...)
      data.frame(event_id = 1)
    }
  )

  result <- .gm_download(
    handle,
    study_id = "8532542312",
    timestamp_start = start,
    timestamp_end = end
  )

  expect_equal(nrow(result), 1)
  expect_equal(call$arguments$entity_type, "event")
  expect_equal(call$arguments$sensor_type_id, "gps")
  expect_equal(call$arguments$timestamp_start, start)
  expect_equal(call$arguments$timestamp_end, end)
  expect_identical(call$arguments$handle, handle)
  expect_false(call$arguments$convert_spatial_columns)
  expect_false(call$arguments$progress)
  expect_true("deployment_id" %in% call$arguments$attributes)
  expect_true(all(c(
    "event_id",
    "sensor_type_id",
    "activity_count",
    "tag_voltage"
  ) %in% call$arguments$attributes))
})


test_that("GM preparation keeps only database measurement columns", {
  events <- data.table(
    event_id = c(101, 102),
    sensor_type_id = c(653, 653),
    deployment_id = c(NA, 9001),
    timestamp = as.POSIXct(
      c("2026-07-20 10:00:00.123", "2026-07-20 11:00:00.456"),
      tz = "UTC"
    ),
    location_long = c(8.1, NA),
    location_lat = c(52.1, 52.2),
    activity_count = c(10, 20),
    external_temperature = c(18.5, 19),
    gps_hdop = c(1.2, 1.3),
    gps_satellite_count = c(8, 9),
    gps_vdop = c(1.9, 2),
    ground_speed = c(7.2, 8),
    heading = c(315, 320),
    height_above_msl = c(34, 35),
    tag_voltage = c(2895, 2890)
  )

  result <- .gm_prepare_locations(events)

  expect_named(
    result,
    c(
      "event_id",
      "sensor_type_id",
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
  )
  expect_equal(nrow(result), 1)
  expect_equal(result$event_id, 101)
  expect_equal(result$timestamp, "2026-07-20 10:00:00.123")
  expect_false("deployment_id" %in% names(result))
})


test_that("GM insertion stages rows and refreshes duplicate events", {
  calls <- new.env(parent = emptyenv())
  calls$disconnected <- FALSE

  local_mocked_bindings(
    dbcon = function(db, server) {
      expect_equal(db, "GM")
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

  locations <- data.table(
    event_id = 101,
    sensor_type_id = 653,
    timestamp = "2026-07-20 10:00:00.123",
    location_long = 8.1,
    location_lat = 52.1
  )

  expect_equal(.gm_insert_locations(locations), 1)
  expect_equal(calls$stage_name, "gm_locations_stage")
  expect_equal(calls$stage, locations)
  expect_true(calls$temporary)
  expect_false(calls$row_names)
  expect_match(calls$statement, "INSERT INTO locations")
  expect_match(calls$statement, "ON DUPLICATE KEY UPDATE")
  expect_match(
    calls$statement,
    "location_long = VALUES\\(location_long\\)"
  )
  expect_true(calls$disconnected)
})


test_that("GM daily update overlaps the watermark in one-day windows", {
  calls <- new.env(parent = emptyenv())
  calls$windows <- list()

  local_mocked_bindings(
    .gm_handle = function() "handle",
    .gm_study_id = function(handle) "8532542312",
    .gm_watermark = function() "2026-07-20T12:00:00.000000Z",
    .gm_current_time = function() {
      ymd_hms("2026-07-21T12:00:00Z", tz = "UTC")
    },
    .gm_download = function(
      handle,
      study_id,
      timestamp_start,
      timestamp_end
    ) {
      calls$windows[[length(calls$windows) + 1]] <- list(
        start = timestamp_start,
        end = timestamp_end
      )
      data.table(
        event_id = length(calls$windows),
        sensor_type_id = 653,
        deployment_id = NA,
        timestamp = timestamp_start,
        location_long = 8,
        location_lat = 52,
        activity_count = NA,
        external_temperature = NA,
        gps_hdop = NA,
        gps_satellite_count = NA,
        gps_vdop = NA,
        ground_speed = NA,
        heading = NA,
        height_above_msl = NA,
        tag_voltage = NA
      )
    },
    .gm_insert_locations = function(locations) nrow(locations)
  )

  result <- GM_update(verbose = FALSE)

  expect_equal(length(calls$windows), 3)
  expect_equal(
    .gm_display_time(calls$windows[[1]]$start),
    "2026-07-18T12:00:00Z"
  )
  expect_equal(
    .gm_display_time(calls$windows[[3]]$end),
    "2026-07-21T12:00:00Z"
  )
  expect_equal(result$downloaded, c(1, 1, 1))
  expect_equal(result$locations, c(1, 1, 1))
})
