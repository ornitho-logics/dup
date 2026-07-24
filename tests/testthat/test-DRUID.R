test_that("GPS preparation follows the database schema", {
  gps <- data.table(
    id = "api-record-id",
    device_id = "68d4a842c3d77b735bfd9d96",
    uuid = "1300001bdd",
    updated_at = "2026-06-01T13:05:13.775Z",
    timestamp = "2026-06-01T00:00:30.979Z",
    longitude = 8.344182,
    latitude = 52.551568,
    speed = 0.1,
    sample_type = list(c(1, 4))
  )

  result <- .druid_prepare_gps(gps)

  expect_named(
    result,
    c(
      "id",
      "uuid",
      "updated_at",
      "timestamp",
      "longitude",
      "latitude",
      "altitude",
      "geoid_altitude",
      "relative_altitude",
      "ground_altitude",
      "speed",
      "course",
      "used_star",
      "view_star",
      "fix_time",
      "horizontal",
      "vertical",
      "hdop",
      "vdop",
      "pdop",
      "quality",
      "sample_type"
    )
  )
  expect_equal(result$id, "68d4a842c3d77b735bfd9d96")
  expect_equal(result$updated_at, "2026-06-01 13:05:13.775000")
  expect_equal(result$timestamp, "2026-06-01 00:00:30.979000")
  expect_equal(result$sample_type, 5)
})


test_that("GPS preparation consolidates duplicate database keys", {
  gps <- data.table(
    id = c("api-old", "api-new", "api-unknown"),
    device_id = rep("68d4a842c3d77b735bfd9d96", 3),
    uuid = rep("1300001bdd", 3),
    updated_at = c(
      "2026-06-01T13:05:13.775Z",
      "2026-06-01T13:06:13.775Z",
      NA_character_
    ),
    timestamp = rep("2026-06-01T00:00:30.979Z", 3),
    longitude = c(8.1, NA, 99),
    latitude = c(52.1, 52.2, 99),
    altitude = c(NA, NA, 10),
    sample_type = list(c(1, 4), 2, 4)
  )

  result <- .druid_prepare_gps(gps)

  expect_equal(nrow(result), 1)
  expect_equal(result$updated_at, "2026-06-01 13:06:13.775000")
  expect_equal(result$longitude, 8.1)
  expect_equal(result$latitude, 52.2)
  expect_equal(result$altitude, 10)
  expect_equal(result$sample_type, 2)
})


test_that("empty GPS input remains empty", {
  expect_equal(.druid_prepare_gps(data.table()), data.table())
})


test_that("GPS watermark ordering does not reference an aggregate alias", {
  query <- .druid_gps_watermark_query()

  expect_match(
    query,
    "ORDER BY\\s+MAX\\(g.timestamp\\) IS NOT NULL,\\s+MAX\\(g.timestamp\\)"
  )
  expect_false(
    grepl("last_timestamp IS NOT NULL", query, fixed = TRUE)
  )
})


test_that("GPS insertion treats existing database keys as no-ops", {
  calls <- new.env(parent = emptyenv())
  calls$disconnected <- FALSE

  local_mocked_bindings(
    dbcon = function(db, server) {
      expect_equal(db, "DRUID")
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

  gps <- data.table(
    id = "68d4a842c3d77b735bfd9d96",
    uuid = "1300001bdd",
    updated_at = "2026-06-01 13:05:13.775000",
    timestamp = "2026-06-01 00:00:30.979000"
  )

  expect_equal(.druid_insert_gps(gps), 1)
  expect_equal(calls$stage_name, "druid_gps_stage")
  expect_equal(calls$stage, gps)
  expect_true(calls$temporary)
  expect_false(calls$row_names)
  expect_match(
    calls$statement,
    "ON DUPLICATE KEY UPDATE\\s+id = VALUES\\(id\\)\\s*$"
  )
  expect_false(grepl("INSERT IGNORE", calls$statement, fixed = TRUE))
  expect_true(calls$disconnected)
})


test_that("GPS update populates an empty table and overlaps existing data", {
  calls <- new.env(parent = emptyenv())
  calls$downloads <- list()

  local_mocked_bindings(
    .druid_gps_watermarks = function() {
      data.table(
        id = c("new-device", "existing-device"),
        last_timestamp = c(
          NA_character_,
          "2026-07-20T00:00:00.000000Z"
        )
      )
    },
    ecotopia_data = function(
      logstring,
      id,
      datetime,
      what,
      verbose
    ) {
      calls$downloads[[id]] <- list(
        logstring = logstring,
        datetime = datetime,
        what = what,
        verbose = verbose
      )

      data.table(
        id = glue("record-{id}"),
        device_id = id,
        uuid = glue("uuid-{id}"),
        updated_at = "2026-07-21T00:00:00Z",
        timestamp = "2026-07-20T00:00:00Z",
        longitude = 8,
        latitude = 52,
        sample_type = 1
      )
    },
    .druid_insert_gps = function(gps) nrow(gps)
  )

  messages <- capture_messages(
    result <- .druid_update_gps("token", verbose = TRUE)
  )
  expect_true(
    any(grepl("GPS \\[1/2\\] new-device", messages))
  )

  expect_equal(
    calls$downloads[["new-device"]]$datetime,
    "2000-01-01T00:00:00Z"
  )
  expect_equal(
    calls$downloads[["existing-device"]]$datetime,
    "2026-07-18T00:00:00Z"
  )
  expect_equal(calls$downloads[["new-device"]]$what, "gps")
  expect_false(calls$downloads[["new-device"]]$verbose)
  expect_true(all(result$success))
  expect_equal(result$downloaded, c(1, 1))
  expect_equal(result$affected, c(1, 1))
})


test_that("GPS update isolates failures by device", {
  local_mocked_bindings(
    .druid_gps_watermarks = function() {
      data.table(
        id = c("working-device", "failed-device"),
        last_timestamp = c(NA_character_, NA_character_)
      )
    },
    ecotopia_data = function(
      logstring,
      id,
      datetime,
      what,
      verbose
    ) {
      if (id == "failed-device") {
        stop("simulated API error")
      }

      data.table(
        id = "api-record-id",
        device_id = id,
        uuid = "uuid-working-device",
        updated_at = "2026-07-21T00:00:00Z",
        timestamp = "2026-07-20T00:00:00Z",
        longitude = 8,
        latitude = 52,
        sample_type = 1
      )
    },
    .druid_insert_gps = function(gps) nrow(gps)
  )

  expect_warning(
    result <- .druid_update_gps("token"),
    "GPS update failed for 1 device"
  )

  expect_true(result[id == "working-device", success])
  expect_false(result[id == "failed-device", success])
  expect_equal(
    result[id == "failed-device", error],
    "simulated API error"
  )
})


test_that("GPS update handles an empty device list", {
  local_mocked_bindings(
    .druid_gps_watermarks = function() {
      data.table(id = character(), last_timestamp = character())
    }
  )

  expect_equal(.druid_update_gps("token"), data.table())
})


test_that("DRUID update runs the device and GPS layers in order", {
  calls <- new.env(parent = emptyenv())
  calls$order <- character()

  local_mocked_bindings(
    .druid_credentials = function() {
      list(
        generic = list(un = "username", pwd = "password"),
        kw1 = "first-keyword",
        kw2 = "second-keyword"
      )
    },
    ecotopia_login = function(un, pwd, kw1, kw2, verbose) {
      expect_equal(un, "username")
      expect_equal(pwd, "password")
      expect_equal(kw1, "first-keyword")
      expect_equal(kw2, "second-keyword")
      expect_false(verbose)
      "login-token"
    },
    .druid_update_device_list = function(logstring) {
      calls$order <- c(calls$order, "devices")
      expect_equal(logstring, "login-token")
      2
    },
    .druid_update_gps = function(logstring, verbose) {
      calls$order <- c(calls$order, "gps")
      expect_equal(logstring, "login-token")
      expect_false(verbose)
      data.table(id = "device", success = TRUE)
    }
  )

  result <- withVisible(DRUID_update(verbose = FALSE))

  expect_false(result$visible)
  expect_equal(calls$order, c("devices", "gps"))
  expect_equal(result$value$devices_added, 2)
  expect_true(result$value$gps$success)
})


test_that("DRUID update reports stages when verbose", {
  local_mocked_bindings(
    .druid_credentials = function() {
      list(
        generic = list(un = "username", pwd = "password"),
        kw1 = "first-keyword",
        kw2 = "second-keyword"
      )
    },
    ecotopia_login = function(un, pwd, kw1, kw2, verbose) {
      "login-token"
    },
    .druid_update_device_list = function(logstring) 1,
    .druid_update_gps = function(logstring, verbose) {
      expect_true(verbose)
      data.table()
    }
  )

  messages <- capture_messages(
    DRUID_update(verbose = TRUE)
  )
  expect_true(
    any(grepl("DRUID: authenticating with Ecotopia", messages))
  )
})
