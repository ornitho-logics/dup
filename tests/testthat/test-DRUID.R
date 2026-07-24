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
    used_star = -99999,
    view_star = -99999,
    quality = -99999,
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
  expect_equal(result$used_star, -99999)
  expect_equal(result$view_star, -99999)
  expect_equal(result$quality, -99999)
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


test_that("ODBA preparation follows the database schema", {
  odba <- data.table(
    id = "api-record-id",
    device_id = "65253b9d046763d5ac35914d",
    uuid = "1300000e6a",
    updated_at = "2026-07-24T15:05:00.125Z",
    timestamp = "2026-07-24T15:00:01Z",
    odba = 260,
    odba_x = -99999,
    odba_y = -99999,
    odba_z = -99999,
    meandl_x = -99999,
    meandl_y = -99999,
    meandl_z = -99999,
    sample_type = list(c(1, 4))
  )

  result <- .druid_prepare_odba(odba)

  expect_named(
    result,
    c(
      "id",
      "uuid",
      "updated_at",
      "timestamp",
      "odba",
      "odba_x",
      "odba_y",
      "odba_z",
      "meandl_x",
      "meandl_y",
      "meandl_z",
      "sample_type"
    )
  )
  expect_equal(result$id, "65253b9d046763d5ac35914d")
  expect_equal(result$updated_at, "2026-07-24 15:05:00.125000")
  expect_equal(result$timestamp, "2026-07-24 15:00:01.000000")
  expect_equal(result$odba, 260)
  expect_equal(result$odba_x, -99999)
  expect_equal(result$sample_type, 5)
})


test_that("empty ODBA input remains empty", {
  expect_equal(.druid_prepare_odba(data.table()), data.table())
})


test_that("ODBA watermark ordering uses its aggregate expression", {
  query <- .druid_odba_watermark_query()

  expect_match(
    query,
    "ORDER BY\\s+MAX\\(o.timestamp\\) IS NOT NULL,\\s+MAX\\(o.timestamp\\)"
  )
  expect_false(
    grepl("last_timestamp IS NOT NULL", query, fixed = TRUE)
  )
})


test_that("ODBA insertion treats existing database keys as no-ops", {
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

  odba <- data.table(
    id = "65253b9d046763d5ac35914d",
    uuid = "1300000e6a",
    updated_at = "2026-07-24 15:05:00.125000",
    timestamp = "2026-07-24 15:00:01.000000",
    odba = 260
  )

  expect_equal(.druid_insert_odba(odba), 1)
  expect_equal(calls$stage_name, "druid_odba_stage")
  expect_equal(calls$stage, odba)
  expect_true(calls$temporary)
  expect_false(calls$row_names)
  expect_match(
    calls$statement,
    "ON DUPLICATE KEY UPDATE\\s+id = VALUES\\(id\\)\\s*$"
  )
  expect_false(grepl("INSERT IGNORE", calls$statement, fixed = TRUE))
  expect_true(calls$disconnected)
})


test_that("ODBA update populates an empty table and overlaps existing data", {
  calls <- new.env(parent = emptyenv())
  calls$downloads <- list()

  local_mocked_bindings(
    .druid_odba_watermarks = function() {
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
        odba = 260,
        sample_type = list(0)
      )
    },
    .druid_insert_odba = function(odba) nrow(odba)
  )

  messages <- capture_messages(
    result <- .druid_update_odba("token", verbose = TRUE)
  )
  expect_true(
    any(grepl("ODBA \\[1/2\\] new-device", messages))
  )

  expect_equal(
    calls$downloads[["new-device"]]$datetime,
    "2000-01-01T00:00:00Z"
  )
  expect_equal(
    calls$downloads[["existing-device"]]$datetime,
    "2026-07-18T00:00:00Z"
  )
  expect_equal(calls$downloads[["new-device"]]$what, "odba")
  expect_false(calls$downloads[["new-device"]]$verbose)
  expect_true(all(result$success))
  expect_equal(result$downloaded, c(1, 1))
  expect_equal(result$affected, c(1, 1))
})


test_that("ODBA update isolates failures by device", {
  local_mocked_bindings(
    .druid_odba_watermarks = function() {
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
        odba = 260,
        sample_type = list(0)
      )
    },
    .druid_insert_odba = function(odba) nrow(odba)
  )

  expect_warning(
    result <- .druid_update_odba("token"),
    "ODBA update failed for 1 device"
  )

  expect_true(result[id == "working-device", success])
  expect_false(result[id == "failed-device", success])
  expect_equal(
    result[id == "failed-device", error],
    "simulated API error"
  )
})


test_that("ENV preparation follows the database schema", {
  environment <- data.table(
    id = "api-record-id",
    device_id = "65253b9d046763d5ac35914d",
    uuid = "1300000e6a",
    updated_at = "2026-07-24T15:05:00.125Z",
    timestamp = "2026-07-24T15:00:01Z",
    inner_temperature = 17.1,
    inner_humidity = 55,
    ambient_light = 3,
    inner_light = 114,
    inner_pressure = 837.64,
    battery_power = 85,
    battery_voltage = 3.97,
    charge_voltage = -99999.99,
    charge_current = -99999.99,
    sample_type = list(c(1, 2))
  )

  result <- .druid_prepare_env(environment)

  expect_named(
    result,
    c(
      "id",
      "uuid",
      "updated_at",
      "timestamp",
      "inner_temperature",
      "inner_humidity",
      "ambient_light",
      "inner_light",
      "inner_pressure",
      "battery_power",
      "battery_voltage",
      "charge_voltage",
      "charge_current",
      "sample_type"
    )
  )
  expect_equal(result$id, "65253b9d046763d5ac35914d")
  expect_equal(result$updated_at, "2026-07-24 15:05:00.125000")
  expect_equal(result$timestamp, "2026-07-24 15:00:01.000000")
  expect_equal(result$inner_temperature, 17.1)
  expect_equal(result$inner_pressure, 837.64)
  expect_equal(result$charge_voltage, -99999.99)
  expect_equal(result$sample_type, 3)
})


test_that("empty ENV input remains empty", {
  expect_equal(.druid_prepare_env(data.table()), data.table())
})


test_that("ENV watermark ordering uses its aggregate expression", {
  query <- .druid_env_watermark_query()

  expect_match(
    query,
    "ORDER BY\\s+MAX\\(e.timestamp\\) IS NOT NULL,\\s+MAX\\(e.timestamp\\)"
  )
  expect_false(
    grepl("last_timestamp IS NOT NULL", query, fixed = TRUE)
  )
})


test_that("ENV insertion treats existing database keys as no-ops", {
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

  environment <- data.table(
    id = "65253b9d046763d5ac35914d",
    uuid = "1300000e6a",
    updated_at = "2026-07-24 15:05:00.125000",
    timestamp = "2026-07-24 15:00:01.000000",
    inner_temperature = 17.1
  )

  expect_equal(.druid_insert_env(environment), 1)
  expect_equal(calls$stage_name, "druid_env_stage")
  expect_equal(calls$stage, environment)
  expect_true(calls$temporary)
  expect_false(calls$row_names)
  expect_match(
    calls$statement,
    "ON DUPLICATE KEY UPDATE\\s+id = VALUES\\(id\\)\\s*$"
  )
  expect_false(grepl("INSERT IGNORE", calls$statement, fixed = TRUE))
  expect_true(calls$disconnected)
})


test_that("ENV update populates an empty table and overlaps existing data", {
  calls <- new.env(parent = emptyenv())
  calls$downloads <- list()

  local_mocked_bindings(
    .druid_env_watermarks = function() {
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
        inner_temperature = 17.1,
        sample_type = list(1)
      )
    },
    .druid_insert_env = function(environment) nrow(environment)
  )

  messages <- capture_messages(
    result <- .druid_update_env("token", verbose = TRUE)
  )
  expect_true(
    any(grepl("ENV \\[1/2\\] new-device", messages))
  )

  expect_equal(
    calls$downloads[["new-device"]]$datetime,
    "2000-01-01T00:00:00Z"
  )
  expect_equal(
    calls$downloads[["existing-device"]]$datetime,
    "2026-07-18T00:00:00Z"
  )
  expect_equal(calls$downloads[["new-device"]]$what, "env")
  expect_false(calls$downloads[["new-device"]]$verbose)
  expect_true(all(result$success))
  expect_equal(result$downloaded, c(1, 1))
  expect_equal(result$affected, c(1, 1))
})


test_that("ENV update isolates failures by device", {
  local_mocked_bindings(
    .druid_env_watermarks = function() {
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
        inner_temperature = 17.1,
        sample_type = list(1)
      )
    },
    .druid_insert_env = function(environment) nrow(environment)
  )

  expect_warning(
    result <- .druid_update_env("token"),
    "ENV update failed for 1 device"
  )

  expect_true(result[id == "working-device", success])
  expect_false(result[id == "failed-device", success])
  expect_equal(
    result[id == "failed-device", error],
    "simulated API error"
  )
})


test_that("behaviour preparation stores the raw base64 payload", {
  raw <- data.table(
    id = "api-record-id",
    device_id = "68d4a842c3d77b735bfd9d96",
    uuid = "1300001bdd",
    updated_at = "2026-06-01T13:05:19.011Z",
    timestamp = "2026-06-01T00:01:50Z",
    version = 0,
    type = 2,
    data = "7wDwAAAAAAAQAAAAAAAAAAAAAgIA"
  )

  result <- .druid_prepare_behaviour(raw)

  expect_named(
    result,
    c(
      "id",
      "uuid",
      "updated_at",
      "timestamp",
      "version",
      "type",
      "behaviour"
    )
  )
  expect_equal(result$id, "68d4a842c3d77b735bfd9d96")
  expect_equal(result$updated_at, "2026-06-01 13:05:19.011000")
  expect_equal(result$timestamp, "2026-06-01 00:01:50.000000")
  expect_equal(result$version, 0)
  expect_equal(result$type, 2)
  expect_equal(result$behaviour, raw$data)
})


test_that("empty behaviour input remains empty", {
  expect_equal(.druid_prepare_behaviour(data.table()), data.table())
})


test_that("behaviour watermark ordering uses its aggregate expression", {
  query <- .druid_behaviour_watermark_query()

  expect_match(
    query,
    "ORDER BY\\s+MAX\\(b.timestamp\\) IS NOT NULL,\\s+MAX\\(b.timestamp\\)"
  )
  expect_false(
    grepl("last_timestamp IS NOT NULL", query, fixed = TRUE)
  )
})


test_that("behaviour insertion treats existing database keys as no-ops", {
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

  behaviour <- data.table(
    id = "68d4a842c3d77b735bfd9d96",
    uuid = "1300001bdd",
    updated_at = "2026-06-01 13:05:19.011000",
    timestamp = "2026-06-01 00:01:50.000000",
    version = 0,
    type = 2,
    behaviour = "7wDwAAAAAAAQAAAAAAAAAAAAAgIA"
  )

  expect_equal(.druid_insert_behaviour(behaviour), 1)
  expect_equal(calls$stage_name, "druid_behaviour_stage")
  expect_equal(calls$stage, behaviour)
  expect_true(calls$temporary)
  expect_false(calls$row_names)
  expect_match(
    calls$statement,
    "ON DUPLICATE KEY UPDATE\\s+id = VALUES\\(id\\)\\s*$"
  )
  expect_false(grepl("INSERT IGNORE", calls$statement, fixed = TRUE))
  expect_true(calls$disconnected)
})


test_that("behaviour update stores structured data without decoding it", {
  calls <- new.env(parent = emptyenv())
  calls$downloads <- list()

  local_mocked_bindings(
    .druid_behaviour_watermarks = function() {
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
        updated_at = "2026-07-21T00:05:00Z",
        timestamp = "2026-07-20T00:00:00Z",
        version = 0,
        type = 2,
        data = "AQID"
      )
    },
    .druid_insert_behaviour = function(behaviour) {
      expect_equal(behaviour$behaviour, "AQID")
      nrow(behaviour)
    }
  )

  messages <- capture_messages(
    result <- .druid_update_behaviour("token", verbose = TRUE)
  )
  expect_true(
    any(grepl("BEHAVIOUR \\[1/2\\] new-device", messages))
  )

  expect_equal(
    calls$downloads[["new-device"]]$datetime,
    "2000-01-01T00:00:00Z"
  )
  expect_equal(
    calls$downloads[["existing-device"]]$datetime,
    "2026-07-18T00:00:00Z"
  )
  expect_equal(calls$downloads[["new-device"]]$what, "structured")
  expect_false(calls$downloads[["new-device"]]$verbose)
  expect_true(all(result$success))
  expect_equal(result$downloaded, c(1, 1))
  expect_equal(result$affected, c(1, 1))
})


test_that("behaviour update isolates failures by device", {
  local_mocked_bindings(
    .druid_behaviour_watermarks = function() {
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
        updated_at = "2026-07-21T00:05:00Z",
        timestamp = "2026-07-20T00:00:00Z",
        version = 0,
        type = 2,
        data = "AQID"
      )
    },
    .druid_insert_behaviour = function(behaviour) nrow(behaviour)
  )

  expect_warning(
    result <- .druid_update_behaviour("token"),
    "BEHAVIOUR update failed for 1 device"
  )

  expect_true(result[id == "working-device", success])
  expect_false(result[id == "failed-device", success])
  expect_equal(
    result[id == "failed-device", error],
    "simulated API error"
  )
})


test_that("DRUID update runs all data layers in order", {
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
    },
    .druid_update_odba = function(logstring, verbose) {
      calls$order <- c(calls$order, "odba")
      expect_equal(logstring, "login-token")
      expect_false(verbose)
      data.table(id = "device", success = TRUE)
    },
    .druid_update_env = function(logstring, verbose) {
      calls$order <- c(calls$order, "env")
      expect_equal(logstring, "login-token")
      expect_false(verbose)
      data.table(id = "device", success = TRUE)
    },
    .druid_update_behaviour = function(logstring, verbose) {
      calls$order <- c(calls$order, "behaviour")
      expect_equal(logstring, "login-token")
      expect_false(verbose)
      data.table(id = "device", success = TRUE)
    }
  )

  result <- withVisible(DRUID_update(verbose = FALSE))

  expect_false(result$visible)
  expect_equal(
    calls$order,
    c("devices", "gps", "odba", "env", "behaviour")
  )
  expect_equal(result$value$devices_added, 2)
  expect_true(result$value$gps$success)
  expect_true(result$value$odba$success)
  expect_true(result$value$env$success)
  expect_true(result$value$behaviour$success)
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
    },
    .druid_update_odba = function(logstring, verbose) {
      expect_true(verbose)
      data.table()
    },
    .druid_update_env = function(logstring, verbose) {
      expect_true(verbose)
      data.table()
    },
    .druid_update_behaviour = function(logstring, verbose) {
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
