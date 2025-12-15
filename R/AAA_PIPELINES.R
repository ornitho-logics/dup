#' Druid pipeline
#' @export
DRUID.pipeline <- function() {
    task1 = try(
        DRUID.downloadNew(what = "GPS", logfile = "log_druid_GPS.csv"),
        silent = TRUE
    )
    task2 = try(
        DRUID.downloadNew(what = "ODBA", logfile = "log_druid_ODBA.csv"),
        silent = TRUE
    )
    task3 = try(
        DRUID.downloadNew(what = "ENV", logfile = "log_druid_ENV.csv"),
        silent = TRUE
    )
    try_outcome(task1, task2, task3, message = "DRUID.pipeline is failing!")
}


#' Argos pipeline
#' @export
ARGOS.pipeline <- function() {
    task1 = extract_email_attachements(maildir = "ARGOS") |> try(silent = TRUE)

    task2 = scidbupdate_ARGOS.incoming() |> try(silent = TRUE)

    task3 = scidbupdate_ARGOS.flush_incoming() |> try(silent = TRUE)

    try_outcome(task1, task2, task3, message = "ARGOS.pipeline is failing!")
}

#' Argos2 pipeline
#' @export
ARGOS2.pipeline <- function() {
    NEW = ARGOS2.downloadNew() |> try(silent = TRUE)

    locations = ARGOS2.prepare_locations(NEW) |> try(silent = TRUE)
    sensors = ARGOS2.prepare_sensors(NEW) |> try(silent = TRUE)

    update_locations = ARGOS2.update(locations, "locations") |>
        try(silent = TRUE)
    update_sensors = ARGOS2.update(sensors, "sensors") |> try(silent = TRUE)

    try_outcome(
        NEW,
        locations,
        sensors,
        update_locations,
        update_sensors,
        message = "ARGOS2.pipeline is failing!"
    )
}


#' DB internal updates pipeline
#' @export
DB_internal_updates.pipeline <- function() {
    task = RUFFatSEEWIESEN.change_ID() |> try(silent = TRUE)
    try_outcome(task, message = "DB_internal_updates.pipeline is failing!")
}

#' Backup pipeline
#' @export
backup.pipeline <- function(cnf = config::get('host')) {
    con <- dbcon(server = "scidb")
    stopifnot(con@host == "scidb.mpio.orn.mpg.de")
    on.exit(dbDisconnect(con))

    x <- dbq(con, 'SELECT db from DBLOG.backup WHERE state = "freeze"')
    Exclude <- c("mysql", "information_schema", "performance_schema", x$db)

    task1 = mysqldump_host(exclude = Exclude) |> try(silent = TRUE)

    task2 = rm_old_backups(keep = 10) |> try(silent = TRUE)

    try_outcome(task1, task2, message = "backup.pipeline is failing!")
}


#' RUFFatSEEWIESEN pipelines
#' @export
RUFFatSEEWIESEN_photos.pipeline <- function(...) {
    task1 = RUFFatSEEWIESEN.photos_update() |> try(silent = TRUE)

    task2 = RUFFatSEEWIESEN.photos_convert(...) |> try(silent = TRUE)

    try_outcome(
        task1,
        task2,
        message = "RUFFatSEEWIESEN_photos.pipeline is failing!"
    )
}
