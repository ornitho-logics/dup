#' Argos pipeline
#' @export
ARGOS.pipeline <- function() {
    task1 <- extract_email_attachements(maildir = "ARGOS") |> try(silent = TRUE)

    task2 <- scidbupdate_ARGOS.incoming() |> try(silent = TRUE)

    task3 <- scidbupdate_ARGOS.flush_incoming() |> try(silent = TRUE)

    try_outcome(task1, task2, task3, message = "ARGOS.pipeline is failing!")
}

#' Argos2 pipeline
#' @export
ARGOS2.pipeline <- function() {
    NEW <- ARGOS2.downloadNew() |> try(silent = TRUE)

    locations <- ARGOS2.prepare_locations(NEW) |> try(silent = TRUE)
    sensors <- ARGOS2.prepare_sensors(NEW) |> try(silent = TRUE)

    update_locations <- ARGOS2.update(locations, "locations") |>
        try(silent = TRUE)
    update_sensors <- ARGOS2.update(sensors, "sensors") |> try(silent = TRUE)

    try_outcome(
        NEW,
        locations,
        sensors,
        update_locations,
        update_sensors,
        message = "ARGOS2.pipeline is failing!"
    )
}

#' RUFFatSEEWIESEN pipelines
#' @export
RUFFatSEEWIESEN_photos.pipeline <- function(...) {
    task1 <- RUFFatSEEWIESEN.photos_update() |> try(silent = TRUE)

    task2 <- RUFFatSEEWIESEN.photos_convert(...) |> try(silent = TRUE)

    try_outcome(
        task1,
        task2,
        message = "RUFFatSEEWIESEN_photos.pipeline is failing!"
    )
}
