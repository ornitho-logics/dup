#' Prepare data for RUFFatSEEWIESEN.photos
#' Expands ADULTS table by photo ID
#' @export
#' @examples
#' x = RUFFatSEEWIESEN.photos_prepare()
RUFFatSEEWIESEN.photos_prepare <- function(
  last_pk,
  basepath = config::get()$dir$ruff_photos
) {
  con = mariacon("RUFFatSEEWIESEN")
  on.exit(dbDisconnect(con))

  sql = glue(
    "SELECT ID, date, location, pic_ID pid, camera_ID, pk ad_pk  FROM
            RUFFatSEEWIESEN.ADULTS
               WHERE pic_ID  IS NOT NULL AND
                  pk > {last_pk} "
  )

  d = dbGetQuery(con, sql) |> setDT()

  o = d[,
    .(pic_ID = expand_string(pid)),
    .(ID, date, location, camera_ID, ad_pk)
  ]

  o[,
    pic_ID := glue_data(
      .SD,
      'P{camera_ID}{str_pad(pic_ID, 6, side = "left", pad = "0")}.RW2'
    )
  ]

  o[,
    path := glue_data(
      .SD,
      "{year(date)}/{location}/{format(date, '%m-%d')}/{pic_ID}"
    )
  ]

  o[, photo_exists := fs::file_exists(glue_data(.SD, "{basepath}{path}"))]

  if (nrow(o) > 0) {
    o[, i := 1:.N, .(ID, date)]

    # photo parts
    pw = data.table(
      photo_class = c(
        "back",
        "left side",
        "left wing above",
        "right wing above",
        "right side",
        "right wing below",
        "left wing below",
        "front&legs",
        "tail above",
        "ruff"
      )
    )[, i := .I]

    o = merge(o, pw, by = "i", sort = FALSE)

    o = o[, .(ID, photo_class, path, photo_exists, ad_pk)]
  }

  o
}


#' update RUFFatSEEWIESEN.photos
#' expands ADULTS table by photo ID
#' @export
#' @return number of rows updated
#' @examples
#' x = RUFFatSEEWIESEN.photos_update()
RUFFatSEEWIESEN.photos_update <- function() {
  con = mariacon("RUFFatSEEWIESEN")
  on.exit(dbDisconnect(con))

  last_pk_photos = dbGetQuery(
    con,
    "SELECT COALESCE(max(ad_pk), 0) pk FROM RUFFatSEEWIESEN.photos "
  )$pk
  last_pk_ADULTS = dbGetQuery(
    con,
    "SELECT max(pk) pk FROM RUFFatSEEWIESEN.ADULTS "
  )$pk

  if (last_pk_ADULTS > last_pk_photos) {
    glue(
      "... preparing a new batch of {last_pk_ADULTS-last_pk_photos} photos."
    ) |>
      print()
    x = RUFFatSEEWIESEN.photos_prepare(last_pk_photos)
    u = DBI::dbWriteTable(con, "photos", x, row.names = FALSE, append = TRUE)
    if (u) o = (last_pk_ADULTS - last_pk_photos) else o = 0
  } else {
    o = 0
  }

  o
}

#' Convert, resize, and remove adult plumage photos.
#' @export
#' @return number of converted files
#' @seealso rw2webp
RUFFatSEEWIESEN.photos_convert <- function(ncores = 30, ...) {
  srcdir = config::get("dir")$ruff_photos
  destdir = config::get("dir")$ruff_photos_app

  con = mariacon("RUFFatSEEWIESEN")
  on.exit(dbDisconnect(con))

  x = DBI::dbGetQuery(
    con,
    "SELECT ID, path FROM photos
         WHERE photo_exists = 1"
  ) |>
    setDT()
  x[, src_path := paste0(srcdir, path)]
  x[, dest_path := paste0(destdir, path) |> str_replace("RW2$", "webp")]

  x[, todo := !file_exists(dest_path)]

  x = x[(todo)]
  x[, i := .I]

  if (nrow(x) > 0) {
    doFuture::registerDoFuture()
    future::plan(future::multicore, workers = ncores)

    o = foreach(i = 1:nrow(x), .combine = c, .errorhandling = "remove") %dopar%
      {
        x[i, rw2webp(src_path, dest_path, ...)]
        x[i, file_exists(dest_path)]
      }

    O = sum(o)
  } else {
    O = 0
  }

  O
}


#' uses ID_changes table
#' @param  cnf  configuration variables are obtained from an external file config file.
#'         default to config::get().
#' @export
RUFFatSEEWIESEN.change_ID <- function(cnf = config::get()) {
  con <- mariacon("RUFFatSEEWIESEN")
  on.exit(dbDisconnect(con))

  d = DBI::dbGetQuery(
    con,
    "SELECT old_ID, new_ID, pk FROM ID_changes WHERE datetime_db IS NULL"
  ) |>
    setDT()

  d = d[,
    .(
      sql = c(
        paste(
          'UPDATE ADULTS    SET ID          =',
          shQuote(new_ID),
          'WHERE ID          =',
          shQuote(old_ID)
        ),
        paste(
          'UPDATE FOUNDERS    SET ID        =',
          shQuote(new_ID),
          'WHERE ID          =',
          shQuote(old_ID)
        ),
        paste(
          'UPDATE CHICKS    SET ID          =',
          shQuote(new_ID),
          'WHERE ID          =',
          shQuote(old_ID)
        ),
        paste(
          'UPDATE SEX_and_MORPH SET ID      =',
          shQuote(new_ID),
          'WHERE ID          =',
          shQuote(old_ID)
        ),
        paste(
          'UPDATE PATERNITY SET ID_father   =',
          shQuote(new_ID),
          'WHERE ID_father   =',
          shQuote(old_ID)
        ),
        paste(
          'UPDATE PATERNITY SET ID_mother   =',
          shQuote(new_ID),
          'WHERE ID_mother   =',
          shQuote(old_ID)
        )
      )
    ),
    by = 'pk'
  ]

  d[, run := DBI::dbExecute(con, sql), by = 1:nrow(d)]

  # when changes were applied then update ID_changes
  pk_timestamp_update = d[run == 1, ]$pk %>%
    unique() %>%
    paste(., collapse = ",")

  if (nchar(pk_timestamp_update) > 1) {
    DBI::dbExecute(
      con,
      paste(
        "UPDATE ID_changes set datetime_db = NOW()
                  WHERE pk in (",
        pk_timestamp_update,
        ")"
      )
    )
  }

  o = d[run == 1]

  nrow(o)
}
