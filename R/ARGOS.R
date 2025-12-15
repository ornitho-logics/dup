#' @title Argos incoming
#' @param  cnf  configuration variables are obtained from an external file config file.
#'         default to config::get().
#' @param daysBefore converted to  Sys.Date() - 365 and passed to read_email_attachements(lastdate=).
#' @export
#' @examples
#' scidbupdate_ARGOS.incoming()
scidbupdate_ARGOS.incoming <- function(cnf = config::get(), daysBefore = 365) {
  host = cnf$host$name
  db = cnf$db$argos
  user = cnf$host$dbadmin
  pwd = cnf$host$dbpwd

  con = dbConnect(
    RMariaDB::MariaDB(),
    user = user,
    password = pwd,
    host = host,
    dbname = db
  )

  on.exit(dbDisconnect(con))

  from = as.POSIXct(Sys.Date() - 365)

  P = dbGetQuery(con, 'select tableName FROM projects WHERE active = "Y"')
  setDT(P)
  P = rbind(P, data.frame(tableName = 'incoming'))
  done = P[,
    dbGetQuery(
      con,
      paste(
        'select distinct filenam from',
        tableName,
        'where filenam is not NULL'
      )
    ),
    by = tableName
  ]

  # already uploaded to db

  if (nrow(done) > 0) {
    x = read_email_attachements(
      maildir = 'ARGOS',
      sep = ";",
      lastdate = from,
      exclude = done$filenam,
      sepDate = ""
    )
  }

  if (nrow(done) == 0) {
    x = read_email_attachements(
      maildir = 'ARGOS',
      sep = ";",
      lastdate = from,
      sepDate = ""
    )
  }

  if (nrow(x) > 0) {
    x = x[!is.na(PTT)]

    # write to DB
    setnames(x, make.names(names(x)))

    x = x[, .(
      PTT,
      Satellite,
      Location.date,
      Message.date,
      Location.class,
      Compression.index,
      Latitude,
      Longitude,
      X1,
      X2,
      X3,
      X4,
      X5,
      X6,
      X7,
      X8,
      filenam
    )]

    setnames(
      x,
      c(
        "tagID",
        "satellite",
        "locationDate",
        "messageDate",
        "locationClass",
        "compressionIndex",
        "latitude",
        "longitude",
        "S1",
        "S2",
        "S3",
        "S4",
        "S5",
        "S6",
        "S7",
        "S8",
        "filenam"
      )
    )

    x[, locationDate := anytime(locationDate)]
    x[, messageDate := anytime(messageDate)]

    x[, let(
      S1 = as.numeric(S1),
      S2 = as.numeric(S2),
      S3 = as.numeric(S3),
      S4 = as.numeric(S4),
      S5 = as.numeric(S5),
      S6 = as.numeric(S6),
      S7 = as.numeric(S7),
      S8 = as.numeric(S8)
    )]

    # announce last pk
    lpk = dbGetQuery(con, 'select max(pk) pk from incoming')$pk
    message(paste('----------> last pk in incoming = ', lpk))

    dbWriteTable(con, 'incoming', x, row.names = FALSE, append = TRUE)
    n_rows = nrow(x)
  } else {
    n_rows = 0
  }

  n_rows
}


#' @title Argos: move from incoming to YYYY_SSSS
#' @param  cnf  configuration variables are obtained from an external file config file.
#'         default to config::get().
#' @export
#' @examples
#' scidbupdate_ARGOS.flush_incoming()
scidbupdate_ARGOS.flush_incoming <- function(cnf = config::get()) {
  host = cnf$host$name
  user = cnf$host$dbadmin
  db = cnf$db$argos
  pwd = cnf$host$dbpwd

  con = dbConnect(
    RMariaDB::MariaDB(),
    user = user,
    password = pwd,
    host = host,
    dbname = db
  )
  on.exit(dbDisconnect(con))

  P = dbGetQuery(
    con,
    'select  tagIDs, startDate, tableName 
          FROM projects  WHERE active = "Y" '
  )
  setDT(P)

  # find pk-s in incoming not yet in YYYY_SSSS tables
  P[, tagIDs := str_replace(tagIDs, '\\[', "(")]
  P[, tagIDs := str_replace(tagIDs, '\\]', ")")]

  P[,
    q := paste(
      'SELECT pk from incoming WHERE locationDate >=',
      shQuote(startDate),
      'and tagID in',
      tagIDs
    )
  ]

  x = P[, dbGetQuery(con, q), by = tableName]

  if (nrow(x) > 0) {
    x = x[, .(hot = sqlin(pk), n = .N), by = tableName]

    x[,
      colnams := paste(
        setdiff(
          dbGetQuery(con, paste('select * from', tableName, 'where FALSE')) %>%
            names,
          'pk'
        ),
        collapse = ','
      ),
      by = tableName
    ]

    x[,
      q := paste(
        'INSERT INTO',
        tableName,
        '(',
        colnams,
        ') 
              SELECT ',
        colnams,
        'FROM incoming 
                  WHERE pk in',
        hot
      )
    ]
    # RUN
    x[
      n > 0,
      run := as.character(try(dbExecute(con, q), silent = TRUE)),
      by = tableName
    ]

    # when RUN ok then remove entries in incoming
    z = x[as.numeric(run) > 0, .(tableName, hot)]

    z[, q := paste('DELETE FROM incoming where pk in', hot)]

    if (nrow(z) > 0) {
      z[, run := dbExecute(con, q), by = tableName]
      out = nrow(z)
    } else {
      out = 0
    }
  } else {
    out = 0
  }

  out
}
