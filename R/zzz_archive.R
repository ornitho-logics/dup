# Retired functions which are not exported anymore

BUTEOatEUROPE.pipeline <- function() {
  cat(red$bold('\n ----> Get new emails and extract attachments ......\n'))
  extract_email_attachements(maildir = "GSM_MTI")

  cat(blue$bold('\n ----> Update gps table.....\n'))
  a = scidbupdate_mti_gps.BUTEOatEUROPE()
  push_msg(a, 'BUTEOatEUROPE.mti_gps')

  cat(green$bold('\n ----> Update sensors table....\n'))
  b = scidbupdate_mti_sensors.BUTEOatEUROPE()
  push_msg(b, 'BUTEOatEUROPE.mti_sensors')
}
SNBatWESTERHOLZ2_pipeline <- function() {
  a = scidb_snbUpdater.b000()
  Sys.sleep(5)
  b = scidb_snbUpdater.transponders()

  # feedback
  m = glue(a, b)
  push_msg(m, 'SNBatWESTERHOLZ2_pipeline')
}
read_boxtxt <- function(f) {
  cnf = config::get('dir')
  d = data.table(V = readLines(con = f, skipNul = TRUE))
  file_path = str_remove(f, paste0(cnf$base, cnf$snb))

  d[, V := str_to_upper(V)]
  d = d[str_detect(V, 'TRANSPONDER:|LBO:|LBI:')]

  d[, datetime_ := snbstring2date_v2(V)]
  d[, sensor_value := str_extract(V, 'TRANSPONDER:[ \\t]*([^\\n\\r]*)')]
  d[
    is.na(sensor_value),
    sensor_value := str_extract(V, 'LB[IO]:[ \\t]*([OFF|ON]*)')
  ]
  d[, sensor := str_extract(sensor_value, 'LB[IO]|TRANSPONDER')]
  d[, sensor_value := str_remove(sensor_value, 'LB[IO]:|TRANSPONDER:')]
  d[, sensor_value := str_trim(sensor_value)]
  d[, sensor := str_sub(sensor, 1, 3) %>% str_to_lower]

  # flag garbage
  d[str_count(sensor_value) != 16 & sensor == 'tra', g := 1]
  d[!sensor_value %in% c('ON', 'OFF') & sensor %in% c('lbi', 'lbo'), g := 1]
  #prop garbage (from the total of possibly good lines)
  pg = nrow(d[g == 1]) / nrow(d)

  # final subset
  o = d[is.na(g), .(datetime_, sensor_value, sensor)]
  o[, path := file_path]

  if (nrow(o) == 0) {
    pg = 1
  }

  # set attributes (set file_path too because of empty files)
  setattr(
    o,
    'SNB2',
    data.frame(box = basename2int(f), path = file_path, garbage = pg)
  )

  o
}
scidb_snbUpdater.b000 <- function(cnf = config::get()) {
  Start = Sys.time()

  u = cnf$host$dbadmin
  h = cnf$host$name
  p = paste0(cnf$dir$base, cnf$dir$snb)
  y = year(Sys.Date())
  db = cnf$db$snb
  pwd = cnf$host$dbpwd
  bb = cnf$db$snb_boxes

  con = dbConnect(
    RMariaDB::MariaDB(),
    user = u,
    password = pwd,
    host = h,
    dbname = db
  )
  on.exit(dbDisconnect(con))

  cat('db set to', dQuote(db), '...OK\n')

  cat(' ------> Searching for proper directory formats ...')
  x = data.table(dirs = list.files(paste0(p, y), full.name = TRUE))
  x[, dirnam := basename(dirs)]
  if (nrow(x) == 0) {
    stop(p, 'does not have any files')
    cat(p, 'does not have any files')
  }
  isNotDate = x[!grepl('^[0-9]{4}\\.[0-9]{1,2}\\.[0-9]{1,2}$', dirnam)]
  if (nrow(isNotDate) > 0) {
    cat('Invalid directories\n')
    stop('Invalid directories')
  }

  cat(' ------> Getting the file listing on ', p, '...')
  rawf = data.table(path = list.files(p, full.name = TRUE, recursive = TRUE))
  rawf[, box := basename2int(path)]
  rawf = rawf[box %in% bb]

  cat('got', nrow(rawf), 'raw files.\n')

  cat(' ------> Getting files already on ', db, '.b000 ... ')
  dbf = data.table(box = paste0("b", str_pad(bb, 3, "left", pad = "0")))
  dbf[, sql := paste0('select distinct path FROM ', db, '.', box)]
  dbf = dbf[, dbGetQuery(con, sql), by = box]

  cat('got', nrow(dbf), ' files listed in DB.\n')

  cat(' ------> Getting black listed files ')
  blf = dbGetQuery(con, paste0('select distinct path from ', db, '.black_list'))
  cat('got', nrow(blf), 'black list files.\n')

  alldbf = rbind(dbf[, .(path)], blf)
  alldbf[, indb := TRUE]
  alldbf[, path := paste0(p, path)]

  cat(' ------> Identifying new files ...')
  if (nrow(alldbf) == 0) {
    newf = copy(rawf)
  } else {
    newf = merge(rawf, alldbf, by = 'path', all.x = TRUE)
    newf = newf[is.na(indb)]
  }

  if (nrow(newf) == 0) {
    cat('None found. Will stop now.\n')
    return(0)
    stop('-------- NO NEW FILES FOUND -------- ')
  }

  cat('got', nrow(newf), 'new files. OK\n')

  cat(' ------> Parsing new txt files ....')

  O = foreach(i = 1:nrow(newf)) %do%
    {
      read_boxtxt(newf[i, path])
    }
  cat('OK\n')

  cat(' ------> Find if there are black listed files ....')
  B = lapply(O, function(x) attributes(x)$SNB2)
  B = rbindlist(B)
  B = B[garbage > 0.5, .(path)]

  if (nrow(B) > 0) {
    cat('got', nrow(B), 'bad files. will write to black_list ... \n')

    DBI::dbWriteTable(con, 'black_list', B, row.names = FALSE, append = TRUE)
  } else {
    cat('All files are OK... \n')
  }

  cat(' ------> Updating b000 tables on', dQuote(db), '.... ')
  pb = txtProgressBar(max = length(O), style = 3)

  out = foreach(i = 1:length(O), .combine = c, .final = sum) %do%
    {
      oi = O[[i]]
      atr = attributes(oi)$SNB2

      if (atr$garbage < 0.5) {
        res = DBI::dbWriteTable(
          con,
          int2b(atr$box),
          oi,
          row.names = FALSE,
          append = TRUE
        )
      } else {
        res = FALSE
      }
      setTxtProgressBar(pb, i)
      res
    }

  cat('\n      Uploaded', out, 'new files.\n')

  tt = difftime(Sys.time(), Start, units = 'mins')
  tt = round(tt, 2) %>% as.numeric

  msg = paste(
    glue('🕘  {tt} mins'),
    glue('📁  {db} got {out} new files'),
    sep = '\n'
  )

  msg
}
scidb_snbUpdater.transponders <- function(cnf = config::get()) {
  Start = Sys.time()
  cat(" ------> Getting settings ...\n")
  u = cnf$host$dbadmin
  h = cnf$host$name
  p = paste0(cnf$dir$base, cnf$dir$snb)
  y = year(Sys.Date())
  db = cnf$db$snb
  pwd = cnf$host$dbpwd
  bb = cnf$db$snb_boxes
  tdb = cnf$db$transponders
  con = dbConnect(
    RMariaDB::MariaDB(),
    user = u,
    password = pwd,
    host = h,
    dbname = db
  )
  on.exit(dbDisconnect(con))
  cat("db set to", dQuote(db), "transponders db set to", dQuote(tdb), "...OK\n")
  cat(" ------> Getting the file list on ", db, ".b000 ... ")
  box000f = data.table(box = int2b(bb))
  box000f[, `:=`(sql, paste("select distinct path FROM", db, ".", box))]
  box000f = box000f[, dbGetQuery(con, sql), by = box]
  cat("got", nrow(box000f), " files listed in DB.\n")
  cat(" ------> Getting the file list on ", tdb, ".transponders ... ", sep = "")
  transpf = dbGetQuery(
    con,
    paste0("select distinct path from ", tdb, ".transponders")
  )
  setDT(transpf)
  transpf[, `:=`(done, 1)]
  cat("got", nrow(transpf), " files listed in transponders and ")
  newf = merge(box000f, transpf, by = "path", all.x = TRUE)
  newf = newf[is.na(done)]
  cat(nrow(newf), " files to append to transponders ... \n")
  cat(" ------> Running INSERT INTO STATEMENTS for each b000 table ...")
  newf = newf[, .(path = paste(shQuote(path), collapse = ",")), by = box]
  newf[, `:=`(path, paste("(", path, ")"))]
  newf[, `:=`(boxno, b2int(box))]
  newf[, `:=`(
    sql,
    paste(
      paste0(
        "INSERT INTO ",
        tdb,
        ".transponders",
        " (site_type, site, transponder,datetime_, path )"
      ),
      "select 1 site_type,",
      boxno,
      " site, sensor_value transponder, datetime_, path \n                from",
      box,
      "where sensor = 'tra' and path in ",
      path
    )
  )]
  if (nrow(newf) > 0) {
    newf[, `:=`(o, dbExecute(con, sql)), by = box]
  }
  cat(sum(newf$o), "rows inserted into transponders ... \n")
  tt = difftime(Sys.time(), Start, units = "mins") %>% round %>% as.character
  msg = paste(
    glue("🕘  {tt} mins"),
    glue("📁  {tdb}.{db} got { nrow(newf) } new files and {sum(newf$o)} rows."),
    sep = "\n"
  )
  msg
}
BT_at_WESTERHOLZ_change_ID <- function(cnf = config::get()) {
  host <- cnf$host$name
  user <- cnf$host$dbadmin
  pwd <- cnf$host$dbpwd

  con <- dbConnect(
    RMariaDB::MariaDB(),
    user = user,
    password = pwd,
    host = host,
    dbname = "BTatWESTERHOLZ"
  )
  on.exit(dbDisconnect(con))

  d <- dbGetQuery(con, "select * from ID_changes") %>% data.table()

  d <- d[,
    .(
      sql = c(
        paste(
          "UPDATE ADULTS    SET ID       =",
          shQuote(new_ID),
          "WHERE ID       =",
          shQuote(old_ID)
        ),
        paste(
          "UPDATE CHICKS    SET ID       =",
          shQuote(new_ID),
          "WHERE ID       =",
          shQuote(old_ID)
        ),
        paste(
          "UPDATE LAB_ID    SET ID       =",
          shQuote(new_ID),
          "WHERE ID       =",
          shQuote(old_ID)
        ),
        paste(
          "UPDATE MICROSATS SET ID       =",
          shQuote(new_ID),
          "WHERE ID       =",
          shQuote(old_ID)
        ),
        paste(
          "UPDATE SEX       SET ID       =",
          shQuote(new_ID),
          "WHERE ID       =",
          shQuote(old_ID)
        ),
        paste(
          "UPDATE PATERNITY SET father   =",
          shQuote(new_ID),
          "WHERE father   =",
          shQuote(old_ID)
        ),
        paste(
          "UPDATE PATERNITY SET mother   =",
          shQuote(new_ID),
          "WHERE mother   =",
          shQuote(old_ID)
        ),
        paste(
          "UPDATE BREEDING  SET IDmale   =",
          shQuote(new_ID),
          "WHERE IDmale   =",
          shQuote(old_ID)
        ),
        paste(
          "UPDATE BREEDING  SET IDfemale =",
          shQuote(new_ID),
          "WHERE IDfemale =",
          shQuote(old_ID)
        )
      )
    ),
    by = "pk"
  ]

  d[, run := dbExecute(con, sql), by = 1:nrow(d)]

  # when changes were applied then update ID_changes
  pk_timestamp_update <- d[run == 1, ]$pk %>%
    unique() %>%
    paste(., collapse = ",")

  if (nchar(pk_timestamp_update) > 1) {
    dbExecute(
      con,
      paste(
        "UPDATE ID_changes set datetime_db = NOW() where pk in (",
        pk_timestamp_update,
        ")"
      )
    )
  }

  o <- d[run == 1]

  glue("{nrow(o)} ID-s updated.")
}
export_to_mapping.pipeline <- function(
  db_tabs = c("2019_LBDO", "2020_BADO", "2022_WRSA"),
  ...
) {
  sqlitedump(
    db = "ARGOS",
    tables = db_tabs,
    exclude_columns = c(
      "satellite",
      "messageDate",
      "locationClass",
      "compressionIndex",
      "filenam",
      "S1",
      "S2",
      "S3",
      "S4",
      "S5",
      "S6",
      "S7",
      "S8"
    ),
    indices = c("tagID", "locationDate"),
    fun = dup::speed_along,
    ...
  )
}
scidbupdate_mti_gps.BUTEOatEUROPE <- function(cnf = config::get()) {
  Start = Sys.time()

  host = cnf$host$name
  db = cnf$db$gps
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

  # already uploaded
  flist = dbGetQuery(con, 'SELECT DISTINCT filenam from mti_gps')$filenam
  # last uploaded date
  lud = anytime(str_split(flist, "_", simplify = TRUE)[, 2]) %>% max

  # new data
  x = read_email_attachements(
    maildir = 'GSM_MTI',
    pattern = "g_",
    exclude = flist,
    lastdate = lud,
    sepDate = "-"
  )

  if (nrow(x) > 0) {
    # then prepare data & write to DB

    x[Altitude_m == 'Low Voltage', low_voltagge := 1]
    x[Altitude_m == 'Low Voltage', Altitude_m := NA]
    x[,
      tagID := str_split(filenam, '_', simplify = TRUE)[, 1] %>%
        str_replace(., 'g', '') %>%
        as.numeric
    ]

    x = x[, .(
      tagID,
      DateTime,
      Latitude_N,
      Longitude_E,
      Altitude_m,
      HDOP,
      VDOP,
      SatelliteCount,
      low_voltagge,
      filenam
    )]
    setnames(
      x,
      c(
        'tagID',
        'DateTime',
        'latitude',
        'longitude',
        'altitude',
        'HDOP',
        'VDOP',
        'SatelliteCount',
        'low_voltagge',
        'filenam'
      )
    )

    x[, altitude := as.numeric(altitude)]

    n_rows = dbWriteTable(con, 'mti_gps', x, row.names = FALSE, append = TRUE)
  } else {
    n_rows = 0
  }

  tt = difftime(Sys.time(), Start, units = 'mins') %>% round %>% as.character

  msg = paste(
    glue('🕘  {tt} mins'),
    glue('🔄  mti_gps got {n_rows} rows '),
    sep = '\n'
  )

  msg
}
scidbupdate_mti_sensors.BUTEOatEUROPE <- function(cnf = config::get()) {
  Start = Sys.time()

  host = cnf$host$name
  db = cnf$db$gps
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

  # already uploaded
  flist = sdb::dbq(con, 'SELECT DISTINCT filenam from mti_sensors')[[1]]
  # last uploaded date
  lud = anytime(str_split(flist, "_", simplify = TRUE)[, 2]) %>% max

  # new data
  x = read_email_attachements(
    maildir = 'GSM_MTI',
    pattern = "e_",
    exclude = flist,
    lastdate = lud,
    sepDate = "-"
  )

  if (nrow(x) > 0) {
    # then prepare data & write to DB

    x[,
      tagID := str_split(filenam, '_', simplify = TRUE)[, 1] %>%
        str_replace(., 'e', '') %>%
        as.numeric
    ]

    setcolorder(x, c(6, 1:5))
    setnames(
      x,
      c(
        'tagID',
        'DateTime',
        'temperature',
        'BatteryVoltage',
        'ActivityCount',
        'filenam'
      )
    )

    n_rows = dbWriteTable(
      con,
      'mti_sensors',
      x,
      row.names = FALSE,
      append = TRUE
    )
  } else {
    n_rows = 0
  }

  tt = difftime(Sys.time(), Start, units = 'mins') %>% round %>% as.character

  msg = paste(
    glue('🕘  {tt} mins'),
    glue('🔄  mti_sensors got {n_rows} rows '),
    sep = '\n'
  )

  msg
}
int2b <- function(x) {
  paste0("b", str_pad(x, 3, "left", pad = "0"))
}
b2int <- function(x) {
  str_remove(x, "b") %>%
    as.integer()
}
snbstring2date_v2 <- function(x) {
  o <- str_extract(x, "(^20\\d{2})(\\d{2})(\\d{2})-(\\d{6})\\.(\\d{3})")
  if (any(is.na(o))) {
    o <- str_extract(x, "(^20\\d{2})(\\d{2})(\\d{2})-(\\d{6})")
  }
  strptime(o, "%Y%m%d-%H%M%OS") %>% as.POSIXct()
}
