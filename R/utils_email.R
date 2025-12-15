#' offlineimap
#' @param  cnf  configuration variables are obtained from an external file config file.
#'         default to config::get().
#' @param  maildir       maildir
#' @return NULL
#' @export

offlineimap <- function(cnf = config::get(), maildir) {
  accounts = cnf$email$accounts
  localfolders = glue('{cnf$dir$base}{cnf$dir$email}')
  remotehost = cnf$email$host
  remoteuser = cnf$email$user
  pwd = cnf$email$pwd

  tf = tempfile()
  on.exit(file.remove(tf))
  Cat = function(...) {
    cat(..., file = tf, sep = '', append = TRUE, fill = TRUE)
  }

  # TEMP CONFIG FILE
  localrepository = paste0(accounts, 'loc')
  remoterepository = paste0(accounts, 'rem')

  Cat('[general]')
  Cat('accounts=', accounts)

  Cat('[Account ', accounts, ']')
  Cat('localrepository=', localrepository)
  Cat('remoterepository=', remoterepository)

  Cat('[retriever]')
  Cat('type=SimpleIMAPSSLRetriever')

  Cat('[Repository ', localrepository, ']')
  Cat('type=Maildir')
  Cat('localfolders=', localfolders)

  Cat('[Repository ', remoterepository, ']')
  Cat('type=IMAP')
  Cat('remotehost=', remotehost)
  Cat('remoteuser=', remoteuser)
  Cat('remotepass=', pwd)
  Cat('ssl=yes')
  Cat('sslcacertfile=/etc/ssl/certs/ca-certificates.crt')

  # file.edit(tf)

  call = paste('offlineimap -f', shQuote(paste0('INBOX/', maildir)), '-c', tf)
  system(call)
}


#' extract_email_attachements to their dirs AFTER calling offlineimap
#' @param  maildir ARGOS or GSM_MTI
#' @param  onlynew default to TRUE
#' @param  keep default to ".*\\.(TXT|txt)"
#' @param  cnf  configuration variables are obtained from an external file config file.
#'         default to config::get().
#' @return NULL
#' @note   When offlineimap() is called independently then, call extract_email_attachements(onlynew = FALSE) once so that
#'          no new emails are disregarded.
#' @export
#' @examples
#' \dontrun{
#'  dup::extract_email_attachements(maildir = 'ARGOS', onlynew = FALSE)
#'  extract_email_attachements(maildir = 'GSM_MTI', onlynew = FALSE)
#'
#' }

extract_email_attachements <- function(
  maildir,
  onlynew = TRUE,
  keep = ".*\\.(TXT|txt|csv|CSV)",
  cnf = config::get()
) {
  sourcedir = glue('{cnf$dir$base}{cnf$dir$email}INBOX.{maildir}')
  targetdir = glue('{cnf$dir$base}{cnf$dir$emailAtt}{maildir}')

  dir.create(targetdir, recursive = TRUE, showWarnings = FALSE)

  # offlineimap [ fetch new e-mails]
  t0 = data.table(
    path = list.files(path = sourcedir, full.names = TRUE, recursive = TRUE),
    new = 0
  )

  offlineimap(maildir = maildir)

  t1 = data.table(
    path = list.files(path = sourcedir, full.names = TRUE, recursive = TRUE)
  )

  o = merge(t0, t1, by = 'path', all.y = TRUE)
  if (onlynew) {
    o = o[is.na(new)]
  }

  # munpack [ extract attachments ]
  o[, munpack_call := paste('munpack -f -C', targetdir, path)]
  if (nrow(o) > 0) {
    o[, system(munpack_call), by = path]
  }

  # unzip
  a = data.table(
    path = list.files(path = targetdir, full.names = TRUE, recursive = TRUE)
  )
  zf = a[str_detect(path, ".zip$")]
  if (nrow(zf) > 0) {
    zf[, unzip(zipfile = path, junkpaths = TRUE, exdir = targetdir), by = path]
  }

  # cleanup unwanted files
  g = list.files(path = targetdir, full.names = TRUE, recursive = TRUE)
  g = g[!str_detect(g, keep)]
  sapply(g, file.remove)
}


#' read_attachements
#' @param  maildir  maildir
#' @param  cnf  configuration variables are obtained from an external file config file.
#'         default to config::get().
#' @param exclude  exclude filenames (if they are in the db already)
#' @param lastdate date of the last included file (date is embedded in the filename)
#' @param pattern  regexp on file name.
#' @param ...     passed to fread
#' @return DT
#' @export
#' @examples
#' x = read_email_attachements(maildir='ARGOS'  , sep = ";")
#' x = read_email_attachements(maildir='GSM_MTI', pattern = "g_")
#' x = read_email_attachements(maildir='GSM_MTI', pattern = "e_")

read_email_attachements <- function(
  maildir,
  exclude,
  pattern,
  lastdate,
  sepDate = "",
  cnf = config::get(),
  ...
) {
  dirloc = glue('{cnf$dir$base}{cnf$dir$emailAtt}{maildir}')

  F = list.files(dirloc, full.names = TRUE)

  if (!missing(pattern) && length(F) > 0) {
    F = F[str_detect(basename(F), pattern = pattern)]
  }
  if (!missing(exclude) && length(F) > 0) {
    F = F[!basename(F) %in% exclude]
  }
  if (!missing(lastdate) && length(F) > 0) {
    F = F[argosfilenam2date(basename(F), sepDate = sepDate) >= lastdate]
  }

  F = na.omit(F)

  if (length(F) > 0) {
    o = foreach(f = F) %do%
      {
        x = fread(f, fill = TRUE, header = TRUE, ...)
        x[, filenam := basename(f)]
        x
      }

    return(rbindlist(o, fill = TRUE))
  } else {
    data.table()
  }
}
