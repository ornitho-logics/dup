#' Convert RW2 photos to WebP
#' @param width output width in px default to 2000
#' @param src output file name
#' @param dest output file name
#' @param resizefact resize factor (default to 1, 2 = half the size of the original image)
#' @param cropamount crop amount (default to 5)
#' @param writethumb write thumbnail (default to TRUE)
#' @export
rw2webp <- function(
  src,
  dest,
  resizefact = 1,
  cropamount = 5,
  writethumb = TRUE
) {
  tif = str_replace(src, "RW2$", "tiff")
  jpg = str_replace(src, "RW2$", "jpeg")

  on.exit({
    file.remove(tif)
    file.remove(jpg)
  })

  glue("dcraw -a -h -T {src}") |> system()

  im = load.image(tif)
  im = resize(im, round(width(im) / resizefact), round(height(im) / resizefact))
  im = crop.borders(im, round(width(im) / 5), round(height(im) / 5))
  save.image(im, file = jpg, quality = 1)

  # save final output to disk
  dirname(dest) |> dir_create()

  jpeg::readJPEG(jpg) |>
    write_webp(dest)

  if (writethumb) {
    tmb_dest = str_replace(dest, "\\.webp", "_thmb.webp")
    save.image(imresize(im, 1 / 5), file = jpg)
    jpeg::readJPEG(jpg) |>
      write_webp(tmb_dest)
  }
}


#' @export
expand_string <- function(x) {
  o <- str_replace(x, "\\-", ":")
  o <- glue("c({o})")
  o <- try(parse(text = o) |> eval(), silent = TRUE)
  if (inherits(o, "try-error")) {
    o <- as.integer(NA)
  }
  as.integer(o)
}
#' @export
mins_taken <- function(x) {
  assert_that(is.time(x))
  o = difftime(Sys.time(), x, units = 'mins') %>% round(digits = 1)
  glue('{o} minutes taken.')
}
#' @export
push_msg <- function(x, title, cnf = config::get('pushover')) {
  x = paste(x, collapse = ' ')
  pushoverr::pushover(
    message = x,
    title = title,
    user = cnf$user,
    app = cnf$app
  )
}
#' argosfilenam2date
#' @export
argosfilenam2date <- function(x, sepDate = "") {
  if (sepDate == "") {
    s = str_extract(x, '(20\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2})')
    o = strptime(s, "%Y%m%d%H%M%S")
  }

  if (sepDate == "-") {
    s = str_extract(x, '(20\\d{2})-(\\d{2})-(\\d{2})')
    o = anytime(s)
  }

  o
}
#' sqlin
#' @description prepare string for select ... where in (1,2,3)
#' @param       s     char vector
#' @export
#' @examples
#' sqlin( 1:3)
sqlin <- function(s) {
  paste(s, collapse = ',') %>%
    paste0('(', ., ')')
}
#' @export
basename2int <- function(ff) {
  basename(ff) %>%
    str_extract("-?\\d+") %>%
    as.integer
}
#' @export
dir_listing <- function(dr) {
  x = data.table(path = list.files(dr, recursive = TRUE, full.names = TRUE))
  o = x[, fs::file_info(path)] |> setDT()
  o = o[, .(path, size = as.character(size), modification_time, birth_time)]
  out = paste0(str_remove(dr, "\\/$"), "_file_listing.csv")
  fwrite(o, file = out)
  out
}
#' @export
dir_size <- function(dr) {
  ff = list.files(dr, all.files = TRUE, recursive = TRUE, full.names = TRUE)
  o = do.call(rbind, lapply(ff, fs::file_info))
  sum(o$size)
}

#' Try outcome
#' @param ... one or several try() values
#' @param message to pass to push_msg
#' @export
try_outcome <- function(..., message) {
  x = list(...)
  gotError = sapply(x, inherits, what = "try-error") |> any()
  if (gotError) {
    push_msg(message, "dup")
  }

  !gotError
}
