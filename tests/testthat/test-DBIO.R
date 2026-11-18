test_that("db_copy forwards only the requested table names", {
  calls <- new.env()
  calls$tables <- c("observations", "table with spaces")
  calls$restores <- 0L
  cnf <- list(
    src = list(name = "source", dbadmin = "user", dbpwd = "password"),
    dst = list(name = "destination", dbadmin = "user", dbpwd = "password")
  )
  local_mocked_bindings(
    mysqldump = function(...) {
      calls$dump <- list(...)
      path <- tempfile()
      writeLines("-- dump", path)
      path
    },
    mysqlrestore = function(...) {
      calls$restores <- calls$restores + 1L
      NULL
    }
  )

  db_copy("example", "src", "dst", cnf, tables = calls$tables)
  expect_identical(calls$dump$tables, calls$tables)
  expect_identical(calls$restores, 1L)

  db_copy("example", "src", "dst", cnf)
  expect_null(calls$dump$tables)
  expect_identical(calls$restores, 2L)

  local_mocked_bindings(mysqldump = function(...) stop("dump failed"))
  expect_error(db_copy("example", "src", "dst", cnf), "dump failed")
  expect_identical(calls$restores, 2L)
})

test_that("dump quotes arguments and detects failures before compression", {
  skip_on_os("windows")
  bin <- tempfile("dump-bin-")
  dir.create(bin)
  on.exit(unlink(bin, recursive = TRUE), add = TRUE)
  executable <- file.path(bin, "mariadb-dump")
  writeLines(c("#!/bin/sh", "printf '%s\\n' \"$@\"", "exit 0"), executable)
  Sys.chmod(executable, "0755")
  old_path <- Sys.getenv("PATH")
  on.exit(Sys.setenv(PATH = old_path), add = TRUE)
  Sys.setenv(PATH = paste(bin, old_path, sep = .Platform$path.sep))
  args <- list(
    db = "database with spaces", tables = c("table one", "table' two"),
    user = "test", pwd = "$(false) ' special", dir = bin,
    filenam = "output file.sql"
  )
  for (compress in c(FALSE, TRUE)) {
    path <- do.call(mysqldump, c(args, list(compress = compress)))
    con <- if (compress) gzfile(path, "rt") else file(path, "rt")
    output <- readLines(con)
    close(con)
    expect_true(all(c(args$db, args$tables, paste0("--password=", args$pwd)) %in% output))
    expect_false("--databases" %in% output)
  }

  writeLines(c("#!/bin/sh", "echo partial", "exit 7"), executable)
  for (compress in c(FALSE, TRUE)) {
    expect_error(
      do.call(mysqldump, c(args, list(compress = compress))), "exit status 7"
    )
    path <- file.path(bin, paste0(args$filenam, if (compress) ".gz"))
    expect_false(file.exists(path))
  }
  expect_error(
    mysqldump("example", character(), "test", "", dryrun = TRUE),
    "No tables selected"
  )
})
