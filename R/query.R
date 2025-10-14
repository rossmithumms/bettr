# Use this file to write up all your R functions to simplify data access.
#' @importFrom rlang .data

# https://www.r-bloggers.com/2019/08/no-visible-binding-for-global-variable/
globalVariables(c("session", "parseQueryString", "bettr_connection_pool"))

#' Get a Database Connection
#' 
#' This function returns a database connection from a simple pool.
#' We expect each R script invocation to use 1 connection at most per data source.
#' 
#' @param connection_name Database connection name; prefix to env vars.
#' @return A DBI connection object
#' @export
get_db_conn <- function(connection_name) {
  connection_name <- toupper(connection_name)
  if (exists("bettr_connection_pool") == FALSE) {
    bettr_connection_pool <<- list()
  }

  if (connection_name %in% names(bettr_connection_pool) == FALSE) {
    bettr_connection_pool[[connection_name]] <<- open_db_conn(
      connection_name = connection_name
    )
  }

  bettr_connection_pool[[connection_name]]
}

#' Open a Database Connection
#'
#' This function opens a database connection.
#' It depends on environment variables following this convention:
#' <CONNECTION_NAME>_CREDS_USR: database user name
#' <CONNECTION_NAME>_CREDS_PSW: database user password
#' <CONNECTION_NAME>_DB_NAME: database connection string
#' (tnsnames.ora name OR Oracle connection string)
#'
#' @param connection_name Database connection name; prefix to env vars.
#' @return A DBI connection object
open_db_conn <- function(connection_name) {
  connection_name <- toupper(connection_name)
  message(stringr::str_glue("+++ opening connection: {connection_name}"))
  ROracle::dbConnect(drv = DBI::dbDriver("Oracle"),
    username = get_db_username(connection_name),
    password = get_db_password(connection_name),
    dbname = get_db_name(connection_name)
  )
}

#' Get the Database Username
#'
#' @param connection_name the snake_case name of the connection.
#' @return The database username for that connection as defined in .Renviron
get_db_username <- function(connection_name) {
  Sys.getenv(paste(toupper(connection_name), "CREDS", "USR", sep = "_"))
}

#' Get the Database Password
#'
#' @param connection_name the snake_case name of the connection.
#' @return The database password for that connection as defined in .Renviron
get_db_password <- function(connection_name) {
  Sys.getenv(paste(toupper(connection_name), "CREDS", "PSW", sep = "_"))
}

#' Get the Database Connection String or TNS Name
#'
#' @param connection_name the snake_case name of the connection.
#' @return The database connection String as defined in .Renviron
get_db_name <- function(connection_name) {
  Sys.getenv(paste(toupper(connection_name), "DB", "NAME", sep = "_"))
}

#' Close Database Connection Pool
#'
#' This function closes all database connections in the pool.
#' @export
close_db_conn_pool <- function() {
  names(bettr_connection_pool) |>
    as.list() |>
    lapply(function(connection_name) {
      message(stringr::str_glue("... closing connection {connection_name}"))
      ROracle::dbDisconnect(bettr_connection_pool[[connection_name]])
    })

  bettr_connection_pool <<- list()
}

#' Get Rows from a Database
#'
#' This function allows for parameterized, batch SELECT queries to an Oracle connection.
#' The SQL statement must be stored in a file located in the following path location:
#' <SQL_DIR>/<CONNECTION_NAME>/<SQL>.sql
#'
#' The query will be called for each row in the args object (a data frame, data.table, 
#' or tibble).  The SQL file may expose bind parameters that will be used through the
#' DBI bind API.  The named bind parameters must be unique, begin with a question mark,
#' and be formatted in snake_case.
#'
#' The SQL_DIR must be provided as an absolute path in an environment variable.
#'
#' By requiring the SQL to be stored outside this file it can be versioned, tested, and
#' audited separately from the R script calling this function.
#'
#' @param binds a data frame, data.table, or tibble with bind parameters.  Query will be called once for each row.
#' @param connection_name The snake_case name of the connection. This is used to retrieve the
#' environment variables to open the connection and locate the SQL query in the SQL_DIR directory.
#' @param sql Name of the SQL file to be used for the query.
#' @param suppress_bind_logging Optionally suppress the logging of bind parameters (defaults to false).
#' @return A tibble of the result set with cleaned column names.
#' @export
get_rows <- function(binds, connection_name, sql, suppress_bind_logging = FALSE) {
  tryCatch(
    {
      bind_colnames <- get_bind_colnames(
        load_sql(
          sql = sql,
          connection_name = connection_name
        )
      )

      if (missing(binds) || length(bind_colnames) == 0) {
        get_rows_nobinds(
          connection_name = connection_name,
          sql = sql
        )
      } else {
        get_rows_binds(
          binds = binds,
          connection_name = connection_name,
          sql = sql,
          suppress_bind_logging = suppress_bind_logging
        )
      }
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      stop(err)
    },

    finally = {
      close_db_conn_pool()
    }
  )
}

#' Get Rows from a Database With Bind Parameters
#'
#' Calls the SELECT query while passing bind parameters.
#' Multiple rows of bind parameters are queried in series,
#' then bound and packaged as a tibble for returning.
#' Delegate function from get_rows().
get_rows_binds <- function(binds, connection_name, sql, suppress_bind_logging = FALSE) {
  tryCatch(
    {
      connection_name <- toupper(connection_name)
      conn <- get_db_conn(connection_name = connection_name)
      sql_statement <- load_sql(sql = sql, connection_name = connection_name)

      binds |>
        dplyr::rename_with(toupper) |>
        dplyr::select(get_bind_colnames(sql_statement)) |>
        as.list() |>
        purrr::list_transpose() |>
        lapply(function(row_binds) {
          message(stringr::str_glue("... querying {connection_name}: {sql}"))
          if (suppress_bind_logging == FALSE) {
            message(paste0(c(
                "... binding: ",
                stringr::str_glue("{names(row_binds)} = {paste(row_binds)}; ")
            )))
          } else {
            message("... binding: <suppressed>")
          }
          rs <- ROracle::dbSendQuery(conn, sql_statement, dplyr::bind_rows(row_binds))
          message("... fetching")
          data <- ROracle::fetch(rs)
          ROracle::dbClearResult(rs)
          message(stringr::str_glue("--- returning {dplyr::count(data)} rows"))
          data
        }) |>
          data.table::rbindlist() |>
          tibble::as_tibble(.name_repair = snakecase::to_snake_case)
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(ROracle::dbGetException(conn))
      stop(err)
    },

    finally = {
      close_db_conn_pool()
    }
  )
}

#' Get Rows from a Database Without Bind Parameters
#' 
#' Calls the SELECT query without passing any bind parameters;
#' no fancy row-wise manipulation to spread the logic out.
#' Delegate function from get_rows().
get_rows_nobinds <- function(connection_name, sql) {
  tryCatch(
    {
      connection_name <- toupper(connection_name)
      conn <- get_db_conn(connection_name = connection_name)
      sql_statement <- load_sql(sql = sql, connection_name = connection_name)

      message(stringr::str_glue("... querying {connection_name}: {sql}"))
      rs <- ROracle::dbSendQuery(conn, sql_statement, tibble::tibble())
      message("... fetching")
      data <- ROracle::fetch(rs)
      ROracle::dbClearResult(rs)
      message(stringr::str_glue("--- returning {dplyr::count(data)} rows"))
      data |>
        tibble::as_tibble(.name_repair = snakecase::to_snake_case)
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(ROracle::dbGetException(conn))
      stop(err)
    },

    finally = {
      close_db_conn_pool()
    }
  )
}

#' Append Rows to a Table
#'
#' Take the passed rows and append them to a table given a connection.
#' If the table does not exist, it is created with `ensure_table()`.
#' Note:
#' * The schema for created tables is expected to be the database username.
#'
#' @param rows a data frame, data.table, or tibble with rows of values to insert
#' @param connection_name The snake_case name of the connection.
#' @param table_name The name of the table to receive data. Any occurrences of
#' `@@@@` in the table name will be replaced by the contents of the environment
#' variable `BETTR_AT_SCOPE` if it contains only alpha characters.
#' @param suppress_bind_logging Optionally suppress the logging of bind parameters (defaults to false).
#' @export
append_rows <- function(rows, connection_name, table_name, suppress_bind_logging = FALSE) {
  tryCatch(
    {
      connection_name <- toupper(connection_name)
      schema <- toupper(get_db_username(connection_name))

      # If the table does not exist yet, create and initialize it
      ensure_table(
        rows = rows,
        connection_name = connection_name,
        table_name = table_name
      )

      # Prepare the table name after we run ensure_table;
      # this allows ensure_table to find the init SQL file
      # with the unprepared name
      table_name <- prepare_table_name(table_name)

      # Append to the table, assuming it exists already
      conn <- get_db_conn(connection_name = connection_name)
      ROracle::dbWriteTable(
        conn = conn,
        name = table_name,
        value = rows |>
          dplyr::mutate(
            key = as.numeric(NA),
            audit_insert_dt = as.Date(NA)
          ),
        schema = schema,
        row.names = FALSE,
        append = TRUE,
        ora.number = FALSE
      )

      ROracle::dbCommit(conn = conn)
      message(stringr::str_glue("+++ appended {rows |> dplyr::count()} rows"))
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message("!!! Error durring append_rows")
      message(err)
      if (exists(conn)) {
        message(ROracle::dbGetException(conn))
      }
      stop(err)
    },

    finally = {
      close_db_conn_pool()
    }
  )
}

#' Ensure a Table Exists for Appending Rows
#'
#' This function ensures a table exists in the named connection for appending rows.
#' Note that this function depends on a SQL file named `create_table_<table_name>.sql`
#' in the connection-specific directory.
#'
#' @param rows Table structure with representative data for the table to create/verify.
#' @param connection_name The snake_case name of the connection.
#' @param table_name The snake_case name of the table to ensure exists in the database.
#' @return Nothing
#' @export
ensure_table <- function(rows, connection_name, table_name) {
  tryCatch(
    {
      connection_name <- toupper(connection_name)
      conn <- get_db_conn(connection_name = connection_name)
      prepared_table_name <- prepare_table_name(table_name)
      schema <- toupper(get_db_username(connection_name))

      # If the table does not exist yet, create it
      table_exists <- exists_table(
        connection_name = connection_name,
        table_name = prepared_table_name
      )

      # message(stringr::str_glue("... table exists: {table_exists}"))
      conn <- get_db_conn(connection_name = connection_name)

      if (table_exists == FALSE) {
        # Create the table with the dbWriteTable API with one row,
        # then delete the row.  This is the simplest way to ensure typing.
        ROracle::dbWriteTable(
          conn = conn,
          name = prepared_table_name,
          value = rows |> dplyr::slice_head(n = 1),
          schema = schema,
          row.names = FALSE,
          override = FALSE,
          append = FALSE
        )

        ROracle::dbSendQuery(
          conn = conn,
          statement = stringr::str_glue(
            stringr::str_glue("DELETE FROM {prepared_table_name}")
          ),
          data.frame(table_name = table_name)
        )

        # Now initialize the table by appending auto-incrementing
        # key columns, indexes, views, etc
        execute_stmts(
          connection_name = connection_name,
          sql = tolower(stringr::str_glue("init_{table_name}"))
        )

        message(stringr::str_glue("+++ initialized table: {prepared_table_name}"))
      } else {
        message(stringr::str_glue("--- table already exists: {prepared_table_name}"))
      }

      TRUE
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message("!!! Error durring ensure_table")
      message(err)
      if (exists(conn)) {
        message(ROracle::dbGetException(conn))
      }
      stop(err)
    },

    finally = {
      close_db_conn_pool()
    }
  )
}

#' Check if a Table Exists
#'
#' Implementing this function here because ROracle::dbExistsTable is being weird.
#'
#' @param connection_name The snake_case name of the connection.
#' @param table_name The snake_case name of the table to drop.
#' @export
exists_table <- function(connection_name, table_name) {
  tryCatch(
    {
      connection_name <- toupper(connection_name)
      conn <- get_db_conn(connection_name = connection_name)
      table_name <- prepare_table_name(table_name)
      bad_characters <- stringr::str_detect(table_name, "[^_A-Za-z0-9]")

      if (bad_characters == TRUE) {
        stop("!!! Invalid characters detected in table name; stopping")
      }

      # If the table does not exist yet, create it
      rs <- ROracle::dbSendQuery(
        conn = conn,
        statement = stringr::str_glue(
          "SELECT TABLE_NAME FROM USER_TABLES WHERE UPPER(TABLE_NAME) = &table_name"
        ),
        data.frame(table_name = table_name)
      )
      result <- ROracle::fetch(rs)
      ROracle::dbClearResult(rs)
      0 < dplyr::count(result)
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message("!!! Error durring exists_table")
      message(err)
      if (exists(conn)) {
        message(ROracle::dbGetException(conn))
      }
      stop(err)
    },

    finally = {
      close_db_conn_pool()
    }
  )
}

#' Drop a Table
#'
#' Drops a table from the database.
#' Note that this function depends on a SQL file named `drop_table_<table_name>.sql`
#' in the connection-specific directory.
#'
#' @param connection_name The snake_case name of the connection.
#' @param table_name The snake_case name of the table to drop.
#' @export
drop_table <- function(connection_name, table_name) {
  tryCatch(
    {
      connection_name <- toupper(connection_name)
      prepared_table_name <- prepare_table_name(table_name)

      table_exists <- exists_table(
        connection_name = connection_name,
        table_name = table_name
      )

      conn <- get_db_conn(connection_name = connection_name)
      if (table_exists == TRUE) {
        execute_stmts(
          connection_name = connection_name,
          sql = tolower(stringr::str_glue("drop_{table_name}"))
        )

        message(stringr::str_glue("+++ dropped table: {prepared_table_name}"))
      } else {
        message(stringr::str_glue("+++ table does not exist: {prepared_table_name}"))
        FALSE
      }
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(ROracle::dbGetException(conn))
      stop(err)
    },

    finally = {
      close_db_conn_pool()
    }
  )
}

#' Execute a Series of SQL Transactions
#'
#' @param connection_name The snake_case name of the connection.
#' @param sql_file the SQL part of the pattern: <SQL_DIR>/<CONNECTION_NAME>/<SQL>.sql
#' @param binds An optional tibble for specifying bind parameters; must have 0 or 1 rows
#' @param suppress_bind_logging Optionally suppress the logging of bind parameters (defaults to false).
#' @return Nothing
#' @export
execute_stmts <- function(binds = tibble::tibble(), connection_name, sql_file,
  suppress_bind_logging = FALSE) {
  tryCatch(
    {
      connection_name <- toupper(connection_name)
      conn <- get_db_conn(connection_name = connection_name)
      binds <- binds |>
        tibble::as_tibble() |>
        dplyr::rename_with(toupper)
      stmts <- tibble::tibble(
        stmt = load_sql(
          sql_file,
          connection_name = connection_name
        ) |>
          stringr::str_replace_all(pattern = "[ \t\r\n]+", replacement = " ") |>
          stringr::str_split(pattern = ";;;") |>
          unlist()
      ) |>
        dplyr::filter(
          !stringr::str_detect(stmt, "^ *$")
        ) |>
        dplyr::pull() |>
        as.list()
      row_bind_ct <- dplyr::count(binds) |> dplyr::pull()

      # Iterate over statements and excute them
      stmts |>
        lapply(function(stmt) {
          message(stringr::str_glue("... executing: {stmt}"))
          # If there are 0 or 1 rows of bind parameters,
          # don't iterate over them; otherwise, iterate over
          # row binds within statements (statement-major)
          if (row_bind_ct < 2) {
            binds <- binds |>
              dplyr::select(get_bind_colnames(stmt))
            # Only log binds if there are any
            if (row_bind_ct == 1) {
              if (suppress_bind_logging == FALSE) {
                message(paste0(c(
                  "... binding: ",
                  stringr::str_glue("{names(binds)} = {paste(binds)}; ")
                )))
              } else {
                message("... binding: <suppressed>")
              }
            }

            ROracle::dbSendQuery(
              conn = conn,
              statement = stmt,
              data = binds
            )

            # Finalize the work
            ROracle::dbCommit(conn = conn)
          } else {
            binds |>
              as.list() |>
              purrr::list_transpose() |>
              lapply(function(row_binds) {
                row_binds <- row_binds |>
                  tibble::as_tibble() |>
                  dplyr::select(get_bind_colnames(stmt))
                if (suppress_bind_logging == FALSE) {
                  message(paste0(
                    c(
                      "... binding: ",
                      stringr::str_glue(
                        "{names(row_binds)} = {paste(row_binds)}; "
                      )
                    )
                  ))
                } else {
                  message("... binding: <suppressed>")
                }
                ROracle::dbSendQuery(
                  conn = conn,
                  statement = stmt,
                  data = row_binds
                )

                # Finalize the work
                ROracle::dbCommit(conn = conn)
              })
          }
        })
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(ROracle::dbGetException(conn))
      stop(err)
    },

    finally = {
      close_db_conn_pool()
    }
  )
}

#' Generate and Save ETL Table Initialization SQL
#'
#' This is sugar to simplify the process of creating initialization SQL files.
#' It depends on the environmental variable SQL_DIR to be set.
#' This function will attempt to generate a SQL file that adds columns, indexes,
#' views, and grants.
#' Note that this is a CLI utility function in the R command console, not meant
#' to be run outside of a developer's console.  No calling this in task R files.
#' Naturally, once the init SQL has been created and tested, it should be
#' added to your project's version control.
#' @param rows Rows of data from an extract that are representative of the data
#' to be stored in the new table and presented in a new view.
#' @param table_name The name of the table.
#' @param connection_name The name of the database connection (schema user).
#' @param index_id_columns If TRUE, columns that end in `_id` will automatically
#' have indexes created on them.  Defaults to TRUE.
#' @param index_columns A vector of column names to be indexed.  If parameter
#' `index_id_columns` = TRUE, this will supplement those columns.
#' @param view_grants The names of database roles to grant SELECT permissions.
#' Defaults to none.
#' @return Nothing.  This function is called for its side effect of writing an
#' initialization SQL file to the hard disk under the path given at the SQL_DIR
#' environment variable.  Defaults to none.
#' @export
generate_init_sql <- function(
  rows,
  table_name,
  connection_name,
  index_id_columns = TRUE,
  index_columns = c(),
  view_grants = c()) {

  sql_file_name <- stringr::str_glue("init_{tolower(table_name)}.sql")
  sql_dir <- Sys.getenv("SQL_DIR")

  # Generate SQL statements for the live (default) table
  sql_init_live <- generate_sql_init_stmts(
    rows = rows,
    table_name = table_name,
    connection_name = connection_name,
    index_id_columns = index_id_columns,
    index_columns = index_columns,
    view_grants = view_grants
  )

  # Generate SQL statements for the archive table
  sql_init_archive <- generate_sql_init_stmts(
    rows = rows,
    table_name = stringr::str_glue("arch_{table_name}"),
    connection_name = connection_name,
    index_id_columns = index_id_columns,
    index_columns = index_columns,
    view_grants = view_grants
  )

  # Generate SQL statements for the union view
  sql_init_union <- generate_sql_init_union_view_stmt(
    rows = rows,
    table_names = c(
      table_name,
      stringr::str_glue("arch_{table_name}")
    ),
    view_name = stringr::str_glue("all_{table_name}"),
    connection_name = connection_name,
    view_grants = view_grants
  )

  init_sql_file <- file(paste(
    sql_dir,
    toupper(connection_name),
    sql_file_name,
    sep = .Platform$file.sep
  ))
  writeLines(
    paste(
      sql_init_live,
      sql_init_archive,
      sql_init_union,
      sep = ""
    ) |>
      stringr::str_replace_all(
        pattern = "(;;;\n)+",
        replacement = ";;;\n"
      )
    ,
    init_sql_file
  )
  close(init_sql_file)
}

#' Generate ETL Initialization SQL
#' 
#' This method helps `bettr::generate_init_sql` by generating the
#' `ALTER TABLE`, `CREATE VIEW` and `GRANT` statements associated
#' with the provided input.
#' Handling this process in a separate function allows the caller
#' to create variants of the requested table, such as archives.
#' @param rows Rows of data from an extract that are representative of the data
#' to be stored in the new table and presented in a new view.
#' @param table_name The name of the table.
#' @param connection_name The name of the database connection (schema user).
#' @param index_id_columns If TRUE, columns that end in `_id` will automatically
#' have indexes created on them.  Defaults to TRUE.
#' @param index_columns A vector of column names to be indexed.  If parameter
#' `index_id_columns` = TRUE, this will supplement those columns.
#' @param view_grants The names of database roles to grant SELECT permissions.
#' Defaults to none.
#' @return String containing the generated SQL statements,
#' delimited by `;;;`.
generate_sql_init_stmts <- function(
  rows,
  table_name,
  connection_name,
  index_id_columns = TRUE,
  index_columns = c(),
  view_grants = c()) {

  table_name <- prepare_table_name(table_name)
  connection_name <- toupper(connection_name)
  schema_name <- get_db_username(connection_name)
  col_names <- rows |> names() |> toupper()
  index_columns <- index_columns |> toupper()

  if (index_id_columns) {
    index_columns <- vctrs::vec_c(
      index_columns,
      col_names[stringr::str_detect(col_names, "_ID$")]
    )
  }

  index_missing <- setdiff(index_columns, col_names)

  if (length(index_missing) > 0) {
    stop(
      stringr::str_glue(
        paste(
          "!!! generate_init_sql_stmts could not index missing columns: ",
          paste0(index_missing, collapse = ", "),
          sep = ""
        )
      )
    )
  }

  alter_table_stmt <- stringr::str_glue(
    "ALTER TABLE {schema_name}.{table_name} ADD (",
    "KEY NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY, ",
    "AUDIT_INSERT_DT DATE DEFAULT ON NULL SYSDATE)"
  )

  index_stmts <- stringr::str_glue(
    "CREATE INDEX IDX_{table_name}_{index_columns} ON ",
    "{schema_name}.{table_name}(\"{tolower(index_columns)}\")"
  ) |>
    paste(collapse = ";;;\n")

  view_stmt <- stringr::str_glue(
    "CREATE OR REPLACE VIEW V_{table_name} (
  {table_name}_KEY,
  ",
    paste0(
      col_names,
      collapse = ",\n  "
    ), ",
  AUDIT_INSERT_DT
) AS (
  SELECT
    KEY,
    \"",
    paste0(
      tolower(col_names),
      collapse = "\",\n    \""
    ),
    "\",
    AUDIT_INSERT_DT
  FROM
    {schema_name}.{table_name}
)")

  view_grant_stmts <- stringr::str_glue(
    "GRANT SELECT ON V_{table_name} TO {toupper(view_grants)}"
  ) |>
    paste0(collapse = ";;;\n")

  paste(
    alter_table_stmt,
    index_stmts,
    view_stmt,
    view_grant_stmts,
    sep = ";;;\n"
  )
}

#' Generate ETL Union View Statement
#' 
#' This method helps `bettr::generate_init_sql` by generating the
#' statement to create a unioned view of two or more replicate tables.
#' This is used to create a unioned view of live and archival data.
#' 
#' @param rows Rows of data from an extract that are representative of the data
#' to be stored in the new table and presented in a new view.
#' @param table_names Vector of table names to combine in the union.
#' @param view_name Name of the union view to create; "V_" prefix will be added.
#' @param connection_name The name of the database connection (schema user).
#' @param view_grants The names of database roles to grant SELECT permissions.
#' Defaults to none.
#' @return String containing the generated SQL statement.
generate_sql_init_union_view_stmt <- function(
  rows,
  table_names,
  view_name,
  connection_name,
  view_grants = c()) {

  table_names <- prepare_table_name(table_names)
  view_name <- prepare_table_name(view_name)
  connection_name <- toupper(connection_name)
  schema_name <- get_db_username(connection_name)
  col_names <- rows |> names() |> toupper()

  # For each table name, select columns and UNION ALL in the view
  view_stmt <- paste0(
    stringr::str_glue("CREATE OR REPLACE VIEW V_{view_name} (
  {view_name}_KEY,
  "),
  paste0(
    col_names,
    collapse = ",\n  "
  ),
  ",
  AUDIT_INSERT_DT
) AS (\n  ",
  paste0("SELECT
    KEY,
    \"",
    paste0(
      tolower(col_names),
      collapse = "\",\n    \""
    ),
    "\",
    AUDIT_INSERT_DT
  FROM
    ", stringr::str_glue("{schema_name}.{table_names}"),
    sep = ""
  ) |>
  paste0(
    collapse = "\n  UNION ALL\n  "
  ), "
)"
  )

  view_grant_stmts <- stringr::str_glue(
    "GRANT SELECT ON V_{view_name} TO {toupper(view_grants)}"
  ) |>
    paste0(collapse = ";;;\n")

  paste(
    view_stmt,
    view_grant_stmts,
    sep = ";;;\n"
  )
}

#' Get URL Query Parameters
#'
#' What it says on the tin.  If the session contains a URL query,
#' this sugar method retrieves it for you.  You can also test by passing
#' a url_query.
#'
#' @param url_query A URL query string to parse (optional).  If not passed, session URL query is used.
#' @return Named list of values from URL query
#' @export
get_url_query <- function(url_query = "") {
  tryCatch({
      if ("" == url_query & exists("session")) {
      url_query <- session$clientData$url_search # nolint
    }

    url_query |> parseQueryString() # nolint
  },

  warning = function(warn) {
    warning(warn)
  },

  error = function(err) {
    stop(err)
  })
}

#' Get the Directory where SQL Queries are Stored
#' 
#' All the functions in this module require that SQL statements are stored in external files.
#' This function refers to the environment variable SQL_DIR and retrieves it for use.
#' 
#' @return The path to the directory where SQL queries are stored.
get_sql_dir <- function(connection_name = "") {
  sql_base_dir <- Sys.getenv("SQL_DIR")
  connection_name <- toupper(connection_name)

  if (connection_name == "BETTR_HOST") {
    sql_base_dir <- system.file(package = "bettr")

    # 2024-03-26: This is insane, but I have to handle it for testing...
    # If "/inst" is detected, that means we're loading from an installed
    # package context; "/inst" files are placed in the package root,
    # so we must strip "/inst" from this directory base.
    # If "/inst" is not detected, that means we're testing from a local
    # source folder via devtools::test(), and we must look inside the
    # "/inst" folder for the SQL we need, thus we add "/inst".
    # Hooray for R package development.
    # if (stringr::str_detect(sql_base_dir, "inst")) {
    #   sql_base_dir <- stringr::str_sub(sql_base_dir, end = -6L)
    # } else {
    #   sql_base_dir <- paste(
    #     sql_base_dir,
    #     "inst",
    #     sep = .Platform$file.sep
    #   )
    # }

    sql_base_dir <- paste(
      sql_base_dir,
      "sql",
      sep = .Platform$file.sep
    )
  }

  sql_dir <- paste(
    sql_base_dir,
    connection_name,
    sep = .Platform$file.sep
  )

  sql_dir
}

#' Load a SQL Statement from an External File
#' 
#' All the functions in this module require that SQL statements are stored in external files.
#' This function loads a SQL statement from a file.
#' 
#' @param sql The snake_case name of the SQL file without the file extension (such as `get_test_data`).
#' @param connection_name The snake_case name of the database connection.
#' @return The SQL statement stored in the file.
load_sql <- function(sql, connection_name) {
  if (!is.na(stringr::str_match(sql, "[^-_@a-zA-Z0-9]"))) {
    stop("!!! SQL file has illegal characters/matches ^[-_@a-zA-Z0-9]; stopping")
  }

  sql_path <- paste(
    get_sql_dir(connection_name = connection_name),
    paste(
      sql,
      "sql",
      sep = "."
    ),
    sep = .Platform$file.sep
  )

  if (!file.exists(sql_path)) {
    stop(stringr::str_glue("!!! SQL file not found: {sql_path}"))
  }

  readr::read_file(file = sql_path) |> prepare_sql()
}

#' Get the Names of Column Binds in a SQL Statement
#' 
#' The query parameters to retrieve data from a database are handled in RORacle as bind parameters.
#' This function scans a SQL statement for ampersand-prefixed, snake_cased bind parameters.
#' 
#' @param sql The SQL statement to scan for bind parameters.
#' @return A character vector of snake_cased bind parameter names to be used in dplyr verbs.
#' @export
get_bind_colnames <- function(sql) {
  sql |>
    stringr::str_match_all("&([a-zA-Z0-9_]+)") |>
    data.frame() |>
    dplyr::pull(.data$X2) |>
    toupper()
}

#' Parse and Sanitize a Table Name
#' 
#' Takes the given table name and prepares it for use in SQL.
#' @param table_name The table name as a string.  Note that table names must
#' begin with an alpha character and only use alphanumerics, _, or the @@@@
#' sequence.  If @@@@ is found in a table name, it will be replaced by
#' the contents of the global variable called 'BETTR_AT_SCOPE' as long as that
#' variable only contains alpha characters.
#' @return The prepared and sanitized table name.
prepare_table_name <- function(table_name) {
  if (any(stringr::str_detect(
    table_name,
    "^[a-zA-Z][_A-Za-z0-9@]+$",
    negate = TRUE
  ))) {
    stop("!!! Bad table name; use A-Z followed by A-Z, 0-9, _, or @@@@")
  }


  toupper(replace_4ats(table_name))
}

#' Parse and Sanitize a SQL Literal
#' 
#' Prepares SQL literals for execution with the ROracle library.
#' This only means that all occurrences of @@@@ in the SQL will be
#' replaced with the contents of the global variable named
#' 'BETTR_AT_SCOPE' as long as that value contains only alpha characters.
#' @param sql SQL literal to search and replace.
#' @return The prepared SQL literal.
prepare_sql <- function(sql) {
  replace_4ats(str = sql, uppercase = TRUE)
}

#' Replace @@@@ Within a String
#' 
#' What it says on the tin.  Will replace all ocurrences of @@@@
#' in the given string with the contents of the global variable named
#' 'BETTR_AT_SCOPE' as long as that value contains only alpha characters.
#' @param str String to search and replace.
#' @param uppercase Boolean to indicate if the name should be uppercased.
#' Defaults to FALSE.
#' @return The prepared string.
replace_4ats <- function(str, uppercase = FALSE) {
  if (any(stringr::str_detect(str, "@@@@"))) {
    replacement <- Sys.getenv("BETTR_AT_SCOPE")

    if (uppercase) {
      replacement <- toupper(replacement)
    }

    if (stringr::str_detect(
      replacement,
      "^[a-zA-Z_]+$",
      negate = TRUE
    )) {
      stop("!!! @@@@ error: env BETTR_AT_SCOPE empty/bad chars (A-Z, _ only)")
    }
    str <- stringr::str_replace_all(
      str,
      "@@@@",
      replacement
    )
  }

  str
}
