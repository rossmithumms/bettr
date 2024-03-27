# This file is run directly in the R session created for it.
# Envirnoment variables needed by bettr are already loaded.
# Use the `task_args` object to get query parameters.
# Have fun!

bettr::execute_stmts(
  connection_name = "APP_DQHI_DEV",
  sql_file = "get_bettr_task_test_before_now"
)

# TODO something observable in the database with/about that,
# so we can mark a test passed