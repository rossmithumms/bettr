# This file is run directly in the R session created for it.
# Envirnoment variables needed by bettr are already loaded.
# Use the `task_args` object to get query parameters.
# Have fun!

bettr::get_rows(
  connection_name = "app_dqhi_dev",
  sql_file = "get_bettr_task_test_before_now"
)