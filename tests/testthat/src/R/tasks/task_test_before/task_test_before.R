# This file is run directly in the R session created for it.
# Envirnoment variables needed by bettr are already loaded.
# Use the `task_args` object to get query parameters.
# Have fun!

bettr::execute_stmts(
  connection_name = "app_dqhi_dev",
  sql_file = "init_bettr_task_test"
)

# The report
tibble::tibble(
  task = "task_test_before",
  measure_01 = 1,
  measure_02 = 0.5,
  mesaure_03 = lubridate::ymd("2024-03-27")
)