# This file is run directly in the R session created for it.
# Envirnoment variables needed by bettr are already loaded.
# Use the `task_args` object to get query parameters.
# Have fun!

bettr::get_rows(
  connection_name = "app_dqhi_dev",
  sql = "get_bettr_task_test_before_now"
)

# An identifier to show this task is the one that exited
"task_test_after"