# 2024-03-22
# We're ready to start building out tests for the new task logic.

# TODO initialize the bettr host

# TODO use `withr` to create some SQL cleanup scripts that remove
# bettr test rows from the bettr host table, rather than scrapping
# the table; that boy's gotta live long and prosper

# TODO populate the bettr task table (see CSV file)

# TODO run the top task in the stack; verify that it was the
# task `task_test_before` and that 1 row was created in its
# newly-minted table

# TODO verify that the updated state of the task in the
# table reflects reality (completed)

# TODO run the top task in the stack; verify that it did
# alter the world as intended

# TODO verify that the updated state of hte task in the
# table reflects reality (completed)

# TODO run the top task in the stack; verify that nothing
# ran, because there are no more jobs

# TODO write out more logic to handle expiry, then write
# tests to verify expiry behavior/constant refresh
# behaves as anticipated