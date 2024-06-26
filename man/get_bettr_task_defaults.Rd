% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/task.R
\name{get_bettr_task_defaults}
\alias{get_bettr_task_defaults}
\title{Defines Default Bettr Task Column Names and Types}
\usage{
get_bettr_task_defaults()
}
\value{
A tibble with column names and values reflecting
the default values for those columns.  Note that several
are from the environment and are thus contextual defaults.
This object is used when populating new tasks to the host,
and it's assumed that the calls to \code{bettr::add_job()} will
either configure those explicitly in their bettr_task tibbles,
or the function will be called in an interactive session where
these values are set before the function is called.

Brief definitions for each column are given below.  Please refer
to function documentation in this file to understand how each
property is interpreted.
\itemize{
\item bettr_task_git_project: Required.  Name of the git repository
where the task R file is stored. (e.g., \code{phlebotomy}).
\item bettr_task_git_branch: Required.  Name of the git branch where
the source code is stored.
\item bettr_task_git_commit: Optional.  Hash identifier of the git
commit matching the task R file.  This is intended for auditing
and validating only.
\item bettr_task_job_comment: Optional.  A freetext field containing
maintenance notes.
\item bettr_task_job_id: Metadata.  A unique numeric identifier of
the job over the tasks.  These functions populate this value
correctly and automatically; it is required in the database,
but are never needed from the user when creating jobs.
\item bettr_task_job_priority: Required.  A number indicating the
job priority; the lower the number, the higher the priority.
\item bettr_task_name: Required.  The name of the R task file to
be run to fulfill the task.  Must be located here in the repo:
<bettr_task_git_project>/src/R/tasks/<bettr_task_name>/<bettr_task_name>.R
\item bettr_task_sort: Optional.  The order in which tasks must be
completed within the job.  If not provided, defaults to the
order that task rows were added.
\item opt_start_dt: Optional.  Parametric "start datetime" for
task R files.  This bounds the scope of ETL work to a specific
range of time, subject to the task's interpretation of that.
\item opt_end_dt:  Optional . See \code{opt_start_dt}.
\item opt_cache_expiry_mins: Optional.  The cache expiry in minutes.
Specifically, any task that completes can be give a value >= 1
here to indicate that the task is only valid for that many
minutes.  After the cache expires, the task is back in the list.
Defaults to -1, indicating the task has no expiry time.
\item opt_number_list: Optional.  Parametric "number list" for task
R files.  Stored in the database as a comma-separated string,
and is subject to that field's storage limits (4000 characters).
\item opt_char_list: Optional.  See \code{opt_number_list}.
\item last_task_started_dt: Metadata.  If this task was ever started,
this field stores the datetime that happened.
\item last_hb_dt: Metadata.  If this task was ever started, this field
stores the last "heartbeat" datetime.  The heartbeat is sent
to the host from the task process frequently while the task is
running.
\item last_status: Metadata.  If this task was ever started, this
stores the last status code of the task, or 0 (Not Started)
for tasks that were never run.
The status codes are:
\itemize{
\item 0: Not Started
\item 10: Started
\item 20: Failed
\item 21: Failed, No Retry
\item 30: Succeeded
}
\item last_error: Metadata.  If this task was ever started and it
generated an error, this field contains a string summary of
the error.  This field should be set each time the last_status
is set.
\item last_report: Metadata.  If this task was ever started,
this stores a task-generated, JSON-formatted "report" object.
The contents of this report are subjective, as tasks can run
arbitrarily complex code, and is intended to provide values
that can be supervised by an automated log interpreter.
}
}
\description{
Defines Default Bettr Task Column Names and Types
}
