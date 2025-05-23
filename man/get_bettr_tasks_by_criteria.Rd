% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/task.R
\name{get_bettr_tasks_by_criteria}
\alias{get_bettr_tasks_by_criteria}
\title{Inspect Tasks on the Host}
\usage{
get_bettr_tasks_by_criteria(bettr_tasks)
}
\arguments{
\item{bettr_tasks}{Required.  A tibble with
one or more row of query criteria.  All values
are optional, but at least one must be provided.
Accepted filter columns and their match method:
\itemize{
\item \code{bettr_task_git_project}: Regex pattern.
\item \code{bettr_task_job_comment}: Regex pattern.
\item \code{bettr_task_name}: Regex pattern.
\item \code{opt_start_dt}: LTE.
\item \code{opt_end_dt}: GTE.
\item \code{opt_cache_expiry_mins}: Negative/zero or positive.
\item \code{opt_number_list}: Regex pattern.
\item \code{opt_char_list}: Regex pattern.
\item \code{last_status}: Regex pattern.
\item \code{last_error}: Regex pattern.
}}
}
\description{
DO NOT USE THIS FUNCTION UNLESS YOU ARE EXTREMELY
SURE YOU KNOW WHAT YOU ARE DOING.  It is possible
to lock the state of the BETTR_HOST, causing
all data extract jobs on the host to stop.  If you
don't know how to avoid that, avoid this function.
}
\details{
Returns a list of tasks on the BETTR_HOST that
meet the criteria given by \code{bettr_tasks}.  This
can have multiple rows; each row queries the
BETTR_HOST separately.  Results of each execution
are returned in a union.
}
