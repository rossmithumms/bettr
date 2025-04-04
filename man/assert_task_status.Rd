% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/task.R
\name{assert_task_status}
\alias{assert_task_status}
\title{Asserts the Status of a Task on the Host}
\usage{
assert_task_status(bettr_task)
}
\arguments{
\item{bettr_task}{Required.  A partial definition
of a \code{bettr_task} used as a filter for
finding exactly 1 matching task in BETTR_HOST.}
}
\value{
TRUE if a single matching row is found;
throws an error in all other cases.
}
\description{
Looks for exactly 1 task on the BETTR_HOST that
matches the criteria given by \code{bettr_task}.
This is intended to allow tasks to verify that
a different task providing data for this one
has already executed successfully on the host, even if
that task is from a different project.
For more information on this argument, see the
definition for \code{bettr::get_bettr_tasks_by_attributes()}.
Be aware that \code{bettr_task_git_project}, \code{bettr_task_name},
and \code{last_status} are required on \code{bettr_task}.
}
