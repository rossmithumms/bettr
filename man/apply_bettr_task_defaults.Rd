% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/task.R
\name{apply_bettr_task_defaults}
\alias{apply_bettr_task_defaults}
\title{Apply Default Values to Bettr Task}
\usage{
apply_bettr_task_defaults(bettr_tasks)
}
\value{
The input bettr_tasks with default values
for all unspecified columns or NA values.
}
\description{
Given one or more bettr_tasks, this function takes
default values from \code{get_bettr_task_defaults()} and
fills them in where NAs are stored and/or the column
is missing.
}
