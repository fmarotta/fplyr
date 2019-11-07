#' fplyr: Read, Process and Write
#'
#' This package provides a set of functions to quickly read files chunk by
#' chunk, apply a function to each chunk, and return the result. It is
#' especially useful when the files to be processed don't fit into the
#' RAM.
#'
#' @section Main functions:
#' The main functions are ffply and flply. The former writes the processed
#' data into a file, while the latter returns it as a list.
#'
#' @docType package
#' @name fplyr
#'
#' @import data.table
NULL