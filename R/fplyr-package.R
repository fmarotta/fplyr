#' fplyr: Read, Process and Write
#'
#' This package provides a set of functions to quickly read files chunk by
#' chunk, apply a function to each chunk, and return the result. It is
#' especially useful when the files to be processed don't fit into the
#' RAM. Familiarity with the \code{data.table} package is essential in order
#' to use \code{fplyr}.
#'
#' @section Definitions:
#' \describe{
#'   \item{Chunked file:}{A delimited file where many contiguous rows have
#'   the same value on the first field. See the example below.}
#'   \item{Block:}{Any portion of the chunked file such that the first field
#'   does not change.}
#'   \item{Chunk:}{Chunks are used internally; they consist of one or more block, but
#'   regular users should not be concerned with them, and can consider chunks
#'   and blocks as synonyms.}
#' }
#'
#' @section Main functions:
#' The main functions are \code{ffply} and \code{flply}. The former writes the processed
#' data into a file, while the latter returns it as a list. The former is also much faster.
#' There is also \code{fdply}, which returns a \code{data.table} and is useful to only read
#' a certain number of chunks from the file (one by default). \code{fmply} is useful
#' when the original file needs to be processed in many ways and each outcome must
#' be written to a different file.
#'
#' @section Note:
#' Throughout the documentation of this package, the word 'file' actually means
#' 'chunked file.'
#'
#' @section Examples:
#' A chunked file may look as follows:
#'
#' |**V1**|**V2**| **V3** |**V4**|
#' |------|------|--------|------|
#' | ID01 | ABC  | Berlin | 0.1  |
#' | ID01 | DEF  | London | 0.5  |
#' | ID01 | GHI  | Rome   | 0.3  |
#' | ID02 | ABC  | Lisbon | 0.2  |
#' | ID02 | DEF  | Berlin | 0.6  |
#' | ID02 | LMN  | Prague | 0.8  |
#' | ID02 | OPQ  | Dublin | 0.7  |
#' | ID03 | DEF  | Lisbon | -0.1 |
#' | ID03 | LMN  | Berlin | 0.01 |
#' | ID03 | XYZ  | Prague | 0.2  |
#'
#' The important thing is that the first field has some contiguous lines that
#' take the same value. The values of the other fields are unimportant. This
#' package is useful to process this kind of files, block by block.
#'
#' @keywords internal
"_PACKAGE"

## usethis namespace: start
#' @import data.table
## usethis namespace: end
NULL
