#' Read some chunks from a file into a \code{data.table}
#'
#' This function is useful to quickly glance at a big chunked file. It is similar
#' to the \code{head()} function, except that it does not read the first few lines, but
#' rather the first few blocks of the file. By default, only the first block will be read;
#' it is not advisable to read a large number of blocks in this way because they may
#' occupy a lot of memory. The blocks are saved to a \code{data.table}. See \code{?fplyr}
#' for the definitions of chunked file and block.
#'
#' @param input Path of the input file.
#' @param key.sep The character that delimits the first field from the rest.
#' @param sep The field delimiter (often equal to \code{key.sep}).
#' @param skip Number of lines to skip at the beginning of the file
#' @param header Whether the file has a header.
#' @param nblocks The number of blocks to read.
#' @param stringsAsFactors Whether to convert strings into factors.
#' @param colClasses Vector or list specifying the class of each field.
#' @param select The columns (names or numbers) to be read.
#' @param drop The columns (names or numbers) not to be read.
#' @param col.names Names of the columns.
#' @param parallel Number of cores to use.
#'
#' @return A \code{data.table} containing the file truncated to the number of
#' blocks specified.
#'
#' @section Slogan:
#' fdply: from \strong{f}ile to \strong{d}ata.table
#'
#' @export
fdply <- function(input, nblocks = 1, key.sep = "\t", sep = "\t", skip = 0,
				  colClasses = NULL, header = TRUE, stringsAsFactors = FALSE,
                  select = NULL, drop = NULL, col.names = NULL,
                  parallel = 1) {
    .Deprecated("ftply")
    l <- flply(input, function(d) d, key.sep = key.sep, sep = sep, skip = skip, header = header,
			   nblocks = nblocks, colClasses = colClasses,
			   stringsAsFactors = stringsAsFactors,
               select = select, drop = drop, col.names = col.names,
               parallel = parallel)
    rbindlist(l)
}
