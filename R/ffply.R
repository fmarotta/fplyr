#' Read, process each block and write the result
#'
#' Suppose you want to process each block of a file and the result is again
#' a \code{data.table} that you want to print to some output file. One possible
#' approach is to use \code{l <- flply(...)} followed by \code{do.call(rbind, l)}
#' and \code{fwrite}, but this would be slow. \code{ffply} offers a faster
#' solution to this problem.
#'
#' @inheritParams flply
#' @param FUN Function to be applied to each block. It must take at least two arguments,
#'     the first of which is a \code{data.table} containing the current block, \emph{without
#'     the first field}; the second argument is a character vector containing the
#'     value of the first field for the current block.
#' @param output String containing the path to the output file.
#'
#' @return Returns NULL invisibly. As a side effect,
#'     writes the processed \code{data.table} to the output file.
#'
#' @section Slogan:
#' ffply: from \strong{f}ile to \strong{f}ile
#'
#' @examples
#' f1 <- system.file("extdata", "dt_iris.csv", package = "fplyr")
#' f2 <- tempfile()
#'
#' # Copy the first two blocks from f1 into f2 to obtain a shorter but
#' # consistent version of the original input file.
#' ffply(f1, f2, function(d, by) {return(d)}, nblocks = 2)
#'
#' # Compute the mean of the columns for each species
#' ffply(f1, f2, function(d, by) d[, lapply(.SD, mean)])
#'
#' # Reshape the file, block by block
#' ffply(f1, f2, function(d, by) {
#'     val <- do.call(c, d)
#'     var <- rep(names(d), each = nrow(d))
#'     data.table(Var = var, Val = val)
#' })
#'
#' @export
ffply <- function(input, output = "", FUN, ...,
                  key.sep = "\t", sep = "\t", skip = 0, header = TRUE,
				  nblocks = Inf, stringsAsFactors = FALSE, colClasses = NULL,
                  select = NULL, drop = NULL, col.names = NULL,
                  parallel = 1) {
    # Open the connections. The input must be binary, so that chunk.reader is
    # happy; the output is handled by data.table's fwrite.
    input <- OpenInput(input, skip)
    head <- GetHeader(input, col.names, header, sep)
	dtstrsplit <- DefineFormatter(sep, colClasses, stringsAsFactors, head, select, drop)

    if (parallel > 1 && .Platform$OS.type != "unix") {
        warning("parallel > 1 is not supported on non-unix systems")
        parallel <- 1
    }

    # Initialise the reader.
    cr <- iotools::chunk.reader(input, sep = key.sep)

    # Parse the file
    fc <- head[1] # first column
    if (parallel == 1) {
        if (nblocks < Inf) {
            i <- 0 # keep track of the number of blocks parsed
            while (i < nblocks && length(r <- iotools::read.chunk(cr))) {
                d <- dtstrsplit(r)
                u <- unique(d[[1]])
                d <- d[d[[1]] %in% u[1:(min(nblocks - i, length(u)))]][, FUN(.SD, .BY, ...), by = eval(fc)]
                if (is.null(d) || nrow(d) == 0) {
                    for (k in 1:length(u))
                        if (i + k <= nblocks)
                            warning(paste0("Block ", i + k, " returned an empty data.table."))
                    i <- i + min(nblocks - i, length(u))
                    next()
                }
                v <- unique(d[[1]])
                if (length(v) != length(u)) {
                    for (k in which(! u %in% v))
                        if (i + k <= nblocks)
                            warning(paste0("Block ", i + k, " returned an empty data.table."))
                }
                if (i == 0) {
                    if (all(names(d) == c(fc, paste0("V", 1:(length(d) - 1))))) {
                        fwrite(d, file = output, col.names = FALSE,
                                           sep = sep, quote = FALSE)
                    } else {
                        fwrite(d, file = output, col.names = TRUE,
                                           sep = sep, quote = FALSE)
                    }
                } else {
                    fwrite(d, file = output, append = TRUE,
                           sep = sep, quote = FALSE, col.names = FALSE)
                }
                i <- i + min(nblocks - i, length(u))
            }
        } else {
            header_printed = F
            while (length(r <- iotools::read.chunk(cr))) {
                d <- dtstrsplit(r)
                u <- unique(d[[1]])
                d <- d[, FUN(.SD, .BY, ...), by = eval(fc)]
                v <- unique(d[[1]])
                failed_blocks <- setdiff(u, v)
                for (k in failed_blocks) {
                    warning("Block ", k, " returned an empty data.table.")
                }
                if (is.null(d) || nrow(d) == 0) {
                    next()
                }
                if (!header_printed) {
                    if (all(names(d) == c(fc, paste0("V", 1:(length(d) - 1))))) {
                        fwrite(d, file = output, col.names = FALSE,
                               sep = sep, quote = FALSE)
                    } else {
                        fwrite(d, file = output, col.names = TRUE,
                               sep = sep, quote = FALSE)
                    }
                    header_printed <- TRUE
                } else {
                    fwrite(d, file = output, append = TRUE,
                           sep = sep, quote = FALSE, col.names = FALSE)
                }
            }
        }
    } else {
        worker_queue = list()
        for (j in 1:max(parallel, 1)) {
            r <- iotools::read.chunk(cr)
            if (length(r) == 0)
                break
            worker_queue[[j]] <- parallel::mcparallel({
                d <- dtstrsplit(r)
                u <- unique(d[[1]])
                d <- d[, FUN(.SD, .BY, ...), by = eval(fc)]
                v <- unique(d[[1]])
                if (is.data.table(d) && nrow(d) > 0) {
                    list(d = d, u = u, v = v)
                } else {
                    list(d = NULL, u = u, v = v)
                }
            })
        }
        if (length(worker_queue) == 0)
            return(NULL)
        if (length(r) > 0)
            r <- iotools::read.chunk(cr)
        if (nblocks < Inf) {
            i <- 0
            while (i < nblocks && length(worker_queue)) {
                w <- parallel::mccollect(worker_queue[[1]])[[1]]
                worker_queue[1] = NULL
                if (is.null(w$d) || nrow(w$d) == 0) {
                    for (k in 1:length(w$u))
                        if (i + k <= nblocks)
                            warning(paste0("Block ", i + k, " returned an empty data.table."))
                    i <- i + min(nblocks - i, length(w$u))
                    next()
                }
                w$d <- w$d[w$d[[1]] %in% w$u[1:(min(nblocks - i, length(w$u)))]]
                if (length(w$v) != length(w$u)) {
                    for (k in which(! w$u %in% w$v))
                        if (i + k <= nblocks)
                            warning(paste0("Block ", i + k, " returned an empty data.table."))
                }
                if (i == 0) {
                    if (all(names(w$d) == c(fc, paste0("V", 1:(length(w$d) - 1))))) {
                        fwrite(w$d, file = output, col.names = FALSE,
                                           sep = sep, quote = FALSE)
                    } else {
                        fwrite(w$d, file = output, col.names = TRUE,
                                           sep = sep, quote = FALSE)
                    }
                } else {
                    fwrite(w$d, file = output, append = TRUE,
                                       sep = sep, quote = FALSE, col.names = FALSE)
                }
                i <- i + min(nblocks - i, length(w$u))

                if (length(r) > 0) {
                    worker_queue[[length(worker_queue) + 1]] <- parallel::mcparallel({
                        d <- dtstrsplit(r)
                        u <- unique(d[[1]])
                        d <- d[, FUN(.SD, .BY, ...), by = eval(fc)]
                        if (is.data.table(d) && nrow(d) > 0) {
                            v <- unique(d[[1]])
                            list(d = d, u = u, v = v)
                        } else {
                            list(d = NULL, u = u, v = NULL)
                        }
                    })
                    r <- iotools::read.chunk(cr)
                }
            }
        } else {
            header_printed <- FALSE
            while (length(worker_queue)) {
                w <- parallel::mccollect(worker_queue[[1]])[[1]]
                worker_queue[1] = NULL
                failed_blocks <- setdiff(w$u, w$v)
                for (k in failed_blocks) {
                    warning("Block ", k, " returned an empty data.table.")
                }
                if (is.null(w$d) || nrow(w$d) == 0) {
                    next()
                }
                if (!header_printed) {
                    if (all(names(w$d) == c(fc, paste0("V", 1:(length(w$d) - 1))))) {
                        fwrite(w$d, file = output, col.names = FALSE,
                               sep = sep, quote = FALSE)
                    } else {
                        fwrite(w$d, file = output, col.names = TRUE,
                               sep = sep, quote = FALSE)
                    }
                    header_printed <- TRUE
                } else {
                    fwrite(w$d, file = output, append = TRUE,
                           sep = sep, quote = FALSE, col.names = FALSE)
                }

                if (length(r) > 0) {
                    worker_queue[[length(worker_queue) + 1]] <- parallel::mcparallel({
                        d <- dtstrsplit(r)
                        u <- unique(d[[1]])
                        d <- d[, FUN(.SD, .BY, ...), by = eval(fc)]
                        if (is.data.table(d) && nrow(d) > 0) {
                            v <- unique(d[[1]])
                            list(d = d, u = u, v = v)
                        } else {
                            list(d = NULL, u = u, v = NULL)
                        }
                    })
                    r <- iotools::read.chunk(cr)
                }
            }
        }
    }
    close(input)
    invisible(NULL)
}
