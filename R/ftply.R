#' Read, process each block and return a data.table
#'
#' \code{ftply} takes as input the path to a file and a function, and
#' returns a \code{data.table}. It is a faster equivalent to using
#' \code{l <- flply(...)} followed by \code{do.call(rbind, l)}.
#'
#' \code{ftply} is similar to \code{ffply}, but while the latter writes
#' to disk the result of the processing after each block, the former
#' keeps the result in memory until all the file has been processed, and
#' then returns the complete \code{data.table}.
#'
#' @inheritParams flply
#' @param FUN Function to be applied to each block. It must take at least two arguments,
#'     the first of which is a \code{data.table} containing the current block, \emph{without
#'     the first field}; the second argument is a character vector containing the
#'     value of the first field for the current block.
#'
#' @return Returns a \code{data.table} with the results of the
#' processing.
#'
#' @section Slogan:
#' ftply: from \strong{f}ile to data.\strong{t}able
#'
#' @examples
#' f1 <- system.file("extdata", "dt_iris.csv", package = "fplyr")
#'
#' # Compute the mean of the columns for each species
#' ftply(f1, function(d, by) d[, lapply(.SD, mean)])
#'
#' # Read only the first two blocks
#' ftply(f1, nblocks = 2)
#'
#' @export
ftply <- function(input, FUN = function(d, by) d, ...,
                  key.sep = "\t", sep = "\t", skip = 0, header = TRUE,
				  nblocks = Inf, stringsAsFactors = FALSE, colClasses = NULL,
                  select = NULL, drop = NULL, col.names = NULL,
                  parallel = 1) {
    # Open the connections. The input must be binary, so that chunk.reader is
    # happy; the output is handled by data.table's fwrite.
    input <- OpenInput(input, skip)
    head <- GetHeader(input, col.names, header, sep)
	dtstrsplit <- DefineFormatter(sep, colClasses, stringsAsFactors, head, select, drop)
    # on.exit(close(input))

    if (parallel > 1 && .Platform$OS.type != "unix") {
        warning("parallel > 1 is not supported on non-unix systems")
        parallel <- 1
    }

    # Initialise the reader.
    cr <- iotools::chunk.reader(input, sep = key.sep)

    # Parse the file
    fc <- head[1] # first column
    dt <- data.table() # return value
    if (parallel == 1) {
        if (nblocks < Inf) {
            i <- 0
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
                dt <- rbind(dt, d)
                i <- i + min(nblocks - i, length(u))
            }
        } else {
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
                dt <- rbind(dt, d)
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
                if (is.data.table(d) && nrow(d) > 0) {
                    v <- unique(d[[1]])
                    list(d = d, u = u, v = v)
                } else {
                    list(d = NULL, u = u, v = NULL)
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
                dt <- rbind(dt, w$d)
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
                dt <- rbind(dt, w$d)

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
    dt
}
