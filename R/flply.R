#' Read, process each block and return a list
#'
#' With \code{flply()} you can apply a function to each block of the file separately.
#' The result of each function is saved into a list and returned. \code{flply()}
#' is similar to \code{lapply()}, except that it applies the function to each
#' block of the file rather than to each element of a list. It is also similar
#' to \code{by()}, except that it does not read the whole file into memory, but
#' each block is processed as soon as it is read from the disk.
#'
#' @inheritParams fdply
#' @param FUN A function to be applied to each block. The first argument to the
#'     function must be a \code{data.table} containing the current block. Additional
#'     arguments can be passed with \code{...}.
#' @param ... Additional arguments to be passed to FUN.
#'
#' @return Returns a list containing, for each chunk, the result of the
#' processing.
#'
#' @section Slogan:
#' fdply: from \strong{f}ile to \strong{l}ist
#'
#' @examples
#' f <- system.file("extdata", "dt_iris.csv", package = "fplyr")
#'
#' # Compute, within each block, the correlation between Sepal.Length and Petal.Length
#' flply(f, function(d) cor(d$Sepal.Length, d$Petal.Length))
#'
#' # Summarise each block
#' flply(f, summary)
#'
#' # Make a different linear model for each block
#' block.lm <- function(d) {
#'   lm(Sepal.Length ~ ., data = d[, !"Species"])
#' }
#' lm.list <- flply(f, block.lm)
#'
#' @export
flply <- function(input, FUN, ...,
                  key.sep = "\t", sep = "\t", skip = 0, header = TRUE,
                  nblocks = Inf, stringsAsFactors = FALSE,
                  select = NULL, drop = NULL, col.names = NULL,
                  parallel = 1) {
    # Prepare the input, find the header and define the formatter.
    input <- OpenInput(input, skip)
    head <- GetHeader(input, col.names, header, sep)
    dtstrsplit <- DefineFormatter(sep, stringsAsFactors, head, select, drop)
    on.exit(close(input))

    if (parallel > 1 && .Platform$OS.type != "unix") {
        warning("parallel > 1is not supported on non-unix systems")
        parallel <- 1
    }

    # Initialise the reader.
    cr <- iotools::chunk.reader(input, sep = key.sep)
    # Parse the file
    i <- 0
    res <- list()
    if (parallel == 1) {
        while (i < nblocks && length(r <- iotools::read.chunk(cr))) {
            d <- dtstrsplit(r)
            u <- unique(d[[1]])
            d <- d[d[[1]] %in% u[1:(min(nblocks - i, length(u)))]]
            l <- by(d, d[, 1], FUN, ...)
            i <- i + length(l)
            res <- append(res, l)
        }
    } else {
        worker_queue = list()
        for (j in 1:max(parallel, 1)) {
            r <- iotools::read.chunk(cr)
            if (length(r) == 0)
                break
            worker_queue[[j]] <- parallel::mcparallel({
                d <- dtstrsplit(r);
                by(d, d[, 1], FUN, ...)
            })
        }
        if (length(worker_queue) == 0)
            return(NULL)
        if (length(r) > 0)
            r <- iotools::read.chunk(cr)
        while (i < nblocks && length(worker_queue)) {
            l <- parallel::mccollect(worker_queue[[1]])[[1]]
            l <- l[1:(min(nblocks - i, length(l)))]
            i <- i + length(l)
            res <- append(res, l)
            worker_queue[1] = NULL
            if (length(r) > 0) {
                worker_queue[[length(worker_queue) + 1]] <- parallel::mcparallel({
                    d <- dtstrsplit(r);
                    by(d, d[, 1], FUN, ...)
                })
                r <- iotools::read.chunk(cr)
            }
        }
    }
    res
}

