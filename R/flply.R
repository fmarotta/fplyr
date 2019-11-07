#' Read, process and return a list.
#'
#' @param input Path of the input file.
#'
#' @return Returns a list containing, for each chunk, the result of the
#' processing.
#'
#' @export
flply <- function(input, FUN, ...,
                  key.sep = "\t", sep = "\t", skip = 0, header = TRUE,
                  nchunks = Inf, stringsAsFactors = FALSE,
                  select = NULL, drop = NULL, col.names = NULL,
                  index = NULL, max.size = 536870912, parallel = 1) {
    # Prepare the input, find the header and define the formatter.
    input <- OpenInput(input, skip)
    head <- GetHeader(input, col.names, header, sep)
    dtstrsplit <- DefineFormatter(sep, stringsAsFactors, head, select, drop, index)
    on.exit(close(input))

    # Initialise the reader.
    cr <- iotools::chunk.reader(input, sep = key.sep)

    # Parse the file
    i <- 0
    res <- list()
    if (parallel == 1) {
        while (i < nchunks && length(r <- iotools::read.chunk(cr))) {
            d <- dtstrsplit(r)
            l <- by(d, d[, 1], FUN, ...)
            if (is.null(l))
                next()
            res <- append(res, l)
            i <- i + 1
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
        while (i < nchunks && length(worker_queue)) {
            res <- append(res, parallel::mccollect(worker_queue[[1]])[[1]])
            worker_queue[1] = NULL
            if (length(r) > 0) {
                worker_queue[[length(worker_queue) + 1]] <- parallel::mcparallel({
                    d <- dtstrsplit(r);
                    by(d, d[, 1], FUN, ...)
                })
                r <- iotools::read.chunk(cr)
            }
            i <- i + 1
        }
    }

    res
}

