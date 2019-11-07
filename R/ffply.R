#' Read, process and write.
#'
#' @param input Path of the input file.
#' @param output Path of the output file.
#'
#' @return Returns the number of chunks that were processed. As a side effect,
#'     writes the processed data.table to the output file.#'
#'
#' @export
ffply <- function(input, output = "", FUN, ...,
                  key.sep = "\t", sep = "\t", skip = 0, header = TRUE,
                  nchunks = Inf, stringsAsFactors = FALSE,
                  select = NULL, drop = NULL, col.names = NULL,
                  index = NULL, max.size = 536870912, parallel = 1) {
    # Open the connections. The input must be binary, so that chunk.reader is
    # happy; the output is handled by data.table's fwrite.
    input <- OpenInput(input, skip)
    head <- GetHeader(input, col.names, header, sep)
    dtstrsplit <- DefineFormatter(sep, stringsAsFactors, head, select, drop, index)
    on.exit(close(input))

    # Initialise the reader.
    cr <- iotools::chunk.reader(input, sep = key.sep)

    # Parse the file
    i <- 0 # keep track of the number of chunks parsed
    fc <- head[1] # first column
    if (parallel == 1) {
        while (i < nchunks && length(r <- iotools::read.chunk(cr, max.size = max.size))) {
            # l <- split(dtstrsplit(r), by = eval(fc), keep.by = T)
            # d <- data.table::rbindlist(lapply(l, function(g) {cbind(g[[1]][1], FUN(g[, -1], g[[1]][1], ...))}))
            d <- dtstrsplit(r)[, FUN(.SD, .BY, ...), by = eval(fc)]
            if (is.null(d) || nrow(d) == 0)
                next()
            if (i == 0 && !is.null(names(d))) {
                names(d)[1] <- fc
                data.table::fwrite(d, file = output,
                       sep = sep, quote = FALSE)
            } else {
                data.table::fwrite(d, file = output, append = TRUE,
                       sep = sep, quote = FALSE, col.names = FALSE)
            }
            i <- i + 1
        }
    } else {
        worker_queue = list()
        for (j in 1:max(parallel, 1)) {
            r <- iotools::read.chunk(cr, max.size = max.size)
            if (length(r) == 0)
                break
            worker_queue[[j]] <- parallel::mcparallel({
                # l <- split(dtstrsplit(r), by = eval(fc), keep.by = T)
                # d <- data.table::rbindlist(lapply(l, function(g) {cbind(g[[1]][1], FUN(g[, -1], g[[1]][1], ...))}))
                # names(d)[1] <- fc
                # d
                d <- dtstrsplit(r)[, FUN(.SD, .BY, ...), by = eval(fc)]
                if (is.data.table(d) && nrow(d) > 0)
                    d
                else
                    list()
            })
        }
        if (length(worker_queue) == 0)
            return(NULL)
        if (length(r) > 0)
            r <- iotools::read.chunk(cr, max.size = max.size)
        while (i < nchunks && length(worker_queue)) {
            if (i == 0) {
                data.table::fwrite(parallel::mccollect(worker_queue[[1]])[[1]],
                                   file = output, sep = sep, quote = FALSE)
            } else {
                data.table::fwrite(parallel::mccollect(worker_queue[[1]])[[1]],
                                   file = output, sep = sep, quote = FALSE,
                                   append = TRUE, col.names = FALSE)
            }
            worker_queue[1] = NULL

            if (length(r) > 0) {
                worker_queue[[length(worker_queue) + 1]] <- parallel::mcparallel({
                    # l <- split(dtstrsplit(r), by = eval(fc), keep.by = T)
                    # data.table::rbindlist(lapply(l, function(g) {cbind(g[[1]][1], FUN(g[, -1], g[[1]][1], ...))}))
                    d <- dtstrsplit(r)[, FUN(.SD, .BY, ...), by = eval(fc)]
                    if (is.data.table(d) && nrow(d) > 0)
                        d
                    else
                        list()
                })
                r <- iotools::read.chunk(cr, max.size = max.size)
            }
            i <- i + 1
        }
    }
    invisible(i)
}
