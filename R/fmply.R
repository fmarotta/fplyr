#' Read, process and write to multiple output files
#'
#' Sometimes a file should be processed in many different ways. \code{fmply()}
#' applies a function to each block of the file; the function should return a
#' list of \emph{m} \code{data.table}s, each of which is written to a different
#' output file. Optionally, the function can return a list of \emph{m + 1},
#' where the first \emph{m} elements are \code{data.table}s and are written
#' to the output files, while the last element is returned as in \code{flply()}.
#'
#' @inheritParams ffply
#' @param outputs Vector of \emph{m} paths for the output files.
#' @param FUN A function to apply to each block. Takes as input a \code{data.table}
#'     and optionally additional arguments. It should return a list of length
#'     \emph{m}, the same length as the \code{outputs} vector. The first element
#'     of the list is written to the first output file, the second element of the
#'     list to the second output file, and so on. Besides these \emph{m} \code{data.table}s,
#'     it can return an additional element, which is also returned by \code{fmply()}.
#'
#' @return If \code{FUN} returns \emph{m} elements, \code{fmply()} returns
#' invisibly the number of blocks parsed. If \code{FUN} returns \emph{m + 1}
#' elements, \code{fmply()} returns the list of all the last elements. As a
#' side effect, it writes the first \emph{m} outputs of \code{FUN} to the
#' \code{outputs} files.
#'
#' @section Slogan:
#' fmply: from \strong{f}ile to \strong{m}ultiple files
#'
#' @examples
#'
#' fin <- system.file("extdata", "dt_iris.csv", package = "fplyr")
#' fout1 <- tempfile()
#' fout2 <- ""
#'
#' # Copy the input file to tempfile as it is, and, at the same time, print
#' # a summary to the console
#' fmply(fin, c(fout1, fout2), function(d) {
#'     list(d, data.table(unclass(summary(d))))
#' })
#'
#' fout3 <- tempfile()
#' fout4 <- tempfile()
#'
#' # Use linear and polynomial regression and print the outputs to two files
#' fmply(fin, c(fout3, fout4), function(d) {
#'     lr.fit <- lm(Sepal.Length ~ ., data = d[, !"Species"])
#'     lr.summ <- data.table(Species = d$Species[1], t(coefficients(lr.fit)))
#'     pr.fit <- lm(Sepal.Length ~ poly(as.matrix(d[, 3:5]), degree = 3),
#'                  data = d[, !"Species"])
#'     pr.summ <- data.table(Species = d$Species[1], t(coefficients(pr.fit)))
#'     list(lr.summ, pr.summ)
#' })
#'
#' @export
fmply <- function(input, outputs, FUN, ...,
                  key.sep = "\t", sep = "\t", skip = 0, header = TRUE,
				  nblocks = Inf, stringsAsFactors = FALSE, colClasses = NULL,
                  select = NULL, drop = NULL, col.names = NULL,
                  parallel = 1) {
    # Prepare the input, find the header and define the formatter.
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
    i <- 0
    res <- list()
    if (parallel == 1) {
        while (i < nblocks && length(r <- iotools::read.chunk(cr))) {
            d <- dtstrsplit(r)
            u <- unique(d[[1]])
            d <- d[d[[1]] %in% u[1:(min(nblocks - i, length(u)))]]
            l <- by(d, d[, 1], FUN, ...)
            m <- lapply(seq_along(outputs), function(j) {
                rbindlist(lapply(l, "[[", j))
            })
            if (length(l[[1]]) > length(outputs)) {
                n <- lapply(l, "[[", length(l[[1]]))
                res <- append(res, n)
            }
            if (i == 0) {
                lapply(seq_along(outputs), function(j) {
                    if (is.null(m[[j]]) || ncol(m[[j]]) == 0L)
                        return()
                    if (all(names(m[[j]]) == paste0("V", 1:length(m[[j]])))) {
                        fwrite(m[[j]], file = outputs[j], col.names = FALSE,
                                           sep = sep, quote = FALSE)
                    } else {
                        fwrite(m[[j]], file = outputs[j], col.names = TRUE,
                                           sep = sep, quote = FALSE)
                    }
                })
            } else {
                lapply(seq_along(outputs), function(j) {
                    if (is.null(m[[j]]) || ncol(m[[j]]) == 0L)
                        return()
                    fwrite(m[[j]], file = outputs[j], append = TRUE,
                                       sep = sep, quote = FALSE, col.names = FALSE)
                })
            }
            i <- i + length(l)
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
            m <- lapply(seq_along(outputs), function(j) {
                rbindlist(lapply(l, "[[", j))
            })
            if (length(l[[1]]) > length(outputs)) {
                n <- lapply(l, "[[", length(l[[1]]))
                res <- append(res, n)
            }
            if (i == 0) {
                lapply(seq_along(m), function(j) {
                    if (is.null(m[[j]]) || ncol(m[[j]]) == 0L)
                        return()
                    if (all(names(m[[j]]) == paste0("V", 1:length(m[[j]])))) {
                        fwrite(m[[j]], file = outputs[j], col.names = FALSE,
                                           sep = sep, quote = FALSE)
                    } else {
                        fwrite(m[[j]], file = outputs[j], col.names = TRUE,
                                           sep = sep, quote = FALSE)
                    }
                })
            } else {
                lapply(seq_along(m), function(j) {
                    if (is.null(m[[j]]) || ncol(m[[j]]) == 0L)
                        return()
                    fwrite(m[[j]], file = outputs[j], append = TRUE,
                           sep = sep, quote = FALSE, col.names = FALSE)
                })
            }
            worker_queue[1] = NULL
            i <- i + length(l)

            if (length(r) > 0) {
                worker_queue[[length(worker_queue) + 1]] <- parallel::mcparallel({
                    d <- dtstrsplit(r);
                    by(d, d[, 1], FUN, ...)
                })
                r <- iotools::read.chunk(cr)
            }
        }
    }
    close(input)
    if (length(res))
        res
    else
        invisible(i)
}
