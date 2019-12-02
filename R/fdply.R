#' Read the whole file into a data.table
#'
#' @inheritParams ffply
#' @param nblocks The number of blocks to read. A block is different from a chunk in that chunks can contain many blocks.
#'
#' @return A data.table. It is not advised to read a high number of blocks in this way.
#' @export
fdply <- function(input, key.sep = "\t", sep = "\t", skip = 0, header = TRUE,
                  nblocks = 1, stringsAsFactors = FALSE,
                  select = NULL, drop = NULL, col.names = NULL,
                  index = NULL, max.size = 536870912, parallel = 1) {
    l <- flply(input, function(d) d, key.sep = key.sep, sep = sep, skip = skip, header = header,
               nchunks = nblocks, stringsAsFactors = stringsAsFactors,
               select = select, drop = drop, col.names = col.names,
               index = index, max.size = max.size, parallel = parallel)
    l <- l[1:nblocks]
    do.call("rbind", l)
}