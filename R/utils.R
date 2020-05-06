OpenInput <- function(input, skip) {
    # Open the connections. The input must be binary, so that chunk.reader is
    # happy; the output is handled by data.table's fwrite.
    if (missing(input))
        stop("input file cannot be empty.")

    if (is.character(input)) {
        filetype = summary( file(input) )$class
        if (filetype == "gzfile")
            input = gzcon(file(input, "rb"))
        else
            input = file(input, "rb")
        # on.exit(close(input))
    }

    # Skip the specified lines. We do this before reading the header to
    # comply with fread's behaviour.
    if (skip > 0L)
        readLines(input, n = skip)

    input
}

GetHeader <- function(input, col.names, header, sep) {
    # Read the input header. If col.names is specified, it has the
    # highest priority. If col.names is NULL, but header is TRUE, we
    # read the first line of the file. Otherwise, we use the default
    # R header.
    if (!is.null(col.names)) {
        head <- col.names
        # Skip the header
        if (header)
            readLines(input, n = 1)
    } else if (header) {
        head <- strsplit(readLines(input, n = 1), sep)[[1]]
    } else {
        # Read one line to know how many fields there are, then
        # "rewind" such line.
        offset <- seek(input, NA, "start")
        head <- paste0("V", 1:length(strsplit(readLines(input, n = 1), sep)[[1]]))
        seek(input, offset, "start")
    }

    head
}

# NOTE: the maximum chunk size is 4GB, after which rawToChar will
# complain about its lacking support for long vectors.
DefineFormatter <- function(sep, colClasses, stringsAsFactors, head, select, drop,
                            max_length = 2147483648) {
    function(chunk) {
        # Define the fread formatter: it reads the raw chunk and returns a
        # mighty data.table. Inspired by mstrsplit and dstrsplit.
        if (length(chunk) < max_length) {
            fread(rawToChar(chunk), sep = sep, header = FALSE,
                stringsAsFactors = stringsAsFactors, col.names = head,
				colClasses = colClasses, select = select, drop = drop)
		} else {
			fread(paste0(rawToChar(chunk, multiple = TRUE), collapse = ""),
				  sep = sep, header = FALSE,
				  stringsAsFactors = stringsAsFactors, col.names = head,
				  colClasses = colClasses, select = select, drop = drop)
		}
    }
}
