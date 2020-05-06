context("Utils: OpenInput, GetHeader, dtstrsplit")

# The utils are the same for all the functions, so we test them only with ftply
file <- system.file("extdata", "dt_iris.csv", package = "fplyr")


test_that("fplyr reads identically to fread", {
    expect_identical(
        ftply(file, FUN = function(d, by) d),
        fread(file, sep = "\t")
    )
    expect_identical(
        ftply(file, FUN = function(d, by) d, header = F),
        fread(file, sep = "\t", header = F)
    )
    expect_identical(
        ftply(file, FUN = function(d, by) d, skip = 2), # header = T by default
        fread(file, sep = "\t", skip = 2, header = T)
    )
    expect_identical(
        ftply(file, FUN = function(d, by) d, skip = 2, header = F),
        fread(file, sep = "\t", skip = 2, header = F)
    )
})


test_that("fplyr reads compressed files", {
    gzf <- tempfile()
    gzfw <- gzfile(gzf, "w")
    write.table(read.table(file, header = T, sep = "\t", stringsAsFactors = F),
                gzf, sep = "\t", quote = F, row.names = F, col.names = T)
    expect_identical(
        ftply(gzf, FUN = function(d, by) d),
        fread(gzf, sep = "\t")
    )
    close(gzfw)
})


test_that("dtstrsplit works", {
    col.names = c("Species", "Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width")
    dtstrsplit <- DefineFormatter(sep = "\t",
                                  colClasses = NULL,
                                  stringsAsFactors = FALSE,
                                  head = col.names,
                                  select = NULL,
                                  drop = NULL)
    crippled_dtstrsplit <- DefineFormatter(sep = "\t",
                                           colClasses = NULL,
                                           stringsAsFactors = FALSE,
                                           head = col.names,
                                           select = NULL,
                                           drop = NULL,
                                           max_length = 3)
    expect_identical(
        crippled_dtstrsplit(iotools::readAsRaw(file)),
        fread(file, header = F, col.names = col.names)
    )
    expect_identical(
        dtstrsplit(iotools::readAsRaw(file)),
        fread(file, header = F, col.names = col.names)
    )
})
