context("Returning data.table")

file <- system.file("extdata", "dt_iris.csv", package = "fplyr")


test_that("ftply processes identically to data.table", {
    expect_equal(
        ftply(file, FUN = function(d, by) {
                        lapply(d, mean)
        }),
        fread(file, sep = "\t")[, lapply(.SD, mean), by = "Species"]
    )
    expect_identical(
        ftply(file, FUN = function(d, by) {
                        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
        }),
        fread(file, sep = "\t")[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length)), by = "Species"]
    )
})

test_that("parallel ftply processes identically to data.table", {
    expect_equal(
        ftply(file, FUN = function(d, by) {
                        lapply(d, mean)
        }, parallel = 3),
        fread(file, sep = "\t")[, lapply(.SD, mean), by = "Species"]
    )
    expect_identical(
        ftply(file, FUN = function(d, by) {
                        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
        }, parallel = 30),
        fread(file, sep = "\t")[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length)), by = "Species"]
    )
})

test_that("the nblocks option works", {
    expect_equal(
        ftply(file, FUN = function(d, by) {
            lapply(d, mean)
        }, nblocks = 2),
        fread(file, sep = "\t", nrows = 100)[, lapply(.SD, mean), by = "Species"]
    )
    expect_equal(
        ftply(file, FUN = function(d, by) {
            d[Sepal.Length < 5, .N]
        }, nblocks = 1, parallel = 3),
        {
            t <- fread(file, sep = "\t", nrows = 50)[Sepal.Length < 5, .N, by = "Species"]
            names(t) <- c("Species", "V1")
            t
        }
    )
})