file <- system.file("extdata", "dt_iris.csv", package = "fplyr")


test_that("flply processes identically to data.table", {
    expect_equal(
        flply(file, I),
        {
            t <- fread(file, sep = "\t")
            split(t, by = "Species")
        },
        ignore_attr = TRUE
    )
    expect_equal(
        flply(file, summary),
        {
            t <- fread(file, sep = "\t")
            by(t, t$Species, summary)
        },
        ignore_attr = TRUE
    )
})

test_that("parallel flply processes identically to data.table", {
    expect_equal(
        flply(file, FUN = function(d) {
            lapply(d[, -1], mean)
        }, parallel = 3),
        {
            t <- fread(file, sep = "\t")
            by(t, t$Species, function(d) {
                lapply(d[, -1], mean)
            })
        },
        ignore_attr = TRUE
    )
    expect_equal(
        flply(file, FUN = function(d, by) {
            d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
        }, parallel = 3),
        {
            t <- fread(file, sep = "\t")
            by(t, t$Species, function(d) {
                d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
            })
        },
        ignore_attr = TRUE
    )
})

test_that("the nblocks option works", {
    expect_equal(
        flply(file, FUN = function(d, by) {
            d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
        }, nblocks = 3, parallel = 3),
        {
            t <- fread(file, sep = "\t")
            by(t, t$Species, function(d) {
                d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
            })
        },
        ignore_attr = TRUE
    )
    expect_equal(
        flply(file, FUN = function(d) {
            lapply(d[, -1], var)
        }, nblocks = 1),
        {
            t <- fread(file, sep = "\t", nrows = 50)
            by(t, t$Species, function(d) {
                lapply(d[, -1], var)
            })
        },
        ignore_attr = TRUE
    )
})
