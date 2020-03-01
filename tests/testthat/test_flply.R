context("List Health")

file <- system.file("extdata", "dt_iris.csv", package = "fplyr")

test_that("flply reads identically to fread", {
    expect_identical(
        fdply(file, nblocks = 4),
        fread(file, sep = "\t")
    )
})
