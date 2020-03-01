context("File Health")

file <- system.file("extdata", "dt_iris.csv", package = "fplyr")

test_that("ffply reads identically to fread", {
    expect_identical(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {return(d)})
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t")
    )
    expect_identical(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {return(d)}, header = F)
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t", header = F)
    )
    expect_identical(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {return(d)}, skip = 2)
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t", skip = 2)
    )
    expect_identical(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {return(d)}, skip = 2, header = F)
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t", skip = 2, header = F)
    )
})

test_that("parallel ffply reads identically to fread", {
    expect_identical(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {return(d)}, parallel = 3)
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t")
    )
    expect_identical(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {return(d)}, header = F, parallel = 3)
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t", header = F)
    )
    expect_identical(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {return(d)}, skip = 2, parallel = 3)
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t", skip = 2)
    )
    expect_identical(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {return(d)}, skip = 2, header = F, parallel = 3)
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t", skip = 2, header = F)
    )
})

test_that("ffply processes identically to data.table", {
    expect_equal(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {
                        lapply(d, mean)
                    })
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t")[, lapply(.SD, mean), by = "Species"]
    )
    expect_identical(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {
                        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
                    })
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t")[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length)), by = "Species"]
    )
})

test_that("parallel ffply processes identically to data.table", {
    expect_equal(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {
                        lapply(d, mean)
                    }, parallel = 3)
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t")[, lapply(.SD, mean), by = "Species"]
    )
    expect_identical(
        fread(
            paste(
                capture.output(
                    ffply(file, FUN = function(d, by) {
                        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
                    }, parallel = 3)
                ),
                collapse = "\n"
            )
        ),
        fread(file, sep = "\t")[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length)), by = "Species"]
    )
})
