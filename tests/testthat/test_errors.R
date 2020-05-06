context("Worst-case scenarios")

file <- system.file("extdata", "dt_iris.csv", package = "fplyr")


test_that("fplyr handles errors", {
    expect_error(
        ftply(file, FUN = function(d, by) {
            stop("Simulation of error")
            d
        })
    )
    expect_identical(
        ftply(file, FUN = function(d, by) {
            tryCatch({
                if (by == "versicolor")
                    stop("I hate versicolor")
                else
                    return(d)
            },
            error = function(e) {
                message(e$message)
                return(NULL)
            })
        }),
        fread(file)[Species != "versicolor", ]
    )
    expect_identical(
        ftply(file, FUN = function(d, by) {
            NULL
        }),
        data.table()
    )
})

