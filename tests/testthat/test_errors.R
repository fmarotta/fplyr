file <- system.file("extdata", "dt_iris.csv", package = "fplyr")


test_that("fplyr handles errors", {
    expect_error(
        ftply(file, FUN = function(d, by) {
            stop("Simulation of error")
            d
        })
    )
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
    }) |> expect_identical(
        fread(file)[Species != "versicolor", ]
    ) |> expect_message(
        "I hate versicolor"
    ) |> expect_warning(
        "Block versicolor returned an empty data.table."
    )
    ftply(file, FUN = function(d, by) {
        NULL
    }) |> expect_identical(
        data.table()
    ) |> expect_warning(
        "Block setosa returned an empty data.table."
    ) |> expect_warning(
        "Block versicolor returned an empty data.table."
    ) |> expect_warning(
        "Block virginica returned an empty data.table."
    )
})

