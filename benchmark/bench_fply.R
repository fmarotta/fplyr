library(fplyr)
library(microbenchmark)

file <- system.file("extdata", "dt_iris.csv", package = "fplyr")

microbenchmark(
    ffply(file, "/dev/null", FUN = function(d, by) {d}),
    ffply(file, "/dev/null", FUN = function(d, by) {d}, parallel = 3),
    times = 100
)

microbenchmark(
    ffply(file, "/dev/null", FUN = function(d, by) {
        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
    }),
    ffply(file, "/dev/null", FUN = function(d, by) {
        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
    }, parallel = 3),
    times = 100
)

microbenchmark(
    ffply(file, "/dev/null", FUN = function(d, by) {
        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
    }),
    {
        l <- flply(file, FUN = function(d) {
            d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length)), by = "Species"]
        })
        fwrite(do.call("rbind", l), "/dev/null")
    },
    times = 100
)
