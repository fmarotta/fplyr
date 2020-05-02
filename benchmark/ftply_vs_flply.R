library(fplyr)
library(microbenchmark)

file <- system.file("extdata", "dt_iris.csv", package = "fplyr")

microbenchmark(
    ftply = ftply(file, FUN = function(d, by) {
        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
    }),
    flply = {
        l <- flply(file, FUN = function(d) {
            d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length)), by = "Species"]
        })
        rbindlist(l)
    },
    times = 100
)
