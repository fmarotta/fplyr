library(fplyr)
library(microbenchmark)

file <- system.file("extdata", "dt_iris.csv", package = "fplyr")

microbenchmark(
    serial_trivial = ftply(file, FUN = function(d, by) d),
    parallel_trivial = ftply(file, FUN = function(d, by) d, parallel = 3),
    serial_nontrivial = ftply(file, FUN = function(d, by) {
        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
    }),
    parallel_nontrivial = ftply(file, FUN = function(d, by) {
        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
    }, parallel = 3),
    times = 100
)

microbenchmark(
    serial_trivial = flply(file, FUN = function(d, by) d),
    parallel_trivial = flply(file, FUN = function(d, by) d, parallel = 3),
    serial_nontrivial = flply(file, FUN = function(d, by) {
        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
    }),
    parallel_nontrivial = flply(file, FUN = function(d, by) {
        d[Sepal.Length < 5, .(Petal.Sum = sum(Petal.Length))]
    }, parallel = 3),
    times = 100
)
