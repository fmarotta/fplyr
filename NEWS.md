# fplyr 1.2.2

* Do not limit the size of a block to 2^31B, but let it be virtually unlimited

# fplyr 1.2.1

* Remove annoying warning about unused connection
* Add reference to the arXiv paper in `citation("fplyr")`
* Let `fmply()` return invisibly if otherwise the output would be an empty list

# fplyr 1.2.0

* Introduce `ftply()`, to return the output as a `"data.table"`
* Deprecate `fdply()` in favor of `ftply()`
* Add a vignette
* Refurbish the tests
* Add some benchmarks
* Fix typos in the docs
* Allow blocks to be `long` vectors
* Support the `colClasses` option

# fplyr 1.1.0

* All function now support the `nblocks` option
* `fmply()` can both print and return something
* Updated documentation

# fplyr 1.0.0

* Initial release
