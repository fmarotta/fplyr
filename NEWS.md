# fplyr 1.3.0

* Do not limit the size of a block to 2^31B, but let it be virtually unlimited
* Use the new way of documenting fplyr-package (see [r-lib/roxygen2#1491](https://github.com/r-lib/roxygen2/issues/1491))
* Use a faster implementation if `nblocks` is not used
* ffply() and fmply() return NULL invisibly rather than the number of blocks processed

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
