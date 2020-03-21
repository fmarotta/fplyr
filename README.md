# fplyr

[![cran version-ago](https://www.r-pkg.org/badges/version-ago/fplyr)](https://www.r-pkg.org/badges/version-ago/fplyr)
[![cran logs](https://cranlogs.r-pkg.org/badges/fplyr)](https://cranlogs.r-pkg.org/badges/fplyr)
[![cran logs last-day](https://cranlogs.r-pkg.org/badges/last-day/fplyr)](https://cranlogs.r-pkg.org/badges/last-day/fplyr)
[![cran logs grand-total](https://cranlogs.r-pkg.org/badges/grand-total/fplyr)](https://cranlogs.r-pkg.org/badges/grand-total/fplyr)

This package combines the power of the `data.table` and `iotools` packages to
efficiently read a large file block by block and, in the meantime, apply a
user-specified function to each block of the file. The outputs can be collected
into a list or printed to an output file.

A 'block' is defined as a set of contiguous lines that have the same value
in the first field. Thus, this package is not intended for all large files,
but rather its usefulness is limited to a particular type of files.

## Examples

A typical file that can be processed with `fplyr` is as follows.

```
V1   V2  V3 V4
ID01 ABC Berlin 0.1
ID01 DEF London 0.5
ID01 GHI Rome   0.3
ID02 ABC Lisbon 0.2
ID02 DEF Berlin 0.6
ID02 LMN Prague 0.8
ID02 OPQ Dublin 0.7
ID03 DEF Lisbon -0.1
ID03 LMN Berlin 0.01
ID03 XYZ Prague 0.2
```

The first block consists of the first three lines, the second block of the next
four lines, and the third and last block is made up of the last three lines.
Suppose you want to compute the mean of the fourth column for each ID in the
first field. If the file is small, you can use `by()`. But if the file is so
big that it does not fit into the available memory, you can use one of the
functions of this package. If the path to the above file is stored in the
variable `f`, the following command returns a list where each element is the
mean of the fourth column for a single block.

```
l <- flply(f, function(d) mean(d$V4))
```
