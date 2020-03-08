## Resubmission
This is a resubmission adressing the comments of the CRAN maintainers; after the
first submission, but before seeing the comments, I also discovered some
other weaknesses of the package, which I have taken the liberty to adress by
slightly altering the original files. In particular, in this resubmission:

* The unexecutable code in man/fplyr.Rd has been removed; I was using a "not run"
  code section to display textual information. Now this information is displayed
  in a textual section, hopefully solving the problem.
* Some functionalities have been added to fmply().
* A bug has been fixed in flply() and ffply().
* The documentation has been updated accordingly.
* The year in the LICENSE file has been updated to 2020.
* Additional tests have been performed as in the previous submission.
* I am sorry about having altered the code, I will wait at least 1-2 months
  before posting another update.

## First submission
This is the first time that this package is submitted.

## Test environments
* ubuntu 18.04 (R 3.6.2)
* win-builder (oldrelease only, I couldn't submit it for release and devel)
* R-hub (it failed on Windows Server because the package 'iotools' was not available)

## R CMD check results
There were no ERRORs, WARNINGs, or NOTEs, except for the NOTE saying that
the package is new.

## Downstream dependencies
No dependencies exist at this time.
