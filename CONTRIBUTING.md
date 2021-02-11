# How to contribute #

Thank you for stopping by. Please read these few small guidelines before
creating an issue or a pull request on godal.


## Reporting issues ##

Bugs, feature requests, and development-related questions should be directed to
our [GitHub issue tracker](https://github.com/airbusgeo/godal/issues).  If
reporting a bug, please try and provide as much context as possible such as
your Go version, GDAL version and anything else that might be relevant to
the bug.  For feature requests, please explain what you're trying to do, and
how the requested feature would help you do that.

## Submitting a patch ##

  1. Patches are to be submitted through pull-requests: https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request

  1. Make sure each group of changes be done in distinct branches in order to
     ensure that a pull request only includes code related to that bug or feature.

  1. Always run `go fmt` on your code before committing it.

  1. Do not squash / force-push your commits inside the pull request branch as
     these tend to mess up the review comments.

  1. As far as possible, the public API exposed by godal should remain backwards
     compatible, meaning that existing code using godal should compile correctly
     when using a newer godal version, and that the resulting behavior of the
     compiled program should be unchanged. godal makes heavy use of
     [optional parameters](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis)
     in its exposed API in order to allow this.

  1. Any changes should almost always be accompanied by tests. Look at some of
     the existing tests if you're unsure how to go about it. Tests should ensure
     that the godal wrapper itself is working correctly, not the underlying GDAL
     library (e.g. the test could check that a particular option has been taken
     into account by gdal, not that gdal has produced correct output for all 
     possible option values)

  1. Pull requests will be automatically tested, vetted and checked for test 
     coverage. You can run the following tools locally before submitting your 
     changes to ensure the checks will pass:
     * `go test ./... -cover`
     * `golangci-lint run --skip-files doc_test.go`

