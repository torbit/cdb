# cdb

This is a liberal fork of github.com/jbarham/go-cdb. The pkg interface has been changed to be safe for concurrent access, it now requires the use of an iterator to get multiple values from one key.

cdb is a pure [Go](http://golang.org/) package to read and write cdb ("constant database") files.

The cdb file format is a machine-independent format with the following features:

 - *Fast lookups:* A successful lookup in a large database normally takes just two disk accesses. An unsuccessful lookup takes only one.
 - *Low overhead:* A database uses 2048 bytes, plus 24 bytes per record, plus the space for keys and data.
 - *No random limits:* cdb can handle any database up to 4 gigabytes. There are no other restrictions; records don't even have to fit into memory.

See the original cdb specification and C implementation by D. J. Bernstein
at http://cr.yp.to/cdb.html.

## Installation

Assuming you have a working Go environment, installation is simply:

	go get github.com/torbit/cdb

The package documentation can be viewed online at
http://godoc.org/github.com/torbit/cdb
or on the command line by running `go doc github.com/torbit/cdb`

The included self-test program `cdb_test.go` illustrates usage of the package.
