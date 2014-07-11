pcp
===

Copy a directory tree using a configurable number of parallel processes.

Most likely, you don't need it. If you read from and/or write to a simple HDD, it can only do one thing at a time, more or less. This script was instead written for testing purposes with a special IO backend.


Usage
-----

    pcp -p <processes> <dir> <to dir>

Source dir will appear inside of destination dir, for example `pcp -p 5 srcdir dstdir` will produce `dstdir/srcdir/...`
