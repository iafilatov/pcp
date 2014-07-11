pcp
===

Copy a directory tree using a configurable number of parallel processes


Usage
-----

    pcp -p <processes> <dir> <to dir>

Source dir will appear inside of destination dir, for example `pcp -p 5 srcdir dstdir` will produce `dstdir/srcdir/...`
