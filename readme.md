# sparse-geonames-ingest

scripts to generate sparse p2p archives for [geonames data](https://download.geonames.org/export/dump/)

to search the data generated by this tool, use [sparse-geonames-search][]

Sorting and loading results are written to disk to save memory.

This tool is capable of building an archive from `allCountries.txt`, a 1.5GB archive (as of 2022)
with all geonames data. On my laptop this took 38 minutes and used several gigabytes of RAM.
`cities500.txt` takes 10 seconds.

[sparse-geonames-search]: https://github.com/peermaps/sparse-geonames-search

# usage

```
usage: sparse-geonames-ingest COMMAND {OPTIONS}

sparse-geonames-ingest ingest {-i INFILE} -o OUTDIR

  Load, sort, and build in one command.

sparse-geonames-ingest load {-i INFILE} -o OUTDIR

  Read geonames newline-delimited text file from INFILE or "-" (stdin),
  writing output to OUTDIR.

sparse-geonames-ingest sort -o OUTDIR

  Sort loaded data from OUTDIR, writing results to OUTDIR.

sparse-geonames-ingest build -o OUTDIR (--size=SIZE)

  Build sorted data from OUTDIR, writing results to OUTDIR.
  Each file in the output is at most SIZE bytes.

```

# api

``` js
var Ingest = require('sparse-geonames-ingest')
```

## var ingest = Ingest(opts)

* opts.outdir - directory to use for output and temporary storage
* opts.recordsLimit - number of records to buffer before flushing to disk
* opts.lookupLimit - number of lookups to buffer before flushing to disk
* opts.limit - number of records and lookups to buffer before flushing to disk

## ingest.write(line, cb)

Process one line of input from a geonames text file, calling `cb(err)` when complete.

## ingest.flush(cb)

Flush buffered records in memory to disk, calling `cb(err)` when complete.

## ingest.sort(cb)

Sort data after lines have been written, calling `cb(err)` when complete.

## ingest.build(opts={}, cb)

Build data after sorting into many files, calling `cb(err)` when complete.

* opts.size - maximum size in bytes for each built file. default `10_000`

# install

```
npm install sparse-geonames-ingest
```

# license

bsd

