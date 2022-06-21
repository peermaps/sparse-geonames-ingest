#!/usr/bin/env node
var fs = require('fs')
var { Transform, pipeline } = require('stream')
var minimist = require('minimist')

var argv = minimist(process.argv.slice(2), {
  alias: {
    i: 'infile', o: 'outdir',
    h: 'help', v: 'version'
  },
  boolean: [ 'help', 'version' ],
})
if (argv.version) {
  return console.log(require('./package.json').version)
}
if (argv.help || argv.outdir === undefined) return usage()
fs.mkdirSync(argv.outdir, { recursive: true })

if (argv._[0] === 'load') {
  var ingest = require('./')({ outdir: argv.outdir })
  load(ingest, onerror)
} else if (argv._[0] === 'sort') {
  var ingest = require('./')({ outdir: argv.outdir })
  ingest.sort(onerror)
} else if (argv._[0] === 'build') {
  var ingest = require('./')({ outdir: argv.outdir })
  build(ingest, onerror)
} else if (argv._[0] === 'ingest') { // everything
  var ingest = require('./')({ outdir: argv.outdir })
  load(ingest, (err) => {
    if (err) return onerror(err)
    ingest.sort(function (err) {
      if (err) return onerror(err)
      build(ingest, onerror)
    })
  })
} else usage()

function load(ingest, cb) {
  var split = require('split2')
  var instream = argv.infile === undefined || argv.infile === '-'
    ? process.stdin : fs.createReadStream(argv.infile)
  pipeline(instream, split(), new Transform({
    transform: (line, enc, next) => ingest.write(line, next),
    flush: (next) => ingest.flush(next),
  }), cb)
}

function build(ingest, cb) {
  var opts = {}
  if (argv.size) opts.size = argv.size
  if (typeof opts.size === 'string') opts.size = Number(opts.size.replace(/_/g,''))
  ingest.build(opts, cb)
}

function onerror(err) {
  if (err) {
    console.error(err)
    process.exit(1)
  }
}

function usage() {
  console.log(`
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

  `.trim().replace(/^ {4}/mg,'')+'\n')
}
