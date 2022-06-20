#!/usr/bin/env node
var fs = require('fs')
var { Transform, pipeline } = require('stream')
var minimist = require('minimist')

var argv = minimist(process.argv.slice(2), {
  alias: { o: 'outdir' }
})
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
}

function load(ingest, cb) {
  var split = require('split2')
  pipeline(process.stdin, split(), new Transform({
    transform: function (line, enc, next) {
      ingest.write(line, next)
    },
    flush: function (next) {
      ingest.flush(next)
    }
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
