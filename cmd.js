#!/usr/bin/env node
var fs = require('fs')
var path = require('path')
var pump = require('stream').pipeline
var Transform = require('stream').Transform
var split = require('split2')
var varint = require('varint')
var uniq = require('uniq')
var minimist = require('minimist')
var bl = Buffer.byteLength

var argv = minimist(process.argv.slice(2), {
  alias: { o: 'outdir' }
})
fs.mkdirSync(argv.outdir, { recursive: true })

if (argv._[0] === 'load') {
  var ingest = require('./')({ outdir: argv.outdir })
  pump(process.stdin, split(), new Transform({
    transform: function (line, enc, next) {
      ingest.write(line, next)
    },
    flush: function (next) {
      ingest.flush(next)
    }
  }), onerror)
} else if (argv._[0] === 'sort') {
  var ingest = require('./')({ outdir: argv.outdir })
  ingest.sort(function (err) {
    console.log(err)
  })
} else if (argv._[0] === 'build') {
  var ingest = require('./')({ outdir: argv.outdir })
  var opts = {}
  if (argv.size) opts.size = argv.size
  if (typeof opts.size === 'string') opts.size = Number(opts.size.replace(/_/g,''))
  ingest.build(opts, function (err) {
    console.log(err)
  })
} else if (argv._[0] === 'ingest') { // everything
}

function onerror(err) { if (err) console.error(err) }
