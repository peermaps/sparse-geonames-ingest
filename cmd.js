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
      ingest.flush(function (err) {
        if (err) return next(err)
        //ingest.sort(next)
        next()
      })
    }
  }), onerror)
} else if (argv._[0] === 'sort') {
  var ingest = require('./')({ outdir: argv.outdir })
  ingest.records.sort({ batchSize: 10_000, compare }, function (err) {
    console.log(err)
  })
  function compare(a, b) {
    var ida = varint.decode(a)
    var idb = varint.decode(b)
    return ida < idb ? -1 : +1
  }
}

function onerror(err) { if (err) console.error(err) }
