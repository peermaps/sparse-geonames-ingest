#!/usr/bin/env node
var fs = require('fs')
var path = require('path')
var pump = require('stream').pipeline
var Transform = require('stream').Transform
var split = require('split2')
var varint = require('varint')
var uniq = require('uniq')
var lp = require('length-prefixed-buffers/without-count')
var minimist = require('minimist')

var argv = minimist(process.argv.slice(2), {
  alias: { o: 'outdir' }
})
fs.mkdirSync(argv.outdir, { recursive: true })

var fields = [
  'id', 'name', 'asciiName', 'alternateNames', 'latitude', 'longitude',
  'featureClass', 'featureCode', 'countryCode', 'cc2',
  'admin1', 'admin2', 'admin3', 'admin4',
  'population', 'elevation', 'dem', 'timezone', 'modificationDate',
]
var records = []
var lookup = []

pump(process.stdin, split(), new Transform({
  transform: function (line, enc, next) {
    var parts = line.toString().split(/\t+/)
    var row = {}
    for (var i = 0; i < parts.length; i++) {
      row[fields[i]] = parts[i]
    }
    row.id = Number(row.id)
    var keys = [
      row.name, row.asciiName, row.countryCode,
      row.admin1, row.admin2, row.admin3, row.admin4
    ].filter(x => x && x.length > 0)
    keys = keys.concat(row.alternateNames.split(/\s*,\s*/))
    uniq(keys)
    for (var i = 0; i < keys.length; i++) {
      lookup.push([keys[i].toLowerCase(),row.id])
    }
    var payload = Buffer.from(row.name) // todo: rest of the fields
    records.push([row.id,payload])
    next()
  },
  flush: function (next) {
    finish()
    next()
  }
}), onerror)

function onerror(err) {
  if (err) console.error(err)
}

function finish() {
  records.sort((a,b) => a[0] < b[0] ? -1 : +1)
  lookup.sort((a,b) => a[0] < b[0] ? -1 : +1)
  var j = 0
  var meta = { record: [], lookup: [] }
  for (var i = 0; i < records.length; i+=10_000) {
    var rfile = path.join(argv.outdir, 'r' + String(j++))
    meta.record.push(records[i][0])
    var buffers = []
    for (var k = i; k < i+10_000 && k < records.length; k++) {
      buffers.push(
        Buffer.from(varint.encode(records[i][0])),
        records[k][1]
      )
    }
    fs.writeFileSync(rfile, lp.from(buffers))
  }
  var j = 0
  for (var i = 0; i < lookup.length; i+=10_000) {
    var lfile = path.join(argv.outdir, 'l' + String(j++))
    var l0 = lookup[Math.max(0,i-1)][0]
    var l1 = lookup[i][0]
    var l2 = lookup[Math.min(lookup.length-1,i+1)][0]
    meta.lookup.push(diff(l0,l1,l2))
    var digits = []
    for (var k = i; k < i+10_000 && k < lookup.length; k++) {
      buffers.push(
        Buffer.from(lookup[k][0]),
        Buffer.from(varint.encode(lookup[k][1]))
      )
    }
    fs.writeFileSync(lfile, lp.from(buffers))
  }
  fs.writeFileSync(path.join(argv.outdir,'meta'), JSON.stringify(meta))
}

function diff(a,b,c) {
  var l = Math.min(a.length,b.length,c.length)
  for (var i = 0; i < l && eq3(a,b,c,i); i++);
  return b.substr(0,i+1)
}

function eq3(a,b,c,i) {
  var ca = a.charAt(i), cb = b.charAt(i), cc = c.charAt(i)
  return ca === cb && ca === cc
}
