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
var bl = Buffer.byteLength

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
    var parts = line.toString().split(/\t/)
    var row = {}
    for (var i = 0; i < parts.length; i++) {
      row[fields[i]] = parts[i]
    }
    row.id = Number(row.id)
    row.longitude = Number(row.longitude)
    row.latitude = Number(row.latitude)
    row.population = Number(row.population)
    row.elevation = Number(row.elevation)
    var keys = [
      row.name, row.asciiName, row.countryCode, row.cc2,
      row.admin1, row.admin2, row.admin3, row.admin4
    ]
    keys = keys.concat(row.alternateNames.split(/\s*,\s*/))
    uniq(keys)
    for (var i = 0; i < keys.length; i++) {
      if (keys[i].length === 0) continue
      lookup.push([keys[i].toLowerCase(),row.id])
    }
    records.push([row.id,writeFields(row)])
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
  var rsize = 2_000
  for (var i = 0; i < records.length; i+=rsize) {
    var rfile = path.join(argv.outdir, 'r' + String(j++))
    meta.record.push(records[i][0])
    var buffers = []
    for (var k = i; k < i+rsize && k < records.length; k++) {
      buffers.push(
        Buffer.from(varint.encode(records[i][0])),
        records[k][1]
      )
    }
    fs.writeFileSync(rfile, lp.from(buffers))
  }
  var j = 0
  var lsize = 5_000
  for (var i = 0; i < lookup.length; i+=lsize) {
    var lfile = path.join(argv.outdir, 'l' + String(j++))
    var l0 = lookup[Math.max(0,i-1)][0]
    var l1 = lookup[i][0]
    var l2 = lookup[Math.min(lookup.length-1,i+1)][0]
    meta.lookup.push(diff(l0,l1,l2))
    var buffers = []
    for (var k = i; k < i+lsize && k < lookup.length; k++) {
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

function writeFields(row) {
  var payload = Buffer.alloc(
    varint.encodingLength(bl(row.name)) + bl(row.name)
    + 4 + 4 // lon,lat
    + varint.encodingLength(bl(row.countryCode)) + bl(row.countryCode)
    + varint.encodingLength(bl(row.cc2)) + bl(row.cc2)
    + varint.encodingLength(bl(row.admin1)) + bl(row.admin1)
    + varint.encodingLength(bl(row.admin2)) + bl(row.admin2)
    + varint.encodingLength(bl(row.admin3)) + bl(row.admin3)
    + varint.encodingLength(bl(row.admin4)) + bl(row.admin4)
    + varint.encodingLength(row.population)
    + varint.encodingLength(row.elevation)
  )
  var offset = 0
  offset += writeField(row.name, payload, offset)
  offset = payload.writeFloatBE(row.longitude, offset)
  offset = payload.writeFloatBE(row.latitude, offset)
  offset += writeField(row.countryCode, payload, offset)
  offset += writeField(row.cc2, payload, offset)
  offset += writeField(row.admin1, payload, offset)
  offset += writeField(row.admin2, payload, offset)
  offset += writeField(row.admin3, payload, offset)
  offset += writeField(row.admin4, payload, offset)
  varint.encode(row.population, payload, offset)
  offset += varint.encode.bytes
  varint.encode(row.elevation, payload, offset)
  offset += varint.encode.bytes
  return payload
}

function writeField(s, out, offset) {
  var n = 0
  varint.encode(bl(s), out, offset+n)
  n += varint.encode.bytes
  n += out.write(s, offset+n)
  return n
}
