var path = require('path')
var varint = require('varint')
var uniq = require('uniq')
var multiSort = require('multi-sort-stream')
var bl = Buffer.byteLength

var append = require('./lib/append.js')
var writeFields = require('./lib/write-fields.js')

var fields = [
  'id', 'name', 'asciiName', 'alternateNames', 'latitude', 'longitude',
  'featureClass', 'featureCode', 'countryCode', 'cc2',
  'admin1', 'admin2', 'admin3', 'admin4',
  'population', 'elevation', 'dem', 'timezone', 'modificationDate',
]

module.exports = Ingest
function Ingest(opts) {
  if (!(this instanceof Ingest)) return new Ingest(opts)
  if (!opts) opts = {}
  if (typeof opts === 'string') opts = { outdir: opts }
  this.records = append(path.join(opts.outdir,'records.tmp'), { limit: 50_000 })
  this.lookup = append(path.join(opts.outdir,'lookup.tmp'), { limit: 50_000 })
}

Ingest.prototype.write = function (line, cb) {
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
  this.records.push(writeFields(row), cb)

  var keys = [
    row.name, row.asciiName, row.countryCode, row.cc2,
    row.admin1, row.admin2, row.admin3, row.admin4
  ]
  keys = keys.concat(row.alternateNames.split(/\s*,\s*/))
  uniq(keys)
  for (var i = 0; i < keys.length; i++) {
    if (keys[i].length === 0) continue
    this._pushLookup(keys[i].toLowerCase(), row.id, cb)
  }
}

Ingest.prototype._pushLookup = function (key, id) {
  var kl = bl(key)
  var buf = Buffer.alloc(varint.encodingLength(kl) + kl + varint.encodingLength(id))
  var offset = 0
  varint.encode(kl, buf, offset)
  offset += varint.encode.bytes
  buf.write(key, offset)
  offset += kl
  varint.encode(id, buf, offset)
  offset += varint.encode.bytes
  this.lookup.push(buf)
}

Ingest.prototype.flush = function (cb) {
  if (!cb) cb = noop
  var pending = 3
  this.records.push(next)
  this.lookup.push(next)
  next()
  function next() { if (--pending === 0) cb() }
}

Ingest.prototype.sort = function () {
  /*
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
        Buffer.from(varint.encode(records[k][0])),
        records[k][1]
      )
    }
    fs.writeFileSync(rfile, Buffer.concat(buffers))
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
      var n = Buffer.byteLength(lookup[k][0])
      var buf = Buffer.alloc(varint.encodingLength(n)
        + n + varint.encodingLength(lookup[k][1]))
      var offset = 0
      varint.encode(n, buf, offset)
      offset += varint.encode.bytes
      buf.write(lookup[k][0], offset)
      offset += n
      varint.encode(lookup[k][1], buf, offset)
      offset += varint.encode.bytes
      buffers.push(buf)
    }
    fs.writeFileSync(lfile, Buffer.concat(buffers))
  }
  fs.writeFileSync(path.join(argv.outdir,'meta.json'), JSON.stringify(meta))
  */
}

function noop() {}

function diff(a,b,c) {
  var l = Math.min(a.length,b.length,c.length)
  for (var i = 0; i < l && eq3(a,b,c,i); i++);
  return b.substr(0,i+1)
}

function eq3(a,b,c,i) {
  var ca = a.charAt(i), cb = b.charAt(i), cc = c.charAt(i)
  return ca === cb && ca === cc
}
