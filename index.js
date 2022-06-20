var fs = require('fs')
var path = require('path')
var varint = require('varint')
var uniq = require('uniq')
var multiSort = require('multi-sort-stream')
var { Transform, pipeline } = require('stream')
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
  this._outdir = opts.outdir
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
  this.records.flush(next)
  this.lookup.flush(next)
  next()
  function next() { if (--pending === 0) cb() }
}

Ingest.prototype.sort = function (cb) {
  if (!cb) cb = noop
  var pending = 3
  this.records.sort({
    batchSize: 50_000,
    compare: (a,b) => {
      var la = varint.decode(a)
      var ida = varint.decode(a, varint.decode.bytes)
      var lb = varint.decode(b)
      var idb = varint.decode(b, varint.decode.bytes)
      return ida < idb ? -1 : +1
    },
  }, done)
  this.lookup.sort({
    batchSize: 50_000,
    compare: (a,b) => {
      var alen = varint.decode(a)
      var astart = varint.decode.bytes
      var blen = varint.decode(b)
      var bstart = varint.decode.bytes
      return bcmp(a, astart, b, bstart)
    },
  }, done)
  done()

  function done() {
    if (--pending !== 0) return
    cb(null)
  }
}

function bcmp(a,ai,b,bi) {
  var m = Math.min(a.length-ai,b.length-bi)
  for (var i = 0; i < m; i++) {
    if (a[ai+i] < b[bi+i]) return -1
    if (a[ai+i] > b[bi+i]) return +1
  }
  return a.length === b.length ? 0 : (a.length < b.length ? -1 : +1)
}

Ingest.prototype.build = function (opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  if (!cb) cb = noop
  var meta = { record: [], lookup: [] }
  var records = [], lookup = []
  var size = 0
  var maxSize = opts.size ?? 100_000
  var rindex = 0, lindex = 0
  var pending = 2

  pipeline(
    this.records.list(),
    Transform({
      writableObjectMode: true,
      transform: function (buf, enc, next) {
        var len = varint.decode(buf)
        var id = varint.decode(buf, varint.decode.bytes)
        if (size + buf.length > maxSize && records.length > 0) {
          var len = varint.decode(buf)
          var id = varint.decode(buf, varint.decode.bytes)
          var prevLen = varint.decode(records[0])
          var prevId = varint.decode(records[0], varint.decode.bytes)
          meta.record.push(id)
          size = 0
          var rfile = path.join(self._outdir, 'r' + String(rindex++))
          var nbuf = Buffer.concat(records)
          records.length = 0
          size = buf.length
          records.push(buf)
          fs.writeFile(rfile, nbuf, next)
        } else {
          size += buf.length
          records.push(buf)
          next()
        }
      },
      flush: function (next) {
        if (records.length > 0) {
          size = 0
          var rfile = path.join(self._outdir, 'r' + String(rindex++))
          var nbuf = Buffer.concat(records)
          records.length = 0
          fs.writeFile(rfile, nbuf, next)
        } else {
          meta.record.pop()
          next()
        }
      }
    }),
    done
  )

  pipeline(
    this.lookup.list(),
    Transform({
      transform: function (buf, enc, next) {
        if (size + buf.length > maxSize && lookup.length > 0) {
          meta.lookup.push(getLKey(lookup[lookup.length-1],buf))
          size = 0
          var lfile = path.join(self._outdir, 'l' + String(lindex++))
          var nbuf = Buffer.concat(lookup)
          lookup.length = 0
          size = buf.length
          lookup.push(buf)
          fs.writeFile(lfile, nbuf, next)
        } else {
          size += buf.length
          lookup.push(buf)
          next()
        }
      },
      flush: function (next) {
        if (lookup.length > 0) {
          size = 0
          var lfile = path.join(self._outdir, 'l' + String(lindex++))
          var nbuf = Buffer.concat(lookup)
          lookup.length = 0
          fs.writeFile(lfile, nbuf, next)
        } else {
          meta.lookup.pop()
          next()
        }
      }
    }),
    done
  )

  function done(err) {
    if (err) {
      cb(err)
      cb = noop
    } else if (--pending === 0) {
      pending = 4
      fs.writeFile(path.join(self._outdir,'meta.json'), JSON.stringify(meta), finish)
      self.records.remove(finish)
      self.lookup.remove(finish)
      finish()
    }
  }
  function finish(err) {
    if (err) {
      cb(err)
      cb = noop
    } else if (--pending === 0) {
      cb(null, meta)
    }
  }
}

function noop() {}

function llcm(a,b) {
  var l = Math.min(a.length,b.length)
  for (var i = 0; i < l && a.charAt(i) === b.charAt(i); i++);
  return b.substr(0,i+1)
}

function getLKey(a, b) {
  return llcm(getKey(a),getKey(b))
}

function getKey(buf) {
  var len = varint.decode(buf)
  var offset = varint.decode.bytes
  return buf.slice(offset,offset+len).toString()
}
