var fs = require('fs')
var lp = require('length-prefixed-buffers/without-count')
var lps = require('length-prefixed-stream')
var varint = require('varint')
var { Transform, pipeline } = require('stream')
var nextTick = process.nextTick
var multiSort = require('multi-sort-stream')

module.exports = Append

function Append(file, opts) {
  var self = this
  if (!(self instanceof Append)) return new Append(file, opts)
  if (!opts) opts = {}
  self.fd = -1
  self._errors = []
  self._flushing = false
  self.length = 0
  self._fdqueue = []
  self._flqueue = []
  self._rbuf = Buffer.alloc(opts.readSize ?? 500_000)
  self._roffset = 0
  self.limit = opts.limit ?? 10_000
  self.queue = []
  self.file = file
  fs.stat(file, function (err, s) {
    if (err && err.code !== 'ENOENT') return self._errors.push(err)
    self.length = s ? s.size : 0
    fs.open(file, 'a+', function (err, fd) {
      if (err) return self._errors.push(err)
      self.fd = fd
      var fdq = self._fdqueue
      self._fdqueue = []
      for (var i = 0; i < fdq.length; i++) {
        fdq[i]()
      }
    })
  })
}

Append.prototype.push = function (buf, cb) {
  this.queue.push(buf)
  if (this.limit > 0 && this.queue.length >= this.limit) {
    this.flush(cb)
  } else if (typeof cb === 'function') {
    nextTick(cb, null)
  }
}

Append.prototype.flush = function (cb) {
  var self = this
  if (!cb) cb = noop
  if (self._errors.length > 0) cb(self._errors.shift())
  else if (self.fd < 0) self._fdqueue.push(() => self.flush(cb))
  else if (self._flushing && self._flqueue) self._flqueue.push(cb)
  else {
    self._flushing = true
    var buf = lp.from(self.queue)
    fs.write(self.fd, buf, 0, buf.length, self.length, function (err) {
      self.length += buf.length
      self._flushing = false
      var fq = self._flqueue
      self._flqueue = []
      for (var i = 0; i < fq.length; i++) {
        if (typeof fq[i] === 'function') fq[i](err)
      }
      cb(err)
    })
    self.queue.length = 0
  }
}

Append.prototype.sort = function (opts, cb) {
  var self = this
  if (self.fd < 0) return self._fdqueue.push(() => self.sort(opts, cb))
  if (!opts) opts = {}
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!cb) cb = noop
  var batchSize = opts.batchSize ?? self.limit
  var cmp = opts.compare || defaultCompare
  var file = opts.file || self.file + '.sorted'
  var offsets = []
  var items = []
  var offset = 0, start = 0

  pipeline(
    fs.createReadStream(null, { fd: self.fd }),
    lpsDecode(),
    Transform({ objectMode: true, transform, flush }),
    fs.createWriteStream(file),
    (err) => {
      if (err) return cb(err)
      var streams = []
      var start = 0
      for (var i = 0; i < offsets.length; i++) {
        var end = offsets[i]
        streams.push(pipeline(fs.createReadStream(file, { start, end }), lpsDecode(), onerror))
        start = end
      }
      streams.push(pipeline(fs.createReadStream(file, { start }), lpsDecode(), onerror))
      pipeline(
        multiSort(streams, { compare: cmp }),
        lps.encode(),
        fs.createWriteStream(self.file),
        (err) => {
          if (err) return cb(err)
          fs.rename(file, self.file, cb)
        }
      )
    }
  )

  function transform(buf, enc, next) {
    offset += buf.length + varint.encodingLength(buf.length)
    items.push(buf)
    if (items.length >= batchSize) {
      offsets.push(offset)
      items.sort(cmp)
      var wbuf = lp.from(items)
      items = []
      next(null, wbuf)
    } else next()
  }
  function flush(next) {
    if (items.length > 0) {
      items.sort(cmp)
      var wbuf = lp.from(items)
      items = []
      next(null, wbuf)
    } else {
      offsets.pop()
      next()
    }
  }
  function onerror(err) {
    if (err) {
      var f = cb
      cb = noop
      f(err)
    }
  }
}

function lpsDecode() {
  var offset = 0
  var buffer = null
  return new Transform({
    readableObjectMode: true,
    transform: function (buf, enc, next) {
      buffer = buffer && buffer.length > 0 ? Buffer.concat([buffer,buf]) : buf
      while (offset < buffer.length) {
        try { var len = varint.decode(buffer, offset) }
        catch (e) { return next() }
        if (offset+len >= buffer.length) break
        offset += varint.decode.bytes
        var out = buffer.slice(offset,offset+len)
        offset += len
        this.push(out)
      }
      if (offset === buffer.length) buffer = null
      else buffer = buffer.slice(offset)
      offset = 0
      next()
    }
  })
}

function noop() {}
function defaultCompare(a,b) { return a < b ? -1 : +1 }
