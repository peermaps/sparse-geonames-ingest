var fs = require('fs')
var lp = require('length-prefixed-buffers/without-count')

module.exports = Append

function Append(file, opts) {
  var self = this
  if (!(self instanceof Append)) return new Append(file, opts)
  if (!opts) opts = {}
  self.fd = -1
  self._errors = []
  self._flushing = false
  fs.open(file, 'a', function (err, fd) {
    if (err) return self._errors.push(err)
    self.fd = fd
    if (self._fqueue) self.flush()
  })
  self.limit = opts.limit ?? 10_000
  self.queue = []
}

Append.prototype.push = function (buf, cb) {
  this.queue.push(buf)
  if (this.limit > 0 && this.queue.length >= this.limit) {
    this.flush(cb)
  }
}

Append.prototype.flush = function (cb) {
  var self = this
  if (!cb) cb = noop
  if (self._errors.length > 0) cb(self._errors.shift())
  else if (self.fd < 0 && self._fqueue) self._fqueue.push(cb)
  else if (self.fd < 0) self._fqueue = [cb]
  else if (self._flushing && self._fqueue) self._fqueue.push(cb)
  else if (self._flushing) self._fqueue = [cb]
  else {
    self._flushing = true
    fs.write(self.fd, lp.from(self.queue), function (err) {
      self._flushing = false
      var fq = self._fqueue
      self._fqueue = null
      if (fq) {
        for (var i = 0; i < fq.length; i++) {
          var cb = fq[i]
          if (typeof cb === 'function') cb(err)
        }
      }
      cb(err)
    })
    self.queue.length = 0
  }
}

function noop() {}
