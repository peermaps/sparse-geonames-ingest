var fs = require('fs')
var lp = require('length-prefixed-buffers/without-count')

module.exports = Append

function Append(file, opts) {
  var self = this
  if (!(self instanceof Append)) return new Append(file, opts)
  if (!opts) opts = {}
  self.fd = -1
  self._errors = []
  fs.open(file, 'a', function (err, fd) {
    if (err) return self._errors.push(err)
    self.fd = fd
    if (self._fqueue) {
      self.flush(function (err) {
        for (var i = 0; i < self._fqueue.length; i++) {
          var cb = self._fqueue[i]
          if (typeof cb === 'function') cb(err)
        }
      })
    }
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
  if (!cb) cb = noop
  if (this._errors.length > 0) return cb(this._errors.shift())
  if (this.fd < 0 && this._fqueue) this._fqueue.push(cb)
  else if (this.fd < 0) this._fqueue = [cb]
  else {
    fs.write(this.fd, lp.from(this.queue), cb)
    this.queue.length = 0
  }
}

function noop() {}
