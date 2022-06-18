var varint = require('varint')
var bl = Buffer.byteLength

module.exports = function writeFields(row) {
  var len = 0
    + varint.encodingLength(row.id)
    + varint.encodingLength(bl(row.name)) + bl(row.name)
    + 4 + 4 // lon,lat
    + varint.encodingLength(bl(row.countryCode)) + bl(row.countryCode)
    + varint.encodingLength(bl(row.cc2)) + bl(row.cc2)
    + varint.encodingLength(bl(row.admin1)) + bl(row.admin1)
    + varint.encodingLength(bl(row.admin2)) + bl(row.admin2)
    + varint.encodingLength(bl(row.admin3)) + bl(row.admin3)
    + varint.encodingLength(bl(row.admin4)) + bl(row.admin4)
    + varint.encodingLength(row.population)
    + varint.encodingLength(row.elevation)
  var payload = Buffer.alloc(len + varint.encodingLength(len))
  var offset = 0
  offset += writeInt(len, payload, offset)
  offset += writeInt(row.id, payload, offset)
  offset += writeField(row.name, payload, offset)
  offset = payload.writeFloatBE(row.longitude, offset)
  offset = payload.writeFloatBE(row.latitude, offset)
  offset += writeField(row.countryCode, payload, offset)
  offset += writeField(row.cc2, payload, offset)
  offset += writeField(row.admin1, payload, offset)
  offset += writeField(row.admin2, payload, offset)
  offset += writeField(row.admin3, payload, offset)
  offset += writeField(row.admin4, payload, offset)
  offset += writeInt(row.population, payload, offset)
  offset += writeInt(row.elevation, payload, offset)
  return payload
}

function writeField(s, out, offset) {
  var n = 0
  varint.encode(bl(s), out, offset+n)
  n += varint.encode.bytes
  n += out.write(s, offset+n)
  return n
}

function writeInt(n, out, offset) {
  varint.encode(n, out, offset)
  return varint.encode.bytes
}
