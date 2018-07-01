var dwDHTRPC = require('@dwdht/rpc')
var dwDhtPeers = require('@dwdht/peers')
var protobuf = require('protocol-buffers')
var equals = require('buffer-equals')
var store = require('./store')
var fs = require('fs')
var inherits = require('inherits')
var events = require('events')
var path = require('path')

var PEERS = new Buffer(1024)
var LOCAL_PEERS = new Buffer(1024)

var messages = protobuf(fs.readFileSync(path.join(__dirname, 'schema.proto')))

module.exports = DwDHT

function DwDHT (opts) {
  if (!(this instanceof DwDHT)) return new DwDHT(opts)
  if (!opts) opts = {}
  events.EventEmitter.call(this)

  if (opts.bootstrap) {
    opts.bootstrap = [].concat(opts.bootstrap).map(parseBootstrap)
  }

  this.dwDHTRPC = dwDHTRPC(opts)
  this.id = this.dwDHTRPC.id
  this.socket = this.dwDHTRPC.socket.socket
  this._store = store()

  var self = this

  this.dwDHTRPC.on('error', function (err) {
    self.emit('error', err)
  })

  this.dwDHTRPC.on('query:peers', function (data, cb) {
    self._onquery(data, false, cb)
  })

  this.dwDHTRPC.on('update:peers', function (data, cb) {
    self._onquery(data, true, cb)
  })
}

inherits(DwDHT, events.EventEmitter)

DwDHT.prototype._onquery = function (data, update, cb) {
  var req = data.value && decode(messages.Request, data.value)
  if (!req) return cb()
  var res = this._processPeers(req, data, update)
  if (!res) return cb()
  cb(null, messages.Response.encode(res))
}

DwDHT.prototype._processPeers = function (req, data, update) {
  var from = {host: data.node.host, port: req.port || data.node.port}
  if (!from.port) return null // TODO: nessesary? check dwDHTRPC-rpc / udp-socket

  var key = data.target.toString('hex')
  var peer = encodePeer(from)

  if (update && req.type === 1) {
    var id = from.host + ':' + from.port
    var stored = {
      localFilter: null,
      localPeer: null,
      peer: peer
    }

    if (req.localAddress && decodePeer(req.localAddress)) {
      stored.localFilter = req.localAddress.slice(0, 2)
      stored.localPeer = req.localAddress.slice(2)
    }

    this.emit('announce', data.target, from)
    this._store.put(key, id, stored)
  } else if (update && req.type === 2) {
    this.emit('unannounce', data.target, from)
    this._store.del(key, from.host + ':' + from.port)
    return null
  } else if (req.type === 0) {
    this.emit('lookup', data.target)
  }

  var off1 = 0
  var off2 = 0
  var next = this._store.iterator(key)
  var filter = req.localAddress && req.localAddress.length === 6 && req.localAddress.slice(0, 2)

  while (off1 + off2 < 900) {
    var n = next()
    if (!n) break
    if (equals(n.peer, peer)) continue

    n.peer.copy(PEERS, off1)
    off1 += 6

    if (n.localPeer && filter && filter[0] === n.localFilter[0] && filter[1] === n.localFilter[1]) {
      if (!equals(n.localPeer, req.localAddress)) {
        n.localPeer.copy(LOCAL_PEERS, off2)
        off2 += 4
      }
    }
  }

  if (!off1 && !off2) return null

  return {
    peers: PEERS.slice(0, off1),
    localPeers: off2 ? LOCAL_PEERS.slice(0, off2) : null
  }
}

DwDHT.prototype._processPeersLocal = function (key, req, stream) {
  if (!this._store.has(key.toString('hex'))) return

  var data = {node: {id: this.dwDHTRPC.id, host: '127.0.0.1', port: this.dwDHTRPC.address().port}, target: key}
  var res = this._processPeers(req, data, false)
  if (!res) return

  stream.push({
    node: data.node,
    peers: dwDhtPeers.decode(res.peers),
    localPeers: decodeLocalPeers(res.localPeers, req.localAddress)
  })
}

DwDHT.prototype.address = function () {
  return this.dwDHTRPC.address()
}

DwDHT.prototype.bootstrap = function (cb) {
  this.dwDHTRPC.bootstrap(cb)
}

DwDHT.prototype.listen = function (port, cb) {
  this.dwDHTRPC.listen(port, cb)
}

DwDHT.prototype.destroy = function (cb) {
  this.dwDHTRPC.destroy(cb)
}

DwDHT.prototype.ready = function (cb) {
  this.dwDHTRPC.ready(cb)
}

DwDHT.prototype.ping = function (peer, cb) {
  this.dwDHTRPC.ping(peer, cb)
}

DwDHT.prototype.holepunch = function (peer, ref, cb) {
  if (ref.id && equals(ref.id, this.id)) return cb()
  this.dwDHTRPC.holepunch(peer, ref, cb)
}

DwDHT.prototype.announce = function (key, opts, cb) {
  if (typeof opts === 'function') return this.announce(key, null, opts)
  if (typeof opts === 'number') opts = {port: opts}
  if (!opts) opts = {}

  var localAddress = encodePeer(opts.localAddress)
  var req = {
    type: 1,
    port: opts.port || 0,
    localAddress: localAddress
  }

  var map = mapper(localAddress)
  var stream = this.dwDHTRPC.update({
    target: key,
    command: 'peers',
    value: messages.Request.encode(req)
  }, {
    query: true,
    map: map
  }, cb)

  this._processPeersLocal(key, req, stream)

  return stream
}

DwDHT.prototype.unannounce = function (key, opts, cb) {
  if (typeof opts === 'function') return this.unannounce(key, null, opts)
  if (typeof opts === 'number') opts = {port: opts}
  if (!opts) opts = {}

  var req = {
    type: 2,
    port: opts.port || 0,
    localAddress: encodePeer(opts.localAddress)
  }

  this.dwDHTRPC.update({
    target: key,
    command: 'peers',
    value: messages.Request.encode(req)
  }, cb)
}

DwDHT.prototype.lookup = function (key, opts, cb) {
  if (typeof opts === 'function') return this.lookup(key, null, opts)
  if (!opts) opts = {}
  if (opts.localAddress && !opts.localAddress.port) opts.localAddress.port = 0

  var localAddress = encodePeer(opts.localAddress)
  var req = {
    type: 0,
    localAddress: localAddress,
    port: opts.port || 0
  }

  var map = mapper(localAddress)
  var stream = this.dwDHTRPC.query({
    target: key,
    command: 'peers',
    value: messages.Request.encode(req)
  }, {
    map: map
  }, cb)

  this._processPeersLocal(key, req, stream)

  return stream
}

function mapper (localAddress) {
  return map

  function map (data) {
    var res = decode(messages.Response, data.value)
    if (!res) return null

    var peers = res.peers && decode(dwDhtPeers, res.peers)
    if (!peers) return null

    var v = {
      node: data.node,
      peers: peers,
      localPeers: decodeLocalPeers(res.localPeers, localAddress)
    }

    return v
  }
}

function decodeLocalPeers (buf, localAddress) {
  var localPeers = []
  if (!localAddress || !buf) return localPeers

  for (var i = 0; i < buf.length; i += 4) {
    if (buf.length - i < 4) return localPeers

    var port = buf.readUInt16BE(i + 2)
    if (!port || port === 65536) continue

    localPeers.push({
      host: localAddress[0] + '.' + localAddress[1] + '.' + buf[i] + '.' + buf[i + 1],
      port: port
    })
  }

  return localPeers
}

function parseBootstrap (node) {
  return node.indexOf(':') === -1 ? node + ':49737' : node
}

function encodePeer (p) {
  return p && dwDhtPeers.encode([p])
}

function decodePeer (b) {
  var p = b && decode(dwDhtPeers, b)
  return p && p[0]
}

function decode (enc, buf) {
  try {
    return enc.decode(buf)
  } catch (err) {
    return null
  }
}
