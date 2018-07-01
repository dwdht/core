#!/usr/bin/env node

var minimist = require('minimist')
var dWebDHT = require('./')

var argv = minimist(process.argv, {
  alias: {
    bootstrap: 'b',
    port: 'p'
  }
})

if (argv.help) {
  console.error(
    'Usage: ddht [options]\n' +
    '  --bootstrap, -b  host:port\n' +
    '  --port, -p       listen-port\n' +
    '  --quiet'
  )
  process.exit(0)
}

var dWebDHTRPC = dWebDHT({
  bootstrap: argv.bootstrap
})

dWebDHTRPC.on('announce', function (key, peer) {
  if (!argv.quiet) console.log('announce:', key.toString('hex'), peer)
})

dWebDHTRPC.on('unannounce', function (key, peer) {
  if (!argv.quiet) console.log('unannounce:', key.toString('hex'), peer)
})

dWebDHTRPC.on('lookup', function (key) {
  if (!argv.quiet) console.log('lookup:', key.toString('hex'))
})

dWebDHTRPC.listen(argv.port, function () {
  console.log('hyperdht listening on ' + dWebDHTRPC.address().port)
})

dWebDHTRPC.ready(function loop () {
  if (!argv.quiet) console.log('bootstrapped...')
  setTimeout(bootstrap, Math.floor((5 + Math.random() * 60) * 1000))

  function bootstrap () {
    dWebDHTRPC.bootstrap(loop)
  }
})
