'use strict';

const initNast = require('./lib/client');

module.exports = function (thorin, opt, pluginName) {
  const defaultOpt = {
    logger: pluginName || 'nats',
    debug: false,
    url: [],    // An array of Nats.io server URLs
    username: null, // The username/password credentials to use.
    password: null,
    token: null,    // An authorization token to use
    options: {       // Additional nats.io direct client options. See https://github.com/nats-io/node-nats
      name: thorin.id, // the Nats.io client name
      encoding: 'utf8',
      json: true,
      maxReconnectAttempts: -1,
      reconnectWait: 1000
    },
  };
  opt = thorin.util.extend(defaultOpt, opt);
  const logger = thorin.logger(defaultOpt.logger);
  if (typeof opt.url === 'string') {
    opt.url = [opt.url];
  }
  if (!(opt.url instanceof Array)) {
    throw thorin.error('PLUGIN.NATS', 'URL Configuration must be an array of nats server URLs');
  }
  // Validate URLs as array of "nats://ip:port"]
  for (let i = 0; i < opt.url.length; i++) {
    let u = opt.url[i];
    if (typeof u !== 'string' || !u) {
      throw thorin.error('PLUGIN.NATS', 'URL Configuration must be an array of nats server URLs');
    }
    if (u.indexOf('://') === -1) u = 'nats://' + u;
    if (u.lastIndexOf(':') !== 5) {
      u += ':4222';
    }
    opt.url[i] = u;
  }
  let cOpt = opt.options;
  cOpt.servers = opt.url;
  if (opt.username) cOpt.user = opt.username;
  if (opt.password) cOpt.pass = opt.password;
  if (opt.token) cOpt.token = opt.token;

  const NatsClient = initNast(thorin, opt, logger);

  const natsObj = {};

  /**
   * On thorin app launch, connect to server.
   * */
  natsObj.run = async (done) => {
    try {
      let clientObj = await natsObj.connect();
      natsObj.client = clientObj; // the default client.
    } catch (e) {
      logger.warn(`Could not connect to nats servers`);
      logger.debug(e);
    }
    done();
  };

  /**
   * Connet to the nats server, creating a client instance.
   * Returns a promise.
   * */
  natsObj.connect = async (opt = {}) => {
    let connectOpt = thorin.util.extend(cOpt, opt);
    let clientObj = new NatsClient(connectOpt);
    if (connectOpt.servers.length === 0) throw thorin.error('PLUGIN.NATS', 'At least one NATS server is required');
    let natsObj = await clientObj.connect();
    return natsObj;
  };

  return natsObj;
};
module.exports.publicName = 'nats';