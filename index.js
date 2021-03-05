'use strict';

const initNats = require('./lib/client'),
  initChannel = require('./lib/channel');

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
    required: true,
    channel: {
      prefix: 'trpc.',   // the subscription prefix for all our channels
      timeout: 3000, // max timeout for RPC dispatch requests.
      debug: false  // if set to true, we will log incoming dispatches/publishes.
    },
    tls: {} // .key, .cert, .ca
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
    if (u.indexOf(':') === -1) {
      u += ':4222';
    }
    opt.url[i] = u;
  }
  let cOpt = opt.options;
  cOpt.servers = opt.url;
  if (opt.username) cOpt.user = opt.username;
  if (opt.password) cOpt.pass = opt.password;
  if (opt.token) cOpt.token = opt.token;
  if (typeof opt.tls === 'object' && opt.tls && opt.tls.key && opt.tls.cert) {
    cOpt.tls = opt.tls;
  }
  const NatsClient = initNats(thorin, cOpt, logger),
    NatsChannel = initChannel(thorin, opt, logger);

  const natsObj = {};

  /**
   * On thorin app launch, connect to server.
   * */
  natsObj.run = async (done) => {
    if (natsObj.client) return done();
    try {
      await natsObj.getClient();  // the default client.
    } catch (e) {
      if (opt.required) {
        logger.warn(`Could not connect to nats servers`);
        logger.debug(e);
      }
    }
    done();
  };

  let channels = {},
    isConnecting = false,
    pendingChannels = [],
    pendingClients = [];

  /**
   * Makes sure that we have a connected client, and returns it.
   * */
  natsObj.getClient = async () => {
    if (isConnecting) {
      return new Promise((resolve) => pendingClients.push(resolve));
    }
    if (natsObj.client) return natsObj.client;
    isConnecting = true;
    let clientObj = await natsObj.connect();
    isConnecting = false;
    natsObj.client = clientObj;
    pendingClients.forEach(resolve => resolve(clientObj));
    pendingClients = [];
    return clientObj;
  }

  /**
   * Connect to the nats server, creating a client instance.
   * Returns a promise.
   * */
  natsObj.connect = async (opt = {}) => {
    let connectOpt = thorin.util.extend(cOpt, opt);
    if (connectOpt.servers.length === 0) throw thorin.error('PLUGIN.NATS', 'At least one NATS server is required');
    let clientObj = new NatsClient();
    let natsObj = await clientObj.connect(connectOpt);
    for (let i = 0, len = pendingChannels.length; i < len; i++) {
      pendingChannels[i].client = natsObj;
    }
    pendingChannels = [];
    return natsObj;
  };

  /**
   * Creates or retrieves the specified NATS Channel object.
   * @Arguments
   *  - name - the channel name to use.
   *  - cOpt.unique - if set to true, we will place this channel into a unique queue group, and messages will be delivered to only one subscriber.
   * */
  natsObj.channel = (name, cOpt = {}) => {
    let cObj = channels[name];
    if (!cObj) {
      cObj = natsObj.channel.create(name, cOpt);
      channels[name] = cObj;
      cObj.on('destroy', () => {
        delete channels[name];
      });
    }
    return cObj;
  }
  /* expose our NatsChannel class */
  natsObj.channel.create = (name, cOpt) => {
    let cObj = new NatsChannel(name, cOpt);
    if (natsObj.client) {
      cObj.client = natsObj.client;
    } else {
      pendingChannels.push(cObj);
    }
    cObj.on('destroy', () => {
      let cix = pendingChannels.indexOf(cObj);
      if (cix !== -1) {
        pendingChannels.splice(cix, 1);
      }
    });
    return cObj;
  }

  return natsObj;
};
module.exports.publicName = 'nats';
