'use strict';
const nats = require('nats'),
  EventEmitter = require('events').EventEmitter;
/**
 * This is a client interface of the Nast plugin
 * */
module.exports = (thorin, opt, logger) => {
  const config = Symbol('config');

  class NatsClient extends EventEmitter {


    /**
     * Connect to the Nats server in the options.
     * */
    connect(config) {
      return new Promise((resolve, reject) => {
        let nc = nats.connect(config);
        nc.connected = false;
        nc.once('connect', () => {
          if (opt.debug) logger.trace(`Connected to NATS server`);
          nc.connected = true;
          resolve(nc);
          nc.on('disconnect', () => {
            nc.connected = false;
            logger.warn(`NATS Client disconnected from server`);
          });
          nc.on('reconnect', () => {
            nc.connected = true;
            if (opt.debug) logger.trace(`Reconnecting to NATS server`);
          });
        });
        nc.once('error', (e) => {
          if (nc.connected) {
            logger.warn(`NATS Client encountered an error`);
            logger.debug(e);
          } else {
            logger.warn(`Could not connect to NATS server`);
            reject(e);
          }
        });
      });
    }

  }


  return NatsClient;
}


function promisify(nc, fn, args) {
  return new Promise((resolve, reject) => {
    let isDone = false;
    try {
      args.push(function (err, res) {
        if (isDone) return;
        isDone = true;
        if (err) return reject(err);
        resolve(res);
      });
      if (typeof fn === 'string') fn = nc[fn];
      fn.apply(nc, args);
    } catch (e) {
      reject(e);
    }
  });
}

function args(_args) {
  return Array.prototype.slice.call(_args);
}
