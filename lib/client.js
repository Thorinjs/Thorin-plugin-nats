'use strict';
const nats = require('nats'),
  EventEmitter = require('events').EventEmitter;
/**
 * This is a client interface of the Nats plugin
 * */
module.exports = (thorin, opt, logger) => {

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
            if (opt.required) {
              logger.warn(`NATS Client encountered an error`);
              logger.debug(e);
            }
          } else {
            if (opt.required) {
              logger.warn(`Could not connect to NATS server`);
            }
            reject(e);
          }
        });
      });
    }

  }

  return NatsClient;
};

