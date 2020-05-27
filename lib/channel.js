'use strict';
const { EventEmitter } = require('events'),
  nats = require('nats');
/**
 * Since we're already using NATS as our message highway,
 * we also provide an abstraction layer over these functionalities.
 * We do so by providing RPC channels.
 * In essence, a RPC Channel is a nats pubsub channel to use only for
 * abstracting away dispatch() and handle() functions.
 * Events:
 *  - ready - triggered once when the client was attached.
 *  - destroy - triggered once when the channel will be destroyed.
 *  - request[action,payload,result] - an incoming dispatch event.
 *  - event[action,payload] - an incoming published event, with no callback.
 *
 *  Usage:
 *    const natsObj = thorin.plugin('nats');
 *    const channel = natsObj.channel('my-channel');
 *    channel.handle('api.user.read', async (data) => {
 *        // TODO something async.
 *        return {    // return the result.
 *          ok: true
 *        }
 *    });
 *    // some time later
 *    let res = await channel.dispatch('api.user.read', {
 *      id: 1
 *    });
 *    console.log(res);
 * */
const EVENTS = [
  'ready',      // triggered once when the client was attached.
  'destroy',    // triggered once when the channel will be destroyed.
  'request',    // {action,payload,result} triggered when an incoming request was processed.
  'event'       // {action,payload} triggered when an event was published to this channel.
];

function noop() {}

module.exports = (thorin, nOpt, logger) => {

  const ERROR_TIMEOUT = thorin.error('NATS.TIMEOUT', 'Request timed out', 502),
    ERROR_REQUEST = thorin.error('NATS.DATA', 'The specified request data is not valid', 400),
    ERROR_HANDLER = thorin.error('NATS.DATA', 'The specified event does not exist', 404),
    ERROR_RESPONSE = thorin.error('NATS.DATA', 'An error occurred while retrieving result', 500);

  const CHANNEL = { // we need  2 sub channels per channel, so that we don't get dispatch actions while we're only subscribed to publishes.
    ACTION: 'a',
    PUBLISH: 'p',
    QUEUE: 'q'
  };

  class NatsChannel extends EventEmitter {

    #client = null;
    #options = {};
    #actions = {};
    #idAction = null;
    #idPublish = null;
    #encode = JSON.stringify;
    #decode = JSON.parse;

    /**
     * @Options
     *  - opt.unique - if set to true, we will add the "queue:" field in the nats subscription
     *  - opt.debug - log incoming/outgoing events, defaults to cOpt.debug
     * */
    constructor(name, cOpt = {}) {
      super();
      _hideEvent(this);
      this.#options = cOpt;
      let hasDebug = (this.#options.debug || nOpt.channel.debug);
      if (hasDebug) {
        this.debug = true;
      }
      this.name = name;
    }

    /**
     * Called internally to set the NATS.io connection client.
     * */
    set client(cObj) {
      if (!this.#client) {
        this.#client = cObj;
        this.start();
        process.nextTick(() => {
          this.emit('ready');
        });
      }
    }

    get active() {
      return !!this.#client;
    }

    /**
     * This is the function that handles all incoming events.
     * Internally, the "req" object contains:
     *  - "a" - when we have a dispatch action request
     *  - "p" - when we have a generic publish
     *  - "d" - the actual data object
     * */
    #handleEvent = (req, reply, subject, sid) => {
      req = this.#decode(req);
      if (req === null) {
        return this.#reply(reply, ERROR_REQUEST);
      }
      // Check if we have a "publish"
      try {
        // Handle: incoming publish event
        let pType = typeof req.p;
        if (pType === 'string' || pType === 'number') {
          return this.#handlePublish(req.p, req.d);
        }
        // Handle: incoming dispatched action.
        let aType = typeof req.a;
        if (aType === 'string' || aType === 'number') {
          return this.#handleDispatch(req.a, req.d, reply ? (res) => this.#reply(reply, res) : null);
        }
      } catch (e) {
        logger.warn(`Could not handle incoming event: [${req.a || req.p}]`, req, e);
      }
    }

    /**
     * Emit an event that came in through publish, not action request.
     * */
    #handlePublish = (event, data) => {
      if (this.debug) {
        logger.debug(`[PUB ${this.name}] ${event}`, data || '');
      }
      this.emit('event', {
        action: event,
        payload: data
      });
    }

    /**
     * Handles an incoming dispatched event.
     * If we have a reply callback, done is given. otherwise, we don't need to reply.
     * */
    #handleDispatch = (event, data, done) => {
      let action = getActionName(event);
      let handler = this.#actions[action],
        debug = this.debug,
        name = this.name,
        start;
      if (!handler) return done && done(ERROR_HANDLER);
      try {
        let debug = this.debug;
        if (debug) {
          start = Date.now();
          logger.debug(`[INC ${name}] ${event}`, data || '');
        }
        let pRes = handler(data);
        if (pRes instanceof Promise) {
          pRes.then(function (r) {
            if (debug) {
              let took = Date.now() - start;
              logger.debug(`[END ${name}] ${event} (took: ${took}ms)`);
            }
            this.emit('request', {
              action: event,
              payload: data,
              result: r
            });
            done && done(r);
          }, function (e) {
            if (debug) {
              let took = Date.now() - start;
              logger.debug(`[END ${name}] ${event} - [${e.code} - ${e.message}] (took: ${took}ms)`);
            }
            handleDispatchError(e, action, done);
          }).catch(function (e) {
            if (debug) {
              let took = Date.now() - start;
              logger.debug(`[END ${name}] ${event} - [${e.code} - ${e.message}] (took: ${took}ms)`);
            }
            handleDispatchError(e, action, done);
          });
        } else {
          if (debug) {
            let took = Date.now() - start;
            logger.debug(`[END ${name}] ${event} (took: ${took}ms)`);
          }
          this.emit('request', {
            action: event,
            payload: data,
            result: pRes
          });
          done && done(pRes);
        }
      } catch (e) {
        if (debug) {
          let took = Date.now() - start;
          logger.debug(`[END ${name}] ${event} - [${e.code} - ${e.message}] (took: ${took}ms)`);
        }
        handleDispatchError(e, action, done);
      }
    }

    /**
     * Uses a reply subscription inbox to publish back a response.
     * If the reply id (rid) does not exist, we simply ignore.
     * If data is error, we set the "e", otherwise we set the "d" field.
     * */
    #reply = (rid, data) => {
      if (!this.#client) return false;
      if (typeof rid !== 'string' || !rid) return false;
      try {
        let res = {};
        if (data instanceof thorin.Error) {
          res.e = data;
        } else if (typeof data !== 'undefined' && data !== null) {
          res.d = data;
        }
        res = this.encode(res);
        if (res === null) return false;
        this.#client.publish(rid, res);
        return true;
      } catch (e) {
        logger.warn(`Could not reply to ${rid} with data`, e);
        return false;
      }
    }

    /**
     * Checks if the channel has any action handlers.
     * */
    #hasActions = () => {
      return Object.keys(this.#actions).length > 0;
    }

    /**
     * Checks if we have any kind of event listeners for "publishes"
     * */
    #hasEvents = () => {
      return !!(this._events.event);
    }

    /**
     * Override the default "on"
     * */
    on(name, fn) {
      if (EVENTS.indexOf(name) === -1) {
        logger.warn(`Channel.${this.name}: .on(${name}) is not a valid event`);
        return this;
      }
      super.on(name, fn);
      if (name === 'event') {
        // subscribe for publishes, on first 'on' event
        this.start();
      }
      return this;
    }

    /**
     * This is the main internal subscription for this channel.
     * */
    start() {
      if (!this.#client) return false;
      let cOpt = {};
      if (this.#options.unique) {
        cOpt.queue = this.getName(CHANNEL.QUEUE);
      }
      if (this.#hasActions() && !this.#idAction) {
        let channel = this.getName(CHANNEL.ACTION);
        if (this.debug) {
          logger.trace(`[SUB ${channel}]`);
        }
        this.#idAction = this.#client.subscribe(channel, cOpt, this.#handleEvent);
      }
      if (this.#hasEvents() && !this.#idPublish) {
        let channel = this.getName(CHANNEL.PUBLISH);
        if (this.debug) {
          logger.trace(`[SUB ${channel}]`);
        }
        this.#idPublish = this.#client.subscribe(channel, cOpt, this.#handleEvent);
      }
      return true;
    }

    /**
     * Stops incoming events by unsubscribing the channel from nats.
     * */
    stop() {
      if (!this.#client) return false;
      if (this.#idAction !== null) {
        if (this.#options.unique) {
          this.#client.drainSubscription(this.#idAction, noop);
        } else {
          this.#client.unsubscribe(this.#idAction);
        }
        this.#idAction = null;
      }
      if (this.#idPublish !== null) {
        if (this.#options.unique) {
          this.#client.drainSubscription(this.#idPublish, noop);
        } else {
          this.#client.unsubscribe(this.#idPublish);
        }
        this.#idPublish = null;
      }
      return true;
    }

    /**
     * If provided with a function, it will be used to encode the payload before we send it through NATS.
     * By default, we use the JSON.stringify encoder.
     *
     * Otherwise, try to encode the data.
     * */
    encode(d) {
      if (typeof d === 'function') {
        this.#encode = d;
        return this;
      }
      try {
        let e = this.#encode(d);
        if (typeof e === 'undefined' || e === null) return null;
        return e;
      } catch (e) {
        return null;
      }
    }

    /**
     * If provided with a funciton, it will be used to decode the payload before we send it through NATS.io
     * By default, we use the JSON.parse decoder.
     * Otherwise, try to decode the data.
     * */
    decode(d) {
      if (typeof d === 'function') {
        this.#decode = d;
        return this;
      }
      try {
        let de = this.#decode(d);
        if (typeof de === 'undefined' || de === null) return null;
        return de;
      } catch (e) {
        return null;
      }
    }


    /**
     * Returns the publish/sub channel name, using the prefix.
     * */
    getName(subChannel) {
      let r = nOpt.channel.prefix || '';
      r += this.name;
      if (typeof subChannel === 'string' && subChannel) {
        r += `.${subChannel}`;
      }
      return r;
    }

    /**
     * Registers an internal RPC handler for a given action name.
     * @Arguments
     *  - action - the action name to handle.
     *  - fn - the callback function to run when we receive something.
     * */
    handle(action, fn) {
      let aType = (typeof action);
      if (aType !== 'string' && aType !== 'number') throw thorin.error('DATA.INVALID', 'Handler action name must be a string or number');
      let aName = getActionName(action);
      if (this.#actions[aName]) throw thorin.error('DATA.INVALID', 'A handler is already registered for ' + action);
      if (typeof fn !== 'function' || !fn) throw thorin.error('DATA.INVALID', 'Handler fn must be a function');
      this.#actions[aName] = fn;
      this.start(); // only start when we have the first handler.
      return this;
    }

    /**
     * Sends a RPC request through this channel. This is similar thorin-plugin-cluster's
     * dispatch function - essentially, we send a RPC name and payload with optional waiting for response.
     * @Arguments
     *  - action - the RPC action name
     *  - payload - an object with the data we want to send.
     *  - opt.timeout [=3000] - the timeout (in ms) we want to use, defaults to opt.channel.timeout
     *  - opt.wait [=true] - if set to false, we do not wait for a reply - acts like a publish.
     *  - opt.max [=1] - if set, we limit the number of responses we get.
     * */
    dispatch(action, payload = {}, opt = {}) {
      if (!this.#client) return false;
      if (typeof payload !== 'object' || !payload) throw thorin.error('DATA.INVALID', 'Request payload must be an object');
      let aType = typeof action;
      if (aType !== 'string' && aType !== 'number') throw thorin.error('DATA.INVALID', 'Request action must be a string or number');
      let timeout = (typeof opt.timeout === 'number' ? opt.timeout : nOpt.channel.timeout),
        wait = (typeof opt.wait === 'boolean' ? opt.wait : true),
        channel = this.getName(CHANNEL.ACTION),
        maxResponses = (typeof opt.max === 'number' ? opt.max : 1);
      if (timeout <= 50) timeout = timeout * 1000;  // make sure we have ms.
      let req = {
        a: action,
        d: payload
      };
      req = this.encode(req);
      if (wait === false) {
        this.#client.request(channel, req);
        return true;
      }
      return new Promise((resolve, reject) => {
        this.#client.request(channel, req, {
          max: maxResponses,
          timeout
        }, (msg) => {
          if (msg instanceof nats.NatsError) {
            let errCode = `NATS.${msg.code.toUpperCase()}`,
              errMsg = `An error occurred while performing request`,
              err;
            if (errCode === 'NATS.REQ_TIMEOUT') {
              err = ERROR_TIMEOUT;
            } else {
              err = thorin.error(errCode, errMsg);
            }
            return reject(err);
          }
          let res = this.decode(msg);
          if (!res) {
            return reject(ERROR_RESPONSE);
          }
          // got error.
          if (typeof res.e === 'object' && res.e) {
            let err = thorin.error(res.e.code, res.e.message, res.e.status, res.e.data);
            return reject(err);
          }
          resolve(res.d);
        });
      });
    }

    /**
     * Publishes the given action and payload to this channel.
     * This acts in essence like a dispatch() but with no handlers, nor reply.
     * @Arguments
     *  - action - the action/event name to publish
     *  - payload - optional object payload to send.
     * */
    publish(action, payload = {}) {
      if (!this.#client) return false;
      let aType = typeof action;
      if (aType !== 'string' && aType !== 'number') throw thorin.error('DATA.INVALID', 'Request action must be a string or number');
      try {
        let channel = this.getName(CHANNEL.PUBLISH);
        let req = {
          p: action,
          d: payload
        };
        req = this.encode(req);
        this.#client.publish(channel, req);
        return true;
      } catch (e) {
        logger.warn(`Could not publish [${action}]`, e);
        return false;
      }
    }

    /**
     * Completely destroy the channel.
     * */
    destroy() {
      this.stop();
      this.#client = null;
      this.#actions = {};
      this.emit('destroy');
      this.removeAllListeners();
    }

  }


  function handleDispatchError(e, action, done) {
    if (done) {
      done(e);
    } else {
      logger.warn(`Could not process action [${action}] - ${e.message}`);
    }
  }

  return NatsChannel;
};


function getActionName(n) {
  if (typeof n === 'string') return n;
  return n.toString();
}

function _hideEvent(c) {
  Object.defineProperty(c, '_events', {
    value: c._events,
    enumerable: false,
    writable: true
  });
  Object.defineProperty(c, '_eventsCount', {
    value: 0,
    enumerable: false,
    writable: true
  });
  Object.defineProperty(c, '_maxListeners', {
    value: undefined,
    enumerable: false,
    writable: true
  });
}
