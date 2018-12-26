const args = JSON.parse(process.argv[2]);

let PORT;
if (args.port) {
  PORT = parseInt(args.port);
}
const BROKER_ID = args.brokerId || 0;
const SOCKET_PATH = args.socketPath;
const EXPIRY_ACCURACY = args.expiryAccuracy || 1000;
const BROKER_CONTROLLER_PATH = args.brokerControllerPath;
const DOWNGRADE_TO_USER = args.downgradeToUser;
const PROCESS_TERM_TIMEOUT = args.processTermTimeout || 10000;
const DEBUG_PORT = args.debug || null;
const INIT_CONTROLLER = null;
const BROKER_CONTROLLER = null;
const DEFAULT_IPC_ACK_TIMEOUT = 10000;

let brokerInitOptions = JSON.parse(process.env.brokerInitOptions);

const AsyncStreamEmitter = require('async-stream-emitter');
const async = require('async');
const fs = require('fs');
const uuid = require('uuid');
const com = require('ncom');
const ExpiryManager = require('expirymanager').ExpiryManager;
const FlexiMap = require('fleximap').FlexiMap;

const scErrors = require('sc-errors');
const BrokerError = scErrors.BrokerError;
const TimeoutError = scErrors.TimeoutError;
const InvalidArgumentsError = scErrors.InvalidArgumentsError;
const InvalidActionError = scErrors.InvalidActionError;

let initialized = {};

// Non-fatal error.
let sendErrorToMaster = function (err) {
  let error = scErrors.dehydrateError(err, true);
  process.send({type: 'error', data: error});
};

// Fatal error.
let exitWithError = function (err) {
  sendErrorToMaster(err);
  process.exit(1);
};

if (DOWNGRADE_TO_USER && process.setuid) {
  try {
    process.setuid(DOWNGRADE_TO_USER);
  } catch (err) {
    sendErrorToMaster(new BrokerError(
      `Could not downgrade to user "${DOWNGRADE_TO_USER}" - Either this user does not exist ` +
      `or the current process does not have the permission to switch to it`
    ));
  }
}

let send = function (socket, object, options) {
  socket.write(object, options);
};

let dataMap = new FlexiMap();
let subscriptions = {};

let dataExpirer = new ExpiryManager();

let addListener = function (socket, channel) {
  if (subscriptions[socket.id] == null) {
    subscriptions[socket.id] = {};
  }
  subscriptions[socket.id][channel] = socket;
};

let hasListener = function (socket, channel) {
  return !!(subscriptions[socket.id] && subscriptions[socket.id][channel]);
};

let anyHasListener = function (channel) {
  let socketSubscriptions = Object.values(subscriptions || {});
  let len = socketSubscriptions.length;
  for (let i = 0; i < len; i++) {
    if (socketSubscriptions[i][channel]) {
      return true;
    }
  }
  return false;
};

let removeListener = function (socket, channel) {
  if (subscriptions[socket.id]) {
    delete subscriptions[socket.id][channel];
  }
};

let removeAllListeners = function (socket) {
  let subMap = subscriptions[socket.id];
  delete subscriptions[socket.id];
  return Object.keys(subMap || {});
};

let getSubscriptions = function (socket) {
  return Object.keys(subscriptions[socket.id] || {});
};

let exec = function (query, baseKey) {
  let rebasedDataMap;
  if (baseKey) {
    rebasedDataMap = dataMap.getRaw(baseKey);
  } else {
    rebasedDataMap = dataMap;
  }

  return Function(`"use strict"; return (${query})(arguments[0], arguments[1], arguments[2]);`)(rebasedDataMap, dataExpirer, subscriptions);
};

let pendingResponseHandlers = {};

function createIPCResponseHandler(ipcAckTimeout, callback) {
  let cid = uuid.v4();

  let responseTimeout = setTimeout(() => {
    let responseHandler = pendingResponseHandlers[cid];
    delete pendingResponseHandlers[cid];
    let timeoutError = new TimeoutError('IPC response timed out');
    responseHandler.callback(timeoutError);
  }, ipcAckTimeout);

  pendingResponseHandlers[cid] = {
    callback: callback,
    timeout: responseTimeout
  };

  return cid;
}

function handleMasterResponse(message) {
  let responseHandler = pendingResponseHandlers[message.rid];
  if (responseHandler) {
    clearTimeout(responseHandler.timeout);
    delete pendingResponseHandlers[message.rid];
    let properError = scErrors.hydrateError(message.error, true);
    responseHandler.callback(properError, message.data);
  }
}

let scBroker;

function SCBroker(options) {
  AsyncStreamEmitter.call(this);

  if (scBroker) {
    let err = new BrokerError('Attempted to instantiate a broker which has already been instantiated');
    throw err;
  }

  options = options || {};
  scBroker = this;

  this.id = BROKER_ID;
  this.debugPort = DEBUG_PORT;
  this.type = 'broker';
  this.dataMap = dataMap;
  this.dataExpirer = dataExpirer;
  this.subscriptions = subscriptions;

  this.MIDDLEWARE_SUBSCRIBE = 'subscribe';
  this.MIDDLEWARE_PUBLISH_IN = 'publishIn';
  this._middleware = {};
  this._middleware[this.MIDDLEWARE_SUBSCRIBE] = [];
  this._middleware[this.MIDDLEWARE_PUBLISH_IN] = [];

  if (options.run != null) {
    this.run = options.run;
  }

  this._init(brokerInitOptions);
}

SCBroker.prototype = Object.create(AsyncStreamEmitter.prototype);

SCBroker.create = function (options) {
  return new SCBroker(options);
};

SCBroker.prototype._init = async function (options) {
  this.options = options;
  this.instanceId = this.options.instanceId;
  this.secretKey = this.options.secretKey;
  this.ipcAckTimeout = this.options.ipcAckTimeout || DEFAULT_IPC_ACK_TIMEOUT;

  let runResult = this.run();
  try {
    await Promise.resolve(runResult);
  } catch (err) {
    exitWithError(err);
    return;
  }
  comServerListen();
};

SCBroker.prototype.run = function () {};

SCBroker.prototype.sendMessageToMaster = function (data) {
  let messagePacket = {
    type: 'brokerMessage',
    data
  };
  process.send(messagePacket);
  return Promise.resolve();
};

SCBroker.prototype.sendRequestToMaster = function (data) {
  return new Promise((resolve, reject) => {
    let messagePacket = {
      type: 'brokerRequest',
      brokerId: this.id,
      data
    };
    messagePacket.cid = createIPCResponseHandler(this.ipcAckTimeout, (err, result) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(result);
    });
    process.send(messagePacket);
  });
};

SCBroker.prototype.exec = function (query, baseKey) {
  return exec(query, baseKey);
};

SCBroker.prototype.publish = function (channel, message) {
  let sock;
  let socketSubscriptions = Object.values(subscriptions || {});
  socketSubscriptions.forEach((subs) => {
    sock = subs[channel];
    if (sock) {
      send(sock, {type: 'message', channel: channel, value: message}, pubSubOptions);
    }
  });
};

SCBroker.prototype._passThroughMiddleware = function (command, socket, cb) {
  let action = command.action;
  let callbackInvoked = false;

  let applyEachMiddleware = (type, req, cb) => {
    async.applyEachSeries(this._middleware[type], req, (err) => {
      if (callbackInvoked) {
        this.emit('warning', {
          warning: new InvalidActionError(`Callback for ${type} middleware was already invoked`)
        });
      } else {
        callbackInvoked = true;
        cb(err, req);
      }
    });
  }

  if (action === 'subscribe') {
    let req = {socket, channel: command.channel};
    applyEachMiddleware(this.MIDDLEWARE_SUBSCRIBE, req, cb);
  } else if (action === 'publish') {
    let req = {socket, channel: command.channel, command};
    applyEachMiddleware(this.MIDDLEWARE_PUBLISH_IN, req, cb);
  } else {
    cb(null);
  }
}

SCBroker.prototype.addMiddleware = function (type, middleware) {
  if (!this._middleware[type]) {
    throw new InvalidArgumentsError(`Middleware type "${type}" is not supported`);
  }

  this._middleware[type].push(middleware);
}

SCBroker.prototype.removeMiddleware = function (type, middleware) {
  let middlewareFunctions = this._middleware[type];
  if (!middlewareFunctions) {
    throw new InvalidArgumentsError(`Middleware type "${type}" is not supported`);
  }

  this._middleware[type] = middlewareFunctions.filter((fn) => {
    return fn !== middleware;
  });
};

let pubSubOptions = {
  batch: true
};

let actions = {
  init: function (command, socket) {
    let brokerInfo = {
      brokerId: BROKER_ID,
      pid: process.pid
    };
    let result = {id: command.id, type: 'response', action: 'init', value: brokerInfo};
    if (scBroker.secretKey == null || command.secretKey === scBroker.secretKey) {
      initialized[socket.id] = {};
    } else {
      let err = new BrokerError('Invalid password was supplied to the broker');
      result.error = scErrors.dehydrateError(err, true);
    }
    send(socket, result);
  },

  set: function (command, socket) {
    let result = scBroker.dataMap.set(command.key, command.value);
    let response = {id: command.id, type: 'response', action: 'set'};
    if (command.getValue) {
      response.value = result;
    }
    send(socket, response);
  },

  expire: function (command, socket) {
    scBroker.dataExpirer.expire(command.keys, command.value);
    let response = {id: command.id, type: 'response', action: 'expire'};
    send(socket, response);
  },

  unexpire: function (command, socket) {
    scBroker.dataExpirer.unexpire(command.keys);
    let response = {id: command.id, type: 'response', action: 'unexpire'};
    send(socket, response);
  },

  getExpiry: function (command, socket) {
    let response = {id: command.id, type: 'response', action: 'getExpiry', value: scBroker.dataExpirer.getExpiry(command.key)};
    send(socket, response);
  },

  get: function (command, socket) {
    let result = scBroker.dataMap.get(command.key);
    send(socket, {id: command.id, type: 'response', action: 'get', value: result});
  },

  getRange: function (command, socket) {
    let result = scBroker.dataMap.getRange(command.key, command.fromIndex, command.toIndex);
    send(socket, {id: command.id, type: 'response', action: 'getRange', value: result});
  },

  getAll: function (command, socket) {
    send(socket, {id: command.id, type: 'response', action: 'getAll', value: scBroker.dataMap.getAll()});
  },

  count: function (command, socket) {
    let result = scBroker.dataMap.count(command.key);
    send(socket, {id: command.id, type: 'response', action: 'count', value: result});
  },

  add: function (command, socket) {
    let result = scBroker.dataMap.add(command.key, command.value);
    let response = {id: command.id, type: 'response', action: 'add', value: result};
    send(socket, response);
  },

  concat: function (command, socket) {
    let result = scBroker.dataMap.concat(command.key, command.value);
    let response = {id: command.id, type: 'response', action: 'concat'};
    if (command.getValue) {
      response.value = result;
    }
    send(socket, response);
  },

  registerDeathQuery: function (command, socket) {
    let response = {id: command.id, type: 'response', action: 'registerDeathQuery'};

    if (initialized[socket.id]) {
      initialized[socket.id].deathQuery = command.value;
    }
    send(socket, response);
  },

  exec: function (command, socket) {
    let ret = {id: command.id, type: 'response', action: 'exec'};
    try {
      let result = scBroker.exec(command.value, command.baseKey);
      if (result !== undefined) {
        ret.value = result;
      }
    } catch (e) {
      let queryErrorPrefix = 'Exception at exec(): ';
      if (typeof e === 'string') {
        e = queryErrorPrefix + e;
      } else if (typeof e.message === 'string') {
        e.message = queryErrorPrefix + e.message;
      }
      ret.error = scErrors.dehydrateError(e, true);
    }
    if (!command.noAck) {
      send(socket, ret);
    }
  },

  remove: function (command, socket) {
    let result = scBroker.dataMap.remove(command.key);
    if (!command.noAck) {
      let response = {id: command.id, type: 'response', action: 'remove'};
      if (command.getValue) {
        response.value = result;
      }
      send(socket, response);
    }
  },

  removeRange: function (command, socket) {
    let result = scBroker.dataMap.removeRange(command.key, command.fromIndex, command.toIndex);
    if (!command.noAck) {
      let response = {id: command.id, type: 'response', action: 'removeRange'};
      if (command.getValue) {
        response.value = result;
      }
      send(socket, response);
    }
  },

  removeAll: function (command, socket) {
    scBroker.dataMap.removeAll();
    if (!command.noAck) {
      send(socket, {id: command.id, type: 'response', action: 'removeAll'});
    }
  },

  splice: function (command, socket) {
    let args = [command.key, command.fromIndex, command.count];
    if (command.items) {
      args = args.concat(command.items);
    }
    // Remove any consecutive undefined references from end of array
    for (let i = args.length - 1; i >= 0; i--) {
      if (args[i] !== undefined) {
        break;
      }
      args.pop();
    }
    let result = scBroker.dataMap.splice.apply(scBroker.dataMap, args);
    if (!command.noAck) {
      let response = {id: command.id, type: 'response', action: 'splice'};
      if (command.getValue) {
        response.value = result;
      }
      send(socket, response);
    }
  },

  pop: function (command, socket) {
    let result = scBroker.dataMap.pop(command.key);
    if (!command.noAck) {
      let response = {id: command.id, type: 'response', action: 'pop'};
      if (command.getValue) {
        response.value = result;
      }
      send(socket, response);
    }
  },

  hasKey: function (command, socket) {
    send(socket, {id: command.id, type: 'response', action: 'hasKey', value: scBroker.dataMap.hasKey(command.key)});
  },

  subscribe: function (command, socket) {
    let hasListener = anyHasListener(command.channel);
    addListener(socket, command.channel);
    if (!hasListener) {
      scBroker.emit('subscribe', {
        channel: command.channel
      });
    }
    send(socket, {id: command.id, type: 'response', action: 'subscribe', channel: command.channel}, pubSubOptions);
  },

  unsubscribe: function (command, socket) {
    if (command.channel) {
      removeListener(socket, command.channel);
      let hasListener = anyHasListener(command.channel);
      if (!hasListener) {
        scBroker.emit('unsubscribe', {
          channel: command.channel
        });
      }
    } else {
      let channels = removeAllListeners(socket);
      let len = channels.length;
      for (let i = 0; i < len; i++) {
        if (!anyHasListener(channels[i])) {
          scBroker.emit('unsubscribe', {
            channel: channels[i]
          });
        }
      }
    }
    send(socket, {id: command.id, type: 'response', action: 'unsubscribe', channel: command.channel}, pubSubOptions);
  },

  publish: function (command, socket) {
    scBroker.publish(command.channel, command.value);
    let response = {id: command.id, type: 'response', action: 'publish', channel: command.channel};
    if (command.getValue) {
      response.value = command.value;
    }
    scBroker.emit('publish', {
      channel: command.channel,
      data: command.value
    });
    send(socket, response, pubSubOptions);
  },

  sendRequest: function (command, socket) {
    scBroker.emit('request', {
      data: command.value,
      end: (data) => {
        send(socket, {
          id: command.id,
          type: 'response',
          action: 'sendRequest',
          value: data
        });
      },
      error: (err) => {
        send(socket, {
          id: command.id,
          type: 'response',
          action: 'sendRequest',
          error: scErrors.dehydrateError(err, true)
        });
      }
    });
  },

  sendMessage: function (command, socket) {
    scBroker.emit('message', {data: command.value});
  }
};

let MAX_ID = Math.pow(2, 53) - 2;
let curID = 1;

let genID = function () {
  curID++;
  curID = curID % MAX_ID;
  return curID;
};

let comServer = com.createServer();
let connections = {};

let handleConnection = function (sock) {
  sock.on('error', sendErrorToMaster);
  sock.id = genID();

  connections[sock.id] = sock;

  sock.on('message', (command) => {
    if (initialized.hasOwnProperty(sock.id) || command.action === 'init') {
      scBroker._passThroughMiddleware(command, sock, (err) => {
        try {
          if (err) {
            throw err;
          } else if (actions[command.action]) {
            actions[command.action](command, sock);
          }
        } catch (err) {
          err = scErrors.dehydrateError(err, true);
          send(sock, {id: command.id, type: 'response', action:  command.action, error: err});
        }
      });
    } else {
      let err = new BrokerError('Cannot process command before init handshake');
      err = scErrors.dehydrateError(err, true);
      send(sock, {id: command.id, type: 'response', action: command.action, error: err});
    }
  });

  sock.on('close', () => {
    delete connections[sock.id];

    if (initialized[sock.id]) {
      if (initialized[sock.id].deathQuery) {
        exec(initialized[sock.id].deathQuery);
      }
      delete initialized[sock.id];
    }
    let channels = removeAllListeners(sock);
    let len = channels.length;
    for (let i = 0; i < len; i++) {
      if (!anyHasListener(channels[i])) {
        scBroker.emit('unsubscribe', {
          channel: channels[i]
        });
      }
    }
  });
};

comServer.on('connection', handleConnection);

comServer.on('listening', function () {
  let brokerInfo = {
    brokerId: BROKER_ID,
    pid: process.pid
  };
  process.send({
    type: 'listening',
    data: brokerInfo
  });
});

let comServerListen = function () {
  if (SOCKET_PATH) {
    if (process.platform !== 'win32' && fs.existsSync(SOCKET_PATH)) {
      fs.unlinkSync(SOCKET_PATH)
    }
    comServer.listen(SOCKET_PATH);
  } else {
    comServer.listen(PORT);
  }
};

process.on('message', function (m) {
  if (m) {
    if (m.type === 'masterMessage') {
      if (scBroker) {
        scBroker.emit('masterMessage', {data: m.data});
      } else {
        let errorMessage = `Cannot send message to broker with id ${BROKER_ID} ` +
          'because the broker was not instantiated';
        let err = new BrokerError(errorMessage);
        sendErrorToMaster(err);
      }
    } else if (m.type === 'masterRequest') {
      if (scBroker) {
        scBroker.emit('masterRequest', {
          data: m.data,
          end: (data) => {
            process.send({
              type: 'brokerResponse',
              brokerId: scBroker.id,
              data,
              rid: m.cid
            });
          },
          error: (err) => {
            process.send({
              type: 'brokerResponse',
              brokerId: scBroker.id,
              error: scErrors.dehydrateError(err, true),
              rid: m.cid
            });
          }
        });
      } else {
        let errorMessage = `Cannot send request to broker with id ${BROKER_ID} ` +
          'because the broker was not instantiated';
        let err = new BrokerError(errorMessage);
        sendErrorToMaster(err);
      }
    } else if (m.type === 'masterResponse') {
      handleMasterResponse(m);
    }
  }
});

let killServer = function () {
  comServer.close(() => {
    process.exit();
  });

  Object.values(connections || {}).forEach((conn) => {
    conn.destroy();
  });

  setTimeout(() => {
    process.exit();
  }, PROCESS_TERM_TIMEOUT);
};

process.on('SIGTERM', killServer);
process.on('disconnect', killServer);

setInterval(() => {
  let keys = dataExpirer.extractExpiredKeys();
  let len = keys.length;
  for (let i = 0; i < len; i++) {
    dataMap.remove(keys[i]);
  }
}, EXPIRY_ACCURACY);

process.on('uncaughtException', exitWithError);

module.exports = SCBroker;
