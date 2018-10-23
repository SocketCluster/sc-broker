var fork = require('child_process').fork;
var EventEmitter = require('events').EventEmitter;
var ComSocket = require('ncom').ComSocket;
var FlexiMap = require('fleximap').FlexiMap;
var uuid = require('uuid');

var scErrors = require('sc-errors');
var BrokerError = scErrors.BrokerError;
var TimeoutError = scErrors.TimeoutError;

var DEFAULT_PORT = 9435;
var HOST = '127.0.0.1';
var DEFAULT_CONNECT_RETRY_ERROR_THRESHOLD = 20;
var DEFAULT_IPC_ACK_TIMEOUT = 10000;

var Server = function (options) {
  EventEmitter.call(this);

  var defaultBrokerControllerPath = __dirname + '/default-broker-controller.js';

  var serverOptions = {
    id: options.id,
    debug: options.debug,
    socketPath: options.socketPath,
    port: options.port,
    expiryAccuracy: options.expiryAccuracy,
    downgradeToUser: options.downgradeToUser,
    brokerControllerPath: options.brokerControllerPath || defaultBrokerControllerPath,
    processTermTimeout: options.processTermTimeout
  };

  this.options = options;

  this._pendingResponseHandlers = {};

  var stringArgs = JSON.stringify(serverOptions);

  this.socketPath = options.socketPath;
  if (!this.socketPath) {
    this.port = options.port;
  }

  if (options.ipcAckTimeout == null) {
    this.ipcAckTimeout = DEFAULT_IPC_ACK_TIMEOUT;
  } else {
    this.ipcAckTimeout = options.ipcAckTimeout;
  }

  if (!options.brokerOptions) {
    options.brokerOptions = {};
  }
  options.brokerOptions.secretKey = options.secretKey;
  options.brokerOptions.instanceId = options.instanceId;

  var debugRegex = /^--debug(=[0-9]*)?$/;
  var debugBrkRegex = /^--debug-brk(=[0-9]*)?$/;
  var inspectRegex = /^--inspect(=[0-9]*)?$/;
  var inspectBrkRegex = /^--inspect-brk(=[0-9]*)?$/;

  // Brokers should not inherit the master --debug argument
  // because they have their own --debug-brokers option.
  var execOptions = {
    execArgv: process.execArgv.filter((arg) => {
      return !debugRegex.test(arg) && !debugBrkRegex.test(arg) && !inspectRegex.test(arg) && !inspectBrkRegex.test(arg);
    }),
    env: {}
  };

  Object.keys(process.env).forEach((key) => {
    execOptions.env[key] = process.env[key];
  });
  execOptions.env.brokerInitOptions = JSON.stringify(options.brokerOptions);

  if (options.debug) {
    execOptions.execArgv.push('--debug=' + options.debug);
  }
  if (options.inspect) {
    execOptions.execArgv.push('--inspect=' + options.inspect);
  }

  this._server = fork(serverOptions.brokerControllerPath, [stringArgs], execOptions);

  var formatError = (error) => {
    var err = scErrors.hydrateError(error, true);
    if (typeof err === 'object') {
      if (err.name == null || err.name === 'Error') {
        err.name = 'BrokerError';
      }
      err.brokerPid = this._server.pid;
    }
    return err;
  };

  this._server.on('error', (error) => {
    var err = formatError(error);
    this.emit('error', err);
  });

  this._server.on('message', (value) => {
    if (value.type === 'error') {
      var err = formatError(value.data);
      this.emit('error', err);
    } else if (value.type === 'brokerMessage') {
      this.emit('brokerMessage', value.brokerId, value.data);
    } else if (value.type === 'brokerRequest') {
      this.emit('brokerRequest', value.brokerId, value.data, (err, data) => {
        this._server.send({
          type: 'masterResponse',
          error: scErrors.dehydrateError(err, true),
          data: data,
          rid: value.cid
        });
      });
    } else if (value.type === 'brokerResponse') {
      var responseHandler = this._pendingResponseHandlers[value.rid];
      if (responseHandler) {
        clearTimeout(responseHandler.timeout);
        delete this._pendingResponseHandlers[value.rid];
        var properError = scErrors.hydrateError(value.error, true);
        responseHandler.callback(properError, value.data);
      }
    } else if (value.type === 'listening') {
      this.emit('ready', value.data);
    }
  });

  this._server.on('exit', (code, signal) => {
    this.emit('exit', {
      id: options.id,
      pid: this._server.pid,
      code: code,
      signal: signal
    });
  });

  this._createIPCResponseHandler = (callback) => {
    var cid = uuid.v4();

    var responseTimeout = setTimeout(() => {
      var responseHandler = this._pendingResponseHandlers[cid];
      delete this._pendingResponseHandlers[cid];
      var timeoutError = new TimeoutError('IPC response timed out');
      responseHandler.callback(timeoutError);
    }, this.ipcAckTimeout);

    this._pendingResponseHandlers[cid] = {
      callback: callback,
      timeout: responseTimeout
    };

    return cid;
  };
};

Server.prototype = Object.create(EventEmitter.prototype);

Server.prototype.sendMessageToBroker = function (data) {
  var messagePacket = {
    type: 'masterMessage',
    data: data
  };
  this._server.send(messagePacket);
  return Promise.resolve();
};

Server.prototype.sendRequestToBroker = function (data) {
  return new Promise((resolve, reject) => {
    var messagePacket = {
      type: 'masterRequest',
      data: data
    };
    messagePacket.cid = this._createIPCResponseHandler((err, result) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(result);
    });
    this._server.send(messagePacket);
  });
};

Server.prototype.destroy = function () {
  this._server.kill('SIGTERM');
};

module.exports.createServer = function (options) {
  if (!options) {
    options = {};
  }
  if (!options.socketPath && !options.port) {
    options.port = DEFAULT_PORT;
  }
  return new Server(options);
};

var Client = function (options) {
  var secretKey = options.secretKey || null;
  var timeout = options.timeout;

  this.socketPath = options.socketPath;
  this.port = options.port;
  this.host = options.host;

  if (options.autoReconnect == null) {
    this.autoReconnect = true;
  } else {
    this.autoReconnect = options.autoReconnect;
  }

  if (this.autoReconnect) {
    if (options.autoReconnectOptions == null) {
      options.autoReconnectOptions = {};
    }

    var reconnectOptions = options.autoReconnectOptions;
    if (reconnectOptions.initialDelay == null) {
      reconnectOptions.initialDelay = 200;
    }
    if (reconnectOptions.randomness == null) {
      reconnectOptions.randomness = 100;
    }
    if (reconnectOptions.multiplier == null) {
      reconnectOptions.multiplier = 1.3;
    }
    if (reconnectOptions.maxDelay == null) {
      reconnectOptions.maxDelay = 1000;
    }
    this.autoReconnectOptions = reconnectOptions;
  }

  if (options.connectRetryErrorThreshold == null) {
    this.connectRetryErrorThreshold = DEFAULT_CONNECT_RETRY_ERROR_THRESHOLD;
  } else {
    this.connectRetryErrorThreshold = options.connectRetryErrorThreshold;
  }

  this.CONNECTED = 'connected';
  this.CONNECTING = 'connecting';
  this.DISCONNECTED = 'disconnected';

  this.state = this.DISCONNECTED;

  if (timeout) {
    this._timeout = timeout;
  } else {
    this._timeout = 10000;
  }

  // Only keeps track of the intention of subscription, not the actual state.
  this._subscriptionMap = {};
  this._commandTracker = {};
  this._pendingBuffer = [];
  this._pendingSubscriptionBuffer = [];

  this.connectAttempts = 0;
  this.pendingReconnect = false;
  this.pendingReconnectTimeout = null;

  var createSocket = () => {
    if (this._socket) {
      this._socket.removeAllListeners();
    }

    this._socket = new ComSocket();
    if (options.pubSubBatchDuration != null) {
      this._socket.batchDuration = options.pubSubBatchDuration;
    }

    this._socket.on('connect', this._connectHandler);
    this._socket.on('error', (err) => {
      var isConnectionFailure = err.code === 'ENOENT' || err.code === 'ECONNREFUSED';
      var isBelowRetryThreshold = this.connectAttempts < this.connectRetryErrorThreshold;

      // We can tolerate a few missed reconnections without emitting a full error.
      if (isConnectionFailure && isBelowRetryThreshold && err.address === options.socketPath) {
        this.emit('warning', err);
      } else {
        this.emit('error', err);
      }
    });
    this._socket.on('close', handleDisconnection);
    this._socket.on('end', handleDisconnection);
    this._socket.on('message', (packet) => {
      var id = packet.id;
      var rawError = packet.error;
      var error = null;
      if (rawError != null) {
        error = scErrors.hydrateError(rawError, true);
      }
      if (packet.type === 'response') {
        if (this._commandTracker.hasOwnProperty(id)) {
          clearTimeout(this._commandTracker[id].timeout);
          var action = packet.action;

          var callback = this._commandTracker[id].callback;
          delete this._commandTracker[id];

          if (packet.value !== undefined) {
            callback(error, packet.value);
          } else {
            callback(error);
          }
        }
      } else if (packet.type === 'message') {
        // Emit the event on the next tick so that it does not disrupt
        // the execution order. This is necessary because other parts of the
        // code (such as the subscribe call) return Promises which resolve
        // on the next tick.
        setTimeout(() => {
          this.emit('message', packet.channel, packet.value);
        }, 0);
      }
    });
  };

  this._curID = 1;
  this.MAX_ID = Math.pow(2, 53) - 2;

  this.setMaxListeners(0);

  this._tryReconnect = (initialDelay) => {
    var exponent = this.connectAttempts++;
    var reconnectOptions = this.autoReconnectOptions;
    var timeout;

    if (initialDelay == null || exponent > 0) {
      var initialTimeout = Math.round(reconnectOptions.initialDelay + (reconnectOptions.randomness || 0) * Math.random());

      timeout = Math.round(initialTimeout * Math.pow(reconnectOptions.multiplier, exponent));
    } else {
      timeout = initialDelay;
    }

    if (timeout > reconnectOptions.maxDelay) {
      timeout = reconnectOptions.maxDelay;
    }

    clearTimeout(this._reconnectTimeoutRef);

    this.pendingReconnect = true;
    this.pendingReconnectTimeout = timeout;
    this._reconnectTimeoutRef = setTimeout(() => {
      this._connect();
    }, timeout);
  };

  this._genID = () => {
    this._curID = (this._curID + 1) % this.MAX_ID;
    return 'n' + this._curID;
  };

  this._flushPendingSubscriptionBuffers = () => {
    var subBufLen = this._pendingSubscriptionBuffer.length;
    for (var i = 0; i < subBufLen; i++) {
      var subCommandData = this._pendingSubscriptionBuffer[i];
      this._execCommand(subCommandData.command, subCommandData.options);
    }
    this._pendingSubscriptionBuffer = [];
  };

  this._flushPendingBuffers = () => {
    this._flushPendingSubscriptionBuffers();

    var bufLen = this._pendingBuffer.length;
    for (var j = 0; j < bufLen; j++) {
      var commandData = this._pendingBuffer[j];
      this._execCommand(commandData.command, commandData.options);
    }
    this._pendingBuffer = [];
  };

  this._flushPendingSubscriptionBuffersIfConnected = () => {
    if (this.state === this.CONNECTED) {
      this._flushPendingSubscriptionBuffers();
    }
  };

  this._flushPendingBuffersIfConnected = () => {
    if (this.state === this.CONNECTED) {
      this._flushPendingBuffers();
    }
  };

  this._prepareAndTrackCommand = (command, callback) => {
    if (command.noAck) {
      callback();
      return;
    }
    command.id = this._genID();
    var request = {callback: callback};
    this._commandTracker[command.id] = request;

    request.timeout = setTimeout(() => {
      var error = new TimeoutError('Broker Error - The ' + command.action + ' action timed out');
      delete request.callback;
      if (this._commandTracker.hasOwnProperty(command.id)) {
        delete this._commandTracker[command.id];
      }
      callback(error);
    }, this._timeout);
  };

  this._bufferSubscriptionCommand = (command, callback, options) => {
    this._prepareAndTrackCommand(command, callback);
    var commandData = {
      command: command,
      options: options
    };
    this._pendingSubscriptionBuffer.push(commandData);
  };

  this._bufferCommand = (command, callback, options) => {
    this._prepareAndTrackCommand(command, callback);
    // Clone the command argument to prevent the user from modifying the data
    // whilst the command is still pending in the buffer.
    var commandData = {
      command: JSON.parse(JSON.stringify(command)),
      options: options
    };
    this._pendingBuffer.push(commandData);
  };

  this._processCommand = (command, execOptions) => {
    return new Promise((resolve, reject) => {
      this._connect();
      this._bufferCommand(command, (err, result) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(result);
      }, execOptions);
      this._flushPendingBuffersIfConnected();
    });
  };

  // Recovers subscriptions after Broker server crash
  this._resubscribeAll = () => {
    var subscribePromises = Object.keys(this._subscriptionMap || {}).map((channel) => {
      return this.subscribe(channel)
      .catch((err) => {
        var errorMessage = err.message || err;
        this.emit('error', new BrokerError('Failed to resubscribe to broker channel - ' + errorMessage));
      });
    });
    return Promise.all(subscribePromises);
  };

  this._connectHandler = () => {
    var command = {
      action: 'init',
      secretKey: secretKey
    };
    var initHandler = (err, brokerInfo) => {
      if (err) {
        this.emit('error', err);
      } else {
        this.state = this.CONNECTED;
        this.connectAttempts = 0;
        this._resubscribeAll()
        .then(() => {
          this._flushPendingBuffers();
          this.emit('ready', brokerInfo);
        });
      }
    };
    this._prepareAndTrackCommand(command, initHandler);
    this._execCommand(command);
  };

  this._connect = () => {
    if (this.state === this.DISCONNECTED) {
      this.pendingReconnect = false;
      this.pendingReconnectTimeout = null;
      clearTimeout(this._reconnectTimeoutRef);
      this.state = this.CONNECTING;
      createSocket();

      if (this.socketPath) {
        this._socket.connect(this.socketPath);
      } else {
        this._socket.connect(this.port, this.host);
      }
    }
  };

  var handleDisconnection = () => {
    this.state = this.DISCONNECTED;
    this.pendingReconnect = false;
    this.pendingReconnectTimeout = null;
    clearTimeout(this._reconnectTimeoutRef);
    this._pendingBuffer = [];
    this._pendingSubscriptionBuffer = [];
    this._tryReconnect();
  };

  this._connect();

  this._execCommand = (command, options) => {
    this._socket.write(command, options);
  };

  this._getPubSubExecOptions = () => {
    var execOptions = {};
    if (options.pubSubBatchDuration != null) {
      execOptions.batch = true;
    }
    return execOptions;
  };

  this._stringifyQuery = (query, data) => {
    query = query.toString();

    var validVarNameRegex = /^[a-zA-Z_$][a-zA-Z0-9_$]*$/;
    var headerString = '';

    Object.keys(data || {}).forEach((i) => {
      if (!validVarNameRegex.test(i)) {
        throw new BrokerError("The variable name '" + i + "' is invalid");
      }
      headerString += 'var ' + i + '=' + JSON.stringify(data[i]) + ';';
    });

    query = query.replace(/^(function *[(][^)]*[)] *{)/, (match) => {
      return match + headerString;
    });

    return query;
  };
};

Client.prototype = Object.create(EventEmitter.prototype);

Client.prototype.isConnected = function () {
  return this.state === this.CONNECTED;
};

Client.prototype.extractKeys = function (object) {
  return Object.keys(object);
};

Client.prototype.extractValues = function (object) {
  return Object.keys(object || {}).map((key) => {
    return object[key];
  });
};

Client.prototype.subscribe = function (channel) {
  return new Promise((resolve, reject) => {
    this._subscriptionMap[channel] = true;

    var command = {
      action: 'subscribe',
      channel: channel
    };
    var callback = (err) => {
      if (err) {
        delete this._subscriptionMap[channel];
        reject(err);
        return;
      }
      resolve();
    };
    var execOptions = this._getPubSubExecOptions();

    this._connect();
    this._bufferSubscriptionCommand(command, callback, execOptions);
    this._flushPendingSubscriptionBuffersIfConnected();
  });
};

Client.prototype.unsubscribe = function (channel) {
  return new Promise((resolve, reject) => {
    delete this._subscriptionMap[channel];
    if (this.state === this.CONNECTED) {
      var command = {
        action: 'unsubscribe',
        channel: channel
      };

      var callback = (err) => {
        // Unsubscribe can never fail because TCP guarantees
        // delivery for the life of the connection. If the
        // connection fails then all subscriptions
        // will be cleared automatically anyway.
        resolve();
      };

      var execOptions = this._getPubSubExecOptions();
      this._bufferSubscriptionCommand(command, callback, execOptions);
      this._flushPendingSubscriptionBuffersIfConnected();
    } else {
      // No need to unsubscribe if the server is disconnected
      // The server cleans up automatically in case of disconnection
      resolve();
    }
  });
};

Client.prototype.subscriptions = function () {
  var command = {
    action: 'subscriptions'
  };

  var execOptions = this._getPubSubExecOptions();
  return this._processCommand(command, execOptions);
};

Client.prototype.isSubscribed = function (channel) {
  var command = {
    action: 'isSubscribed',
    channel: channel
  };

  var execOptions = this._getPubSubExecOptions();
  return this._processCommand(command, execOptions);
};

Client.prototype.publish = function (channel, value) {
  var command = {
    action: 'publish',
    channel: channel,
    value: value,
    getValue: 1
  };

  var execOptions = this._getPubSubExecOptions();
  return this._processCommand(command, execOptions);
};

Client.prototype.sendRequest = function (data) {
  var command = {
    action: 'sendRequest',
    value: data
  };

  return this._processCommand(command);
};

Client.prototype.sendMessage = function (data) {
  var command = {
    action: 'sendMessage',
    value: data,
    noAck: 1
  };

  return this._processCommand(command);
};

/*
  set(key, value,[ options])
  Returns a Promise.
*/
Client.prototype.set = function (key, value, options) {
  var command = {
    action: 'set',
    key: key,
    value: value
  };

  if (options && options.getValue) {
    command.getValue = 1;
  }

  return this._processCommand(command);
};

/*
  expire(keys, seconds)
  Returns a Promise.
*/
Client.prototype.expire = function (keys, seconds) {
  var command = {
    action: 'expire',
    keys: keys,
    value: seconds
  };
  return this._processCommand(command);
};

/*
  unexpire(keys)
  Returns a Promise.
*/
Client.prototype.unexpire = function (keys) {
  var command = {
    action: 'unexpire',
    keys: keys
  };
  return this._processCommand(command);
};

/*
  getExpiry(key)
  Returns a Promise.
*/
Client.prototype.getExpiry = function (key) {
  var command = {
    action: 'getExpiry',
    key: key
  };
  return this._processCommand(command);
};

/*
  add(key, value)
  Returns a Promise.
*/
Client.prototype.add = function (key, value) {
  var command = {
    action: 'add',
    key: key,
    value: value
  };

  return this._processCommand(command);
};

/*
  concat(key, value,[ options])
  Returns a Promise.
*/
Client.prototype.concat = function (key, value, options) {
  var command = {
    action: 'concat',
    key: key,
    value: value
  };

  if (options && options.getValue) {
    command.getValue = 1;
  }

  return this._processCommand(command);
};

/*
  get(key)
  Returns a Promise.
*/
Client.prototype.get = function (key) {
  var command = {
    action: 'get',
    key: key
  };

  return this._processCommand(command);
};

/*
  getRange(key, options)
  Returns a Promise.
*/
Client.prototype.getRange = function (key, options) {
  var command = {
    action: 'getRange',
    key: key
  };

  if (options) {
    if (options.fromIndex != null) {
      command.fromIndex = options.fromIndex;
    }
    if (options.toIndex != null) {
      command.toIndex = options.toIndex;
    }
  }

  return this._processCommand(command);
};

/*
  getAll()
  Returns a Promise.
*/
Client.prototype.getAll = function () {
  var command = {
    action: 'getAll'
  };
  return this._processCommand(command);
};

/*
  count(key)
  Returns a Promise.
*/
Client.prototype.count = function (key) {
  var command = {
    action: 'count',
    key: key
  };
  return this._processCommand(command);
};

/*
  registerDeathQuery(query,[ data])
  Returns a Promise.
*/
Client.prototype.registerDeathQuery = function (query, data) {
  if (!data) {
    data = query.data || {};
  }

  query = this._stringifyQuery(query, data);

  var command = {
    action: 'registerDeathQuery',
    value: query
  };
  return this._processCommand(command);
};

/*
  exec(query,[ options])
  Returns a Promise.
*/
Client.prototype.exec = function (query, options) {
  options = options || {};
  var data;

  if (options.data) {
    data = options.data;
  } else {
    data = query.data || {};
  }

  query = this._stringifyQuery(query, data);

  var command = {
    action: 'exec',
    value: query
  };

  if (options.baseKey) {
    command.baseKey = options.baseKey;
  }
  if (options.noAck) {
    command.noAck = options.noAck;
  }

  return this._processCommand(command);
};

/*
  query(query,[ data])
  Returns a Promise.
*/
Client.prototype.query = function (query, data) {
  var options = {
    data: data
  };
  return this.exec(query, options);
};

/*
  remove(key,[ options])
  Returns a Promise.
*/
Client.prototype.remove = function (key, options) {
  var command = {
    action: 'remove',
    key: key
  };

  if (options) {
    if (options.getValue) {
      command.getValue = 1;
    }
    if (options.noAck) {
      command.noAck = 1;
    }
  }

  return this._processCommand(command);
};

/*
  removeRange(key,[ options])
  Returns a Promise.
*/
Client.prototype.removeRange = function (key, options) {
  var command = {
    action: 'removeRange',
    key: key
  };

  if (options) {
    if (options.fromIndex != null) {
      command.fromIndex = options.fromIndex;
    }
    if (options.toIndex != null) {
      command.toIndex = options.toIndex;
    }
    if (options.getValue) {
      command.getValue = 1;
    }
    if (options.noAck) {
      command.noAck = 1;
    }
  }

  return this._processCommand(command);
};

/*
  removeAll()
  Returns a Promise.
*/
Client.prototype.removeAll = function () {
  var command = {
    action: 'removeAll'
  };
  return this._processCommand(command);
};

/*
  splice(key,[ options])
  The following options are supported:
  - fromIndex // Start index
  - count // Number of items to delete
  - items // Must be an Array of items to insert as part of splice
  Returns a Promise.
*/
Client.prototype.splice = function (key, options) {
  var command = {
    action: 'splice',
    key: key
  };

  if (options) {
    if (options.fromIndex != null) {
      command.fromIndex = options.fromIndex;
    }
    if (options.count != null) {
      command.count = options.count;
    }
    if (options.items != null) {
      command.items = options.items;
    }
    if (options.getValue) {
      command.getValue = 1;
    }
    if (options.noAck) {
      command.noAck = 1;
    }
  }

  return this._processCommand(command);
};

/*
  pop(key,[ options])
  Returns a Promise.
*/
Client.prototype.pop = function (key, options) {
  var command = {
    action: 'pop',
    key: key
  };

  if (options) {
    if (options.getValue) {
      command.getValue = 1;
    }
    if (options.noAck) {
      command.noAck = 1;
    }
  }

  return this._processCommand(command);
};

/*
  hasKey(key)
  Returns a Promise.
*/
Client.prototype.hasKey = function (key) {
  var command = {
    action: 'hasKey',
    key: key
  };
  return this._processCommand(command);
};

/*
  end()
  Returns a Promise.
*/
Client.prototype.end = function () {
  clearTimeout(this._reconnectTimeoutRef);
  return this.unsubscribe()
  .then(() => {
    return new Promise((resolve, reject) => {
      var disconnectCallback = () => {
        if (disconnectTimeout) {
          clearTimeout(disconnectTimeout);
        }
        setTimeout(() => {
          resolve();
        }, 0);
        this._socket.removeListener('end', disconnectCallback);
      };

      var disconnectTimeout = setTimeout(() => {
        this._socket.removeListener('end', disconnectCallback);
        var error = new TimeoutError('Disconnection timed out');
        reject(error);
      }, this._timeout);

      if (this._socket.connected) {
        this._socket.on('end', disconnectCallback);
      } else {
        disconnectCallback();
      }
      var setDisconnectStatus = () => {
        this._socket.removeListener('end', setDisconnectStatus);
        this.state = this.DISCONNECTED;
      };
      if (this._socket.connected) {
        this._socket.on('end', setDisconnectStatus);
        this._socket.end();
      } else {
        this._socket.destroy();
        this.state = this.DISCONNECTED;
      }
    });
  });
};

module.exports.createClient = function (options) {
  if (!options) {
    options = {};
  }
  if (!options.socketPath && !options.port) {
    options.port = DEFAULT_PORT;
  }
  if (!options.socketPath && !options.host) {
    options.host = HOST;
  }
  return new Client(options);
};
