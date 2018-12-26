const fork = require('child_process').fork;
const AsyncStreamEmitter = require('async-stream-emitter');
const ComSocket = require('ncom').ComSocket;
const FlexiMap = require('fleximap').FlexiMap;
const uuid = require('uuid');

const scErrors = require('sc-errors');
const BrokerError = scErrors.BrokerError;
const TimeoutError = scErrors.TimeoutError;

const DEFAULT_PORT = 9435;
const HOST = '127.0.0.1';
const DEFAULT_CONNECT_RETRY_ERROR_THRESHOLD = 20;
const DEFAULT_IPC_ACK_TIMEOUT = 10000;
const DEFAULT_COMMAND_ACK_TIMEOUT = 10000;
const DEFAULT_PUB_SUB_ACK_TIMEOUT = 4000;

function Server(options) {
  AsyncStreamEmitter.call(this);

  let defaultBrokerControllerPath = __dirname + '/default-broker-controller.js';

  let serverOptions = {
    brokerId: options.brokerId,
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

  let stringArgs = JSON.stringify(serverOptions);

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

  let debugRegex = /^--debug(=[0-9]*)?$/;
  let debugBrkRegex = /^--debug-brk(=[0-9]*)?$/;
  let inspectRegex = /^--inspect(=[0-9]*)?$/;
  let inspectBrkRegex = /^--inspect-brk(=[0-9]*)?$/;

  // Brokers should not inherit the master --debug argument
  // because they have their own --debug-brokers option.
  let execOptions = {
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

  let formatError = (error) => {
    let err = scErrors.hydrateError(error, true);
    if (typeof err === 'object') {
      if (err.name == null || err.name === 'Error') {
        err.name = 'BrokerError';
      }
      err.brokerPid = this._server.pid;
    }
    return err;
  };

  this._server.on('error', (err) => {
    let error = formatError(err);
    this.emit('error', {error});
  });

  this._server.on('message', (value) => {
    if (value.type === 'error') {
      let error = formatError(value.data);
      this.emit('error', {error});
    } else if (value.type === 'brokerMessage') {
      this.emit('brokerMessage', {data: value.data});
    } else if (value.type === 'brokerRequest') {
      this.emit('brokerRequest', {
        brokerId: value.brokerId,
        data: value.data,
        end: (data) => {
          this._server.send({
            type: 'masterResponse',
            data,
            rid: value.cid
          });
        },
        error: (err) => {
          this._server.send({
            type: 'masterResponse',
            error: scErrors.dehydrateError(err, true),
            rid: value.cid
          });
        }
      });
    } else if (value.type === 'brokerResponse') {
      let responseHandler = this._pendingResponseHandlers[value.rid];
      if (responseHandler) {
        clearTimeout(responseHandler.timeout);
        delete this._pendingResponseHandlers[value.rid];
        let properError = scErrors.hydrateError(value.error, true);
        responseHandler.callback(properError, value.data);
      }
    } else if (value.type === 'listening') {
      this.emit('ready', value.data);
    }
  });

  this._server.on('exit', (code, signal) => {
    this.emit('exit', {
      brokerId: options.brokerId,
      pid: this._server.pid,
      code,
      signal
    });
  });

  this._createIPCResponseHandler = (callback) => {
    let cid = uuid.v4();

    let responseTimeout = setTimeout(() => {
      let responseHandler = this._pendingResponseHandlers[cid];
      delete this._pendingResponseHandlers[cid];
      let timeoutError = new TimeoutError('IPC response timed out');
      responseHandler.callback(timeoutError);
    }, this.ipcAckTimeout);

    this._pendingResponseHandlers[cid] = {
      callback,
      timeout: responseTimeout
    };

    return cid;
  };
}

Server.prototype = Object.create(AsyncStreamEmitter.prototype);

Server.prototype.sendMessageToBroker = function (data) {
  let messagePacket = {
    type: 'masterMessage',
    data
  };
  this._server.send(messagePacket);
  return Promise.resolve();
};

Server.prototype.sendRequestToBroker = function (data) {
  return new Promise((resolve, reject) => {
    let messagePacket = {
      type: 'masterRequest',
      data
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

function Client(options) {
  AsyncStreamEmitter.call(this);

  let secretKey = options.secretKey || null;
  this._commandTimeout = options.commandAckTimeout == null ?
    DEFAULT_COMMAND_ACK_TIMEOUT : options.commandAckTimeout;
  this._pubSubTimeout = options.pubSubAckTimeout == null ?
    DEFAULT_PUB_SUB_ACK_TIMEOUT : options.pubSubAckTimeout;

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

    let reconnectOptions = options.autoReconnectOptions;
    if (reconnectOptions.initialDelay == null) {
      reconnectOptions.initialDelay = 100;
    }
    if (reconnectOptions.randomness == null) {
      reconnectOptions.randomness = 100;
    }
    if (reconnectOptions.multiplier == null) {
      reconnectOptions.multiplier = 1.1;
    }
    if (reconnectOptions.maxDelay == null) {
      reconnectOptions.maxDelay = 500;
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

  // Only keeps track of the intention of subscription, not the actual state.
  this._subscriptionMap = {};
  this._pendingSubscriptionMap = {};
  this._commandTracker = {};
  this._pendingBuffer = [];

  this.connectAttempts = 0;
  this.pendingReconnect = false;
  this.pendingReconnectTimeout = null;

  let createSocket = () => {
    if (this._socket) {
      this._socket.removeAllListeners();
    }

    this._socket = new ComSocket();
    if (options.pubSubBatchDuration != null) {
      this._socket.batchDuration = options.pubSubBatchDuration;
    }

    this._socket.on('connect', this._connectHandler);
    this._socket.on('error', (error) => {
      let isConnectionFailure = error.code === 'ENOENT' || error.code === 'ECONNREFUSED';
      let isBelowRetryThreshold = this.connectAttempts < this.connectRetryErrorThreshold;

      // We can tolerate a few missed reconnections without emitting a full error.
      if (isConnectionFailure && isBelowRetryThreshold && error.address === options.socketPath) {
        this.emit('warning', {warning: error});
      } else {
        this.emit('error', {error});
      }
    });
    this._socket.on('close', handleDisconnection);
    this._socket.on('end', handleDisconnection);
    this._socket.on('message', (packet) => {
      let id = packet.id;
      let rawError = packet.error;
      let error = null;
      if (rawError != null) {
        error = scErrors.hydrateError(rawError, true);
      }
      if (packet.type === 'response') {
        if (this._commandTracker.hasOwnProperty(id)) {
          clearTimeout(this._commandTracker[id].timeout);
          let action = packet.action;

          let callback = this._commandTracker[id].callback;
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
          this.emit('message', {channel: packet.channel, data: packet.value});
        }, 0);
      }
    });
  };

  this._curID = 1;
  this.MAX_ID = Math.pow(2, 53) - 2;

  this._tryReconnect = (initialDelay) => {
    let exponent = this.connectAttempts++;
    let reconnectOptions = this.autoReconnectOptions;
    let timeout;

    if (initialDelay == null || exponent > 0) {
      let initialTimeout = Math.round(reconnectOptions.initialDelay + (reconnectOptions.randomness || 0) * Math.random());

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

  this._flushPendingSubscriptionMap = () => {
    Object.values(this._pendingSubscriptionMap).forEach((commandData) => {
      this._execCommand(commandData.command, commandData.options);
    });
  };

  this._flushPendingBuffers = () => {
    this._flushPendingSubscriptionMap();

    let len = this._pendingBuffer.length;
    for (let j = 0; j < len; j++) {
      let commandData = this._pendingBuffer[j];
      this._execCommand(commandData.command, commandData.options);
    }
    this._pendingBuffer = [];
  };

  this._flushPendingSubscriptionMapIfConnected = () => {
    if (this.state === this.CONNECTED) {
      this._flushPendingSubscriptionMap();
    }
  };

  this._flushPendingBuffersIfConnected = () => {
    if (this.state === this.CONNECTED) {
      this._flushPendingBuffers();
    }
  };

  this._prepareAndTrackCommand = (command, callback, options) => {
    if (command.noAck) {
      callback();
      return;
    }
    command.id = this._genID();
    let request = {
      action: command.action,
      callback
    };
    this._commandTracker[command.id] = request;

    if (!options || !options.noTimeout) {
      let timeout = options && options.ackTimeout != null ?
      options.ackTimeout : this._commandTimeout;

      request.timeout = setTimeout(() => {
        let error = new TimeoutError(
          `Broker Error - The ${command.action} action timed out`
        );
        delete this._commandTracker[command.id];
        callback(error);
      }, timeout);
    }
  };

  this._bufferCommand = (command, callback, options) => {
    this._prepareAndTrackCommand(command, callback, options);
    // Clone the command argument to prevent the user from modifying the data
    // whilst the command is still pending in the buffer.
    let commandData = {
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
  this._resubscribeAll = async () => {
    let subscribePromises = Object.keys(this._subscriptionMap || {})
    .map(async (channel) => {
      try {
        await this.subscribe(channel);
      } catch (err) {
        let errorMessage = err.message || err;
        this.emit('error', {
          error: new BrokerError(
            `Failed to resubscribe to broker channel - ${errorMessage}`
          )
        });
      }
    });
    await Promise.all(subscribePromises);
  };

  this._connectHandler = () => {
    let command = {
      action: 'init',
      secretKey: secretKey
    };
    let initHandler = async (error, brokerInfo) => {
      if (error) {
        this.emit('error', {error});
      } else {
        this.state = this.CONNECTED;
        this.connectAttempts = 0;
        await this._resubscribeAll();
        this._flushPendingBuffers();
        this.emit('ready', brokerInfo);
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

  let handleDisconnection = () => {
    this.state = this.DISCONNECTED;
    this.pendingReconnect = false;
    this.pendingReconnectTimeout = null;
    clearTimeout(this._reconnectTimeoutRef);
    this._tryReconnect();
  };

  this._connect();

  this._execCommand = (command, options) => {
    this._socket.write(command, options);
  };

  this._getPubSubExecOptions = () => {
    let execOptions = {};
    if (options.pubSubBatchDuration != null) {
      execOptions.batch = true;
    }
    execOptions.ackTimeout = this._pubSubTimeout;
    return execOptions;
  };

  this._stringifyQuery = (query, data) => {
    query = query.toString();

    let validVarNameRegex = /^[a-zA-Z_$][a-zA-Z0-9_$]*$/;
    let headerString = '';

    Object.keys(data || {}).forEach((i) => {
      if (!validVarNameRegex.test(i)) {
        throw new BrokerError(`The variable name "${i}" is invalid`);
      }
      headerString += `let ${i}=${JSON.stringify(data[i])};`;
    });

    query = query.replace(/^(function *[(][^)]*[)] *{)/, (match) => {
      return match + headerString;
    });

    return query;
  };
}

Client.prototype = Object.create(AsyncStreamEmitter.prototype);

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
    let command = {
      action: 'subscribe',
      channel
    };
    let callback = (err) => {
      if (err) {
        reject(err);
        return;
      }
      resolve();
    };
    let execOptions = this._getPubSubExecOptions();

    let existingCommandData = this._pendingSubscriptionMap[channel];
    if (existingCommandData) {
      existingCommandData.callbacks.push(callback);
    } else {
      this._pendingSubscriptionMap[channel] = {
        command,
        options: execOptions,
        callbacks: [callback]
      };
      this._prepareAndTrackCommand(command, (err, data) => {
        let commandData = this._pendingSubscriptionMap[channel];
        if (commandData) {
          if (err) {
            delete this._subscriptionMap[channel];
          }
          delete this._pendingSubscriptionMap[channel];
          commandData.callbacks.forEach((callback) => {
            callback(err, data);
          });
        }
      }, {noTimeout: true});
    }

    this._connect();
    this._flushPendingSubscriptionMapIfConnected();
  });
};

Client.prototype.unsubscribe = function (channel) {
  return new Promise((resolve, reject) => {
    delete this._subscriptionMap[channel];
    delete this._pendingSubscriptionMap[channel];

    if (this.state === this.CONNECTED) {
      let command = {
        action: 'unsubscribe',
        channel
      };
      let execOptions = this._getPubSubExecOptions();
      this._execCommand(command, execOptions);
    }

    // Unsubscribe can never fail because TCP guarantees
    // delivery for the life of the connection. If the
    // connection fails then all subscriptions
    // will be cleared automatically anyway.
    resolve();
  });
};

Client.prototype.subscriptions = function (includePending) {
  let subscriptions = Object.keys(this._subscriptionMap);
  if (includePending) {
    return subscriptions;
  }
  return subscriptions.filter((channel) => {
    return !this._pendingSubscriptionMap[channel];
  });
};

Client.prototype.isSubscribed = function (channel, includePending) {
  if (includePending) {
    return this._subscriptionMap.hasOwnProperty(channel);
  }
  return !!this._subscriptionMap[channel] && !this._pendingSubscriptionMap[channel];
};

Client.prototype.publish = function (channel, value) {
  let command = {
    action: 'publish',
    channel,
    value,
    getValue: 1
  };

  let execOptions = this._getPubSubExecOptions();
  return this._processCommand(command, execOptions);
};

Client.prototype.sendRequest = function (data) {
  let command = {
    action: 'sendRequest',
    value: data
  };

  return this._processCommand(command);
};

Client.prototype.sendMessage = function (data) {
  let command = {
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
  let command = {
    action: 'set',
    key,
    value
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
  let command = {
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
  let command = {
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
  let command = {
    action: 'getExpiry',
    key
  };
  return this._processCommand(command);
};

/*
  add(key, value)
  Returns a Promise.
*/
Client.prototype.add = function (key, value) {
  let command = {
    action: 'add',
    key,
    value
  };

  return this._processCommand(command);
};

/*
  concat(key, value,[ options])
  Returns a Promise.
*/
Client.prototype.concat = function (key, value, options) {
  let command = {
    action: 'concat',
    key,
    value
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
  let command = {
    action: 'get',
    key
  };

  return this._processCommand(command);
};

/*
  getRange(key, options)
  Returns a Promise.
*/
Client.prototype.getRange = function (key, options) {
  let command = {
    action: 'getRange',
    key
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
  let command = {
    action: 'getAll'
  };
  return this._processCommand(command);
};

/*
  count(key)
  Returns a Promise.
*/
Client.prototype.count = function (key) {
  let command = {
    action: 'count',
    key
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

  let command = {
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
  let data;

  if (options.data) {
    data = options.data;
  } else {
    data = query.data || {};
  }

  query = this._stringifyQuery(query, data);

  let command = {
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
  let options = {data};
  return this.exec(query, options);
};

/*
  remove(key,[ options])
  Returns a Promise.
*/
Client.prototype.remove = function (key, options) {
  let command = {
    action: 'remove',
    key
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
  let command = {
    action: 'removeRange',
    key
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
  let command = {
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
  let command = {
    action: 'splice',
    key
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
  let command = {
    action: 'pop',
    key
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
  let command = {
    action: 'hasKey',
    key
  };
  return this._processCommand(command);
};

/*
  end()
  Returns a Promise.
*/
Client.prototype.end = async function () {
  clearTimeout(this._reconnectTimeoutRef);
  await this.unsubscribe();
  return new Promise((resolve, reject) => {
    let disconnectCallback = () => {
      if (disconnectTimeout) {
        clearTimeout(disconnectTimeout);
      }
      setTimeout(() => {
        resolve();
      }, 0);
      this._socket.removeListener('end', disconnectCallback);
    };

    let disconnectTimeout = setTimeout(() => {
      this._socket.removeListener('end', disconnectCallback);
      let error = new TimeoutError('Disconnection timed out');
      reject(error);
    }, this._commandTimeout);

    if (this._socket.connected) {
      this._socket.on('end', disconnectCallback);
    } else {
      disconnectCallback();
    }
    let setDisconnectStatus = () => {
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
