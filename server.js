var args = JSON.parse(process.argv[2]);

var PORT;
if (args.port) {
  PORT = parseInt(args.port);
}
var BROKER_ID = args.id;
var SOCKET_PATH = args.socketPath;
var EXPIRY_ACCURACY = args.expiryAccuracy || 1000;
var BROKER_CONTROLLER_PATH = args.brokerControllerPath;
var INIT_CONTROLLER_PATH = args.initControllerPath || null;
var DOWNGRADE_TO_USER = args.downgradeToUser;
var PROCESS_TERM_TIMEOUT = args.processTermTimeout || 10000;
var DEBUG_PORT = args.debug || null;
var INIT_CONTROLLER = null;
var BROKER_CONTROLLER = null;

var EventEmitter = require('events').EventEmitter;

var fs = require('fs');
var domain = require('sc-domain');
var com = require('ncom');
var ExpiryManager = require('expirymanager').ExpiryManager;
var FlexiMap = require('fleximap').FlexiMap;

var initialized = {};

var errorHandler = function (err) {
  var error;

  if (err.stack) {
    error = {
      message: err.message,
      stack: err.stack
    };
  } else {
    error = err;
  }

  process.send({event: 'error', data: error});
};

// errorDomain handles non-fatal errors.
var errorDomain = domain.create();
errorDomain.on('error', errorHandler);

if (DOWNGRADE_TO_USER && process.setuid) {
  try {
    process.setuid(DOWNGRADE_TO_USER);
  } catch (err) {
    errorDomain.emit('error', new Error('Could not downgrade to user "' + DOWNGRADE_TO_USER +
      '" - Either this user does not exist or the current process does not have the permission' +
      ' to switch to it.'));
  }
}

var send = function (socket, object, options) {
  socket.write(object, options);
};

var dataMap = new FlexiMap();
var subscriptions = {};

var dataExpirer = new ExpiryManager();

var addListener = function (socket, channel) {
  if (subscriptions[socket.id] == null) {
    subscriptions[socket.id] = {};
  }
  subscriptions[socket.id][channel] = socket;
};

var hasListener = function (socket, channel) {
  return !!(subscriptions[socket.id] && subscriptions[socket.id][channel]);
};

var anyHasListener = function (channel) {
  for (var i in subscriptions) {
    if (subscriptions.hasOwnProperty(i)) {
      if (subscriptions[i][channel]) {
        return true;
      }
    }
  }
  return false;
};

var removeListener = function (socket, channel) {
  if (subscriptions[socket.id]) {
    delete subscriptions[socket.id][channel];
  }
};

var removeAllListeners = function (socket) {
  var subMap = subscriptions[socket.id];
  var channels = [];
  for (var i in subMap) {
    if (subMap.hasOwnProperty(i)) {
      channels.push(i);
    }
  }
  delete subscriptions[socket.id];
  return channels;
};

var run = function (query, baseKey) {
  var rebasedDataMap;
  if (baseKey) {
    rebasedDataMap = dataMap.getRaw(baseKey);
  } else {
    rebasedDataMap = dataMap;
  }

  return Function('"use strict"; return (' + query + ')(arguments[0], arguments[1], arguments[2]);')(rebasedDataMap, dataExpirer, subscriptions);
};

var Broker = function (options) {
  EventEmitter.call(this);

  this.id = BROKER_ID;
  this.type = 'broker';
  this.options = options;
  this.instanceId = this.options.instanceId;
  this.secretKey = this.options.secretKey;
  this.debugPort = DEBUG_PORT;

  this.dataMap = dataMap;
  this.dataExpirer = dataExpirer;
  this.subscriptions = subscriptions;
};

Broker.prototype = Object.create(EventEmitter.prototype);

Broker.prototype.sendToMaster = function (data) {
  process.send({
    event: 'brokerMessage',
    brokerId: this.id,
    data: data
  });
};

Broker.prototype.run = function (query, baseKey) {
  return run(query, baseKey);
};

Broker.prototype.publish = function (channel, message) {
  var sock;
  for (var i in subscriptions) {
    if (subscriptions.hasOwnProperty(i)) {
      sock = subscriptions[i][channel];
      if (sock && sock instanceof com.ComSocket) {
        send(sock, {type: 'message', channel: channel, value: message}, pubSubOptions);
      }
    }
  }
};

var scBroker;

var initBrokerServer = function (options) {
  scBroker = new Broker(options);
  global.broker = scBroker;

  // Create the controller instances now.
  // This is more symmetric to SocketCluster's worker cluster.

  if (INIT_CONTROLLER_PATH != null) {
    INIT_CONTROLLER = require(INIT_CONTROLLER_PATH);
    INIT_CONTROLLER.run(scBroker);
  }

  if (BROKER_CONTROLLER_PATH != null) {
    BROKER_CONTROLLER = require(BROKER_CONTROLLER_PATH);
    BROKER_CONTROLLER.run(scBroker);
  }
};

var pubSubOptions = {
  batch: true
};

var actions = {
  init: function (command, socket) {
    var result = {id: command.id, type: 'response', action: 'init'};
    if (command.secretKey == scBroker.secretKey) {
      initialized[socket.id] = {};
    } else {
      result.error = 'Invalid password was supplied to the broker';
    }
    send(socket, result);
  },

  set: function (command, socket) {
    var result = scBroker.dataMap.set(command.key, command.value);
    var response = {id: command.id, type: 'response', action: 'set'};
    if (command.getValue) {
      response.value = result;
    }
    send(socket, response);
  },

  expire: function (command, socket) {
    scBroker.dataExpirer.expire(command.keys, command.value);
    var response = {id: command.id, type: 'response', action: 'expire'};
    send(socket, response);
  },

  unexpire: function (command, socket) {
    scBroker.dataExpirer.unexpire(command.keys);
    var response = {id: command.id, type: 'response', action: 'unexpire'};
    send(socket, response);
  },

  getExpiry: function (command, socket) {
    var response = {id: command.id, type: 'response', action: 'getExpiry', value: scBroker.dataExpirer.getExpiry(command.key)};
    send(socket, response);
  },

  get: function (command, socket) {
    var result = scBroker.dataMap.get(command.key);
    send(socket, {id: command.id, type: 'response', action: 'get', value: result});
  },

  getRange: function (command, socket) {
    var result = scBroker.dataMap.getRange(command.key, command.fromIndex, command.toIndex);
    send(socket, {id: command.id, type: 'response', action: 'getRange', value: result});
  },

  getAll: function (command, socket) {
    send(socket, {id: command.id, type: 'response', action: 'getAll', value: scBroker.dataMap.getAll()});
  },

  count: function (command, socket) {
    var result = scBroker.dataMap.count(command.key);
    send(socket, {id: command.id, type: 'response', action: 'count', value: result});
  },

  add: function (command, socket) {
    var result = scBroker.dataMap.add(command.key, command.value);
    var response = {id: command.id, type: 'response', action: 'add', value: result};
    send(socket, response);
  },

  concat: function (command, socket) {
    var result = scBroker.dataMap.concat(command.key, command.value);
    var response = {id: command.id, type: 'response', action: 'concat'};
    if (command.getValue) {
      response.value = result;
    }
    send(socket, response);
  },

  registerDeathQuery: function (command, socket) {
    var response = {id: command.id, type: 'response', action: 'registerDeathQuery'};

    if (initialized[socket.id]) {
      initialized[socket.id].deathQuery = command.value;
    }
    send(socket, response);
  },

  run: function (command, socket) {
    var ret = {id: command.id, type: 'response', action: 'run'};
    try {
      var result = scBroker.run(command.value, command.baseKey);
      if (result !== undefined) {
        ret.value = result;
      }
    } catch(e) {
      if (e.stack) {
        e = e.stack;
      }
      ret.error = 'Exception at run(): ' + e;
    }
    if (!command.noAck) {
      send(socket, ret);
    }
  },

  remove: function (command, socket) {
    var result = scBroker.dataMap.remove(command.key);
    if (!command.noAck) {
      var response = {id: command.id, type: 'response', action: 'remove'};
      if (command.getValue) {
        response.value = result;
      }
      send(socket, response);
    }
  },

  removeRange: function (command, socket) {
    var result = scBroker.dataMap.removeRange(command.key, command.fromIndex, command.toIndex);
    if (!command.noAck) {
      var response = {id: command.id, type: 'response', action: 'removeRange'};
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
    var args = [command.key, command.index, command.count];
    if (command.items) {
      args = args.concat(command.items);
    }
    // Remove any consecutive undefined references from end of array
    for (var i = args.length - 1; i >= 0; i--) {
      if (args[i] !== undefined) {
        break;
      }
      args.pop();
    }
    var result = scBroker.dataMap.splice.apply(scBroker.dataMap, args);
    if (!command.noAck) {
      var response = {id: command.id, type: 'response', action: 'splice'};
      if (command.getValue) {
        response.value = result;
      }
      send(socket, response);
    }
  },

  pop: function (command, socket) {
    var result = scBroker.dataMap.pop(command.key);
    if (!command.noAck) {
      var response = {id: command.id, type: 'response', action: 'pop'};
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
    var hasListener = anyHasListener(command.channel);
    addListener(socket, command.channel);
    if (!hasListener) {
      scBroker.emit('subscribe', command.channel);
    }
    send(socket, {id: command.id, type: 'response', action: 'subscribe', channel: command.channel}, pubSubOptions);
  },

  unsubscribe: function (command, socket) {
    if (command.channel) {
      removeListener(socket, command.channel);
      var hasListener = anyHasListener(command.channel);
      if (!hasListener) {
        scBroker.emit('unsubscribe', command.channel);
      }
    } else {
      var channels = removeAllListeners(socket);
      for (var i in channels) {
        if (channels.hasOwnProperty(i)) {
          if (!anyHasListener(channels[i])) {
            scBroker.emit('unsubscribe', channels[i]);
          }
        }
      }
    }
    send(socket, {id: command.id, type: 'response', action: 'unsubscribe', channel: command.channel}, pubSubOptions);
  },

  isSubscribed: function (command, socket) {
    var result = hasListener(socket, command.channel);
    send(socket, {id: command.id, type: 'response', action: 'isSubscribed', channel: command.channel, value: result}, pubSubOptions);
  },

  publish: function (command, socket) {
    scBroker.publish(command.channel, command.value);
    var response = {id: command.id, type: 'response', action: 'publish', channel: command.channel};
    if (command.getValue) {
      response.value = command.value;
    }
    scBroker.emit('publish', command.channel, command.value);
    send(socket, response, pubSubOptions);
  },

  send: function (command, socket) {
    scBroker.emit('message', command.value, function (err, data) {
      var response = {
        id: command.id,
        type: 'response',
        action: 'send',
        value: data
      };
      if (err) {
        response.error = err;
      }
      send(socket, response);
    });
  }
};

var MAX_ID = Math.pow(2, 53) - 2;
var curID = 1;

var genID = function () {
  curID++;
  curID = curID % MAX_ID;
  return curID;
};

var comServer = com.createServer();
var connections = {};

var handleConnection = errorDomain.bind(function (sock) {
  errorDomain.add(sock);
  sock.id = genID();

  connections[sock.id] = sock;

  sock.on('message', function (command) {
    if (initialized.hasOwnProperty(sock.id) || command.action == 'init') {
      try {
        if (actions[command.action]) {
          actions[command.action](command, sock);
        }
      } catch(e) {
        if (e.stack) {
          console.log(e.stack);
        } else {
          console.log(e);
        }
        if (e instanceof Error) {
          e = e.toString();
        }
        send(sock, {id: command.id, type: 'response', action:  command.action, error: 'Failed to process command due to the following error: ' + e});
      }
    } else {
      var e = 'Cannot process command before init handshake';
      console.log(e);
      send(sock, {id: command.id, type: 'response', action: command.action, error: e});
    }
  });

  sock.on('close', function () {
    delete connections[sock.id];

    if (initialized[sock.id]) {
      if (initialized[sock.id].deathQuery) {
        run(initialized[sock.id].deathQuery);
      }
      delete initialized[sock.id];
    }
    var channels = removeAllListeners(sock);
    for (var i in channels) {
      if (channels.hasOwnProperty(i)) {
        if (!anyHasListener(channels[i])) {
          scBroker.emit('unsubscribe', channels[i]);
        }
      }
    }
    errorDomain.remove(sock);
  });
});

comServer.on('connection', handleConnection);

comServer.on('listening', function () {
  process.send({event: 'listening'});
});

var comServerListen = function () {
  if (SOCKET_PATH) {
    if (process.platform != 'win32' && fs.existsSync(SOCKET_PATH)) {
      fs.unlinkSync(SOCKET_PATH)
    }
    comServer.listen(SOCKET_PATH);
  } else {
    comServer.listen(PORT);
  }
};

process.on('message', function (m) {
  if (m) {
    if (m.type == 'masterMessage') {
      scBroker.emit('masterMessage', m.data);
    } else if (m.type == 'initBrokerServer') {
      if (scBroker) {
        throw new Error('Attempted to initialize broker which has already been initialized.');
      } else {
        initBrokerServer(m.data);
        comServerListen();
      }
    }
  }
});

var killServer = function () {
  comServer.close(function () {
    process.exit();
  });

  for (var i in connections) {
    if (connections.hasOwnProperty(i)) {
      connections[i].destroy();
    }
  }

  setTimeout(function () {
    process.exit();
  }, PROCESS_TERM_TIMEOUT);
};

process.on('SIGTERM', killServer);
process.on('disconnect', killServer);

setInterval(function () {
  var keys = dataExpirer.extractExpiredKeys();
  var len = keys.length;
  for (var i = 0; i < len; i++) {
    dataMap.remove(keys[i]);
  }
}, EXPIRY_ACCURACY);

process.on('uncaughtException', function (err) {
  errorHandler(err);
  process.exit(1);
});
