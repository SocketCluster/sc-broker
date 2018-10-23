var _ = require('underscore');
var scBroker = require('../index');
var assert = require('assert');

var conf = {
  port: 9002,
  timeout: 2000,
  ipcAckTimeout: 1000,
  brokerOptions: {
    ipcAckTimeout: 1000
  }
};

if (process.env.TEST_TYPE === 'es6') {
  conf.brokerControllerPath = __dirname + '/stubs/broker-controller-stub.mjs';
} else {
  conf.brokerControllerPath = __dirname + '/stubs/broker-controller-stub.js';
}

var wait = function (duration) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve();
    }, duration);
  });
};

var server;
var client;

describe('sc-broker client', function () {

  before('run the server before start', function (done) {
    server = scBroker.createServer(conf);
    server.on('error', (err) => {
      console.log('SERVER ERROR:', err);
    });
    client = scBroker.createClient(conf);
    client.on('error', (err) => {
      console.log('CLIENT ERROR:', err);
    });
    server.on('ready', () => {
      done();
    });
  });

  after('shut down server afterwards', function (done) {
    server.destroy();
    done();
  });

  describe('sc-broker#executeCommandWhenClientIsDisconnected', function () {
    it('should be able to execute getAll action if client starts out disconnected', function () {
      return client.end()
      .then(() => {
        return client.getAll();
      });
    });

    it('should emit the data by value (not by reference) when recovering from lost connection', function () {
      return client.end()
      .then(() => {
        var obj = {
          foo: 'bar'
        };

        var objString = JSON.stringify(obj);
        return client.set('someUniqueKey', obj, true)
        .then(() => {
          return client.get('someUniqueKey');
        })
        .then((value) => {
          var valueString = JSON.stringify(value);
          assert.equal(valueString, objString);
        });

        obj.propertyAddedLater = 123;
      });
    });
  });

  describe('sc-broker#createServer', function () {
    it('should provide server.on', function (done) {
      assert(_.isFunction(server.on), true);
      done();
    });

    it('should provide server.destroy', function (done) {
      assert(_.isFunction(server.destroy), true);
      done();
    });
  });

  describe('sc-broker#createClient', function () {
    it('should provide scBroker.createClient', function (done) {
      assert.equal(_.isFunction(scBroker.createClient), true);
      done();
    });
  });

  describe('sc-broker#sendRequestToBroker', function () {
    it('should be able to send a request to the broker and get a response', function () {
      return server.sendRequestToBroker({subject: 'world'})
      .then((data) => {
        var expected = JSON.stringify({hello: 'world'});
        var actual = JSON.stringify(data);
        assert.equal(actual, expected);
      });
    });

    it('should be able to send a request to the broker and get back an error if something went wrong', function () {
      var error = null;
      return server.sendRequestToBroker({sendBackError: true})
      .catch((err) => {
        error = err;
      })
      .then(() => {
        assert.notEqual(error, null);
        assert.equal(error.name, 'CustomBrokerError');
        assert.equal(error.message, 'This is an error');
      });
    });

    it('should be able to send a request to the broker and timeout if broker does not respond', function () {
      var error = null;
      return server.sendRequestToBroker({doNothing: true})
      .catch((err) => {
        error = err;
      })
      .then(() => {
        assert.notEqual(error, null);
        assert.equal(error.name, 'TimeoutError');
      });
    });
  });

  describe('sc-broker#sendMessageToBroker', function () {
    it('should be able to send data to the broker and not timeout if a broker does not respond', function () {
      return server.sendMessageToBroker({doNothing: true});
    });
  });

  describe('broker-controller#sendRequestToMaster', function () {
    var currentTestCallbacks = {};

    before('prepare message responder on master', function (done) {
      server.on('brokerRequest', (brokerId, data, respond) => {
        if (data.sendBackError) {
          var err = new Error('This is an error');
          err.name = 'CustomMasterError';
          respond(err);
        } else if (!data.doNothing) {
          var responseData = {
            hello: data.brokerSubject
          };
          respond(null, responseData);
        }
      });
      server.on('brokerMessage', (brokerId, data) => {
        if (data.brokerTestResult) {
          currentTestCallbacks[data.brokerTestResult](data.err, data.data);
        }
      });
      done();
    });

    it('should be able to send a message to the master and get a response', function (done) {
      currentTestCallbacks['test1'] = (err, data) => {
        var expected = JSON.stringify({hello: 'there'});
        var actual = JSON.stringify(data);
        assert.equal(actual, expected);
        done();
      };
      server.sendMessageToBroker({brokerTest: 'test1'});
    });

    it('should be able to send a message to the master and get back an error if something went wrong', function (done) {
      currentTestCallbacks['test2'] = (err, data) => {
        assert.notEqual(err, null);
        assert.equal(err.name, 'CustomMasterError');
        assert.equal(err.message, 'This is an error');
        done();
      };
      server.sendMessageToBroker({brokerTest: 'test2'});
    });

    it('should be able to send a message to the master and timeout if callback is provided and master does not respond', function (done) {
      currentTestCallbacks['test3'] = (err, data) => {
        assert.notEqual(err, null);
        assert.equal(err.name, 'TimeoutError');
        done();
      };
      server.sendMessageToBroker({brokerTest: 'test3'});
    });

    it('should be able to send a message to the master and not timeout if no callback is provided and master does not respond', function (done) {
      currentTestCallbacks['test4'] = (err, data) => {
        done();
      };
      server.sendMessageToBroker({brokerTest: 'test4'});
    });
  });

  describe('client#getAll', function () {
    it('should get all', function () {
      return client.getAll();
    });
  });

  var val1 = 'This is a value';
  var path1 = ['a', 'b', 'c'];
  var path2 = ['d', 'e', 'f'];
  var val2 = 'append this';

  describe('client#get', function () {
    it('should provide client.get', function () {
      assert.equal(_.isFunction(client.get), true);
    });

    it('should set values', function () {
      return client.set(path2, val1, true)
      .then((value) => {
        return client.get(path2);
      })
      .then((value) => {
        assert.equal(value, val1);
      });
    });
  });

  describe('client#add', function () {
    it(
      'should add a value to an existing, '
      + 'existing should be kept',
      function () {
        return client.set(path2, val1, true)
        .then((value) => {
          return client.add(path2, val2);
        })
        .then((insertionIndex) => {
          assert.equal(insertionIndex , 1);
          return client.get(path2);
        })
        .then((value) => {
          assert.equal(value[0] , val1);
          assert.equal(value[1] , val2);
        });
      }
    );
  });

  var val3 = [1, 2, 3, 4];
  var path3 = ['g', 'h', 'i'];
  var path4 = ['j', 'k', 'l'];
  var val4 = {one: 1, two: 2, three: 3};
  var path5 = ['m', 'n', 'o'];

  describe('client#concat', function () {
    it('should concat string values', function () {
      return client.set(path3, val1)
      .then(() => {
        return client.concat(path3, val2);
      })
      .then(() => {
        return client.get(path3);
      })
      .then((value) => {
        assert.equal(value[0] , val1);
        assert.equal(value[1] , val2);
      });
    });

    it('should concat arrays', function () {
      return client.set(path4, val1)
      .then(() => {
        return client.concat(path4, val3);
      })
      .then(() => {
        return client.get(path4);
      })
      .then((value) => {
        assert.equal(value[0], val1);
        assert.equal(value[1], val3[0]);
        assert.equal(value[2], val3[1]);
        assert.equal(value[3], val3[2]);
        assert.equal(value[4], val3[3]);
      });
    });

    it('should concat objects', function () {
      return client.set(path5, val1)
      .then(() => {
        return client.concat(path5, val4);
      })
      .then(() => {
        return client.get(path5);
      })
      .then((value) => {
        assert.equal(value[0], val1);
        assert.equal(value[1].one, val4.one);
        assert.equal(value[1].two, val4.two);
        assert.equal(value[1].three, val4.three);
      });
    });
  });


  var val5 = {one: 1, two: 2, three: 3, four: 4, five: 5};
  var path6 = ['p', 'q'];
  var val6 = [0, 1, 2, 3, 4, 5, 6, 7, 8];
  var expected1 = [0, 1, 2, 6, 7, 8];
  var fromIndex = 3;
  var toIndex = 6;

  describe('client#removeRange', function () {
    it('should remove object entries by range', function () {
      return client.set(path5, val5)
      .then(() => {
        return client.removeRange(path5, {fromIndex: 'two', toIndex: 'three'});
      })
      .then((value) => {
        return client.get(path5);
      })
      .then((value) => {
        var expected = {
          one: 1,
          three: 3,
          four: 4,
          five: 5
        };
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should remove array entries by range', function () {
      return client.set(path6, val6)
      .then(() => {
        return client.removeRange(path6, {
          fromIndex: fromIndex,
          toIndex: toIndex
        });
      })
      .then((value) => {
        return client.get(path6);
      })
      .then((value) => {
        assert(JSON.stringify(value) === JSON.stringify(expected1));
      });
    });

    it('should not remove array entries by range if no fromIndex is provided', function () {
      return client.set(path6, val6)
      .then(() => {
        return client.removeRange(path6, {});
      })
      .then((value) => {
        return client.get(path6);
      })
      .then((value) => {
        assert(JSON.stringify(value) === JSON.stringify(val6));
      });
    });
  });

  describe('client#exec', function () {
    it('should execute query functions', function () {
      return client.set(['one', 'two', 'three', 'four'], val1)
      .then(() => {
        var query = function (DataMap) { return DataMap.get(['one', 'two', 'three']); };
        return client.exec(query);
      })
      .then((value) => {
        var expected = {
          four: val1
        };
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should set values over query.data', function () {
      var obj = {
        x: 1,
        y: 2
      };
      var query = function (DataMap) {
        DataMap.set('point', point);
        return DataMap.get(['point']);
      };
      query.data = {
        point: obj
      };
      return client.exec(query)
      .then((value) => {
        var expected = {
          x: 1,
          y: 2
        };
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });
  });

  var arr = [0, 1, 2, 3, 4, 5, 6, 7];
  var obj = {red: 1, green: 2, blue: 3, yellow: 4, orange: 5};
  var path7 = ['this', 'is', 'an', 'array'];
  var path8 = ['this', 'is', 'an', 'object'];

  describe('client#getRange', function () {
    it('should get range test1', function () {
      return client.set(path7, arr)
      .then(() => {
        return client.getRange(path7, {fromIndex: 2, toIndex: 5});
      })
      .then((value) => {
        var expected = [2, 3, 4];
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should get range test2', function () {
      return client.set(path7, arr)
      .then(() => {
        return client.getRange(path7, {fromIndex: 4});
      })
      .then((value) => {
        var expected = [4, 5, 6, 7];
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should get range test3', function () {
      return client.set(path7, arr)
      .then(() => {
        return client.getRange(path7, {fromIndex: 0, toIndex: 5});
      })
      .then((value) => {
        var expected = [0, 1, 2, 3, 4];
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should get range test4', function () {
      return client.set(path7, arr)
      .then(() => {
        return client.getRange(path7, {fromIndex: 4, toIndex: 15});
      })
      .then((value) => {
        var expected = [4, 5, 6, 7];
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should get range test5', function () {
      return client.set(path8, obj)
      .then(() => {
        return client.getRange(path8, {fromIndex: 'green', toIndex: 'blue'});
      })
      .then((value) => {
        var expected = {
          green: 2
        };
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should get range test6', function () {
      return client.getRange(path8, {fromIndex: 'blue'})
      .then((value) => {
        var expected = {
          blue: 3,
          yellow: 4,
          orange: 5
        };
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should get range test7', function () {
      return client.getRange(path8, {fromIndex: 'blue'})
      .then(() => {
        return client.getRange(path8, {fromIndex: 'green', toIndex: 'yellow'});
      })
      .then((value) => {
        var expected = {
          green: 2,
          blue: 3
        };
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });
  });

  var itemsB = ['a', 'b', 'c', 'd', 'e'];
  var itemsC = ['a', 'b', 'c', 'd', 'e'];
  var itemsD = ['c', 'd', 'e'];
  var itemsE = ['a', 'b'];

  describe('client#splice', function () {
    it('should splice values test1', function () {
      var itemsA = ['a', 'b', 'c', 'd', 'e'];
      return client.set(['levelA1', 'levelA2'], itemsA)
      .then(() => {
        var spliceOptions = {
          fromIndex: 2,
          count: 2,
          items: ['c2', 'd2']
        };
        return client.splice(['levelA1', 'levelA2'], spliceOptions);
      })
      .then(() => {
        return client.get(['levelA1', 'levelA2']);
      })
      .then((value) => {
        var expected = ['a', 'b', 'c2', 'd2', 'e'];
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should splice values test2', function () {
      return client.set(['levelB1', 'levelB2'], itemsB)
      .then(() => {
        var spliceOptions = {
          fromIndex: 2
        };
        return client.splice(['levelB1', 'levelB2'], spliceOptions);
      })
      .then(() => {
        return client.get(['levelB1', 'levelB2']);
      })
      .then((value) => {
        var expected = ['a', 'b'];
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should splice values test3', function () {
      return client.set(['levelC1', 'levelC2'], itemsC)
      .then(() => {
        var spliceOptions = {
          count: 3
        };
        return client.splice(['levelC1', 'levelC2'], spliceOptions);
      })
      .then(() => {
        return client.get(['levelC1', 'levelC2']);
      })
      .then((value) => {
        var expected = ['d', 'e'];
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should splice values test4', function () {
      return client.set(['levelD1', 'levelD2'], itemsD)
      .then(() => {
        var spliceOptions = {
          items: ['a', 'b']
        };
        return client.splice(['levelD1', 'levelD2'], spliceOptions);
      })
      .then(() => {
        return client.get(['levelD1', 'levelD2']);
      })
      .then((value) => {
        var expected = ['a', 'b', 'c', 'd', 'e'];
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should splice values test5', function () {
      client.set(['levelE1', 'levelE2'], itemsE)
      .then(() => {
        var spliceOptions = {
          fromIndex: 2,
          count: 0,
          items: [{key1: 1, key2: {nestedKey1: 'hi'}}, 'c']
        };
        return client.splice(['levelE1', 'levelE2'], spliceOptions);
      })
      .then(() => {
        return Promise.all([
          client.get(['levelE1', 'levelE2'])
          .then((value) => {
            var expected = ['a', 'b', {key1: 1, key2: {nestedKey1: 'hi'}}, 'c'];
            assert(JSON.stringify(value) === JSON.stringify(expected));
          }),
          client.get(['levelE1', 'levelE2', 2, 'key2'])
          .then((value) => {
            var expected = {nestedKey1: 'hi'};
            assert(JSON.stringify(value) === JSON.stringify(expected));
          })
        ]);
      });
    });
  });

  var ch1 = 'foo';
  var ch2 = 'bar';
  var ch3 = 'allowOnce';
  var badChannel = 'badChannel';
  var silentChannel = 'silentChannel';
  var delayedChannel = 'delayedChannel';

  describe('client#subscriptions', function () {
    it('should have no subscriptions (empty array)', function () {
      return client.subscriptions()
      .then((result) => {
        assert(JSON.stringify(result) === JSON.stringify([]));
      });
    });

    it('should not reject', function () {
      return client.subscribe(ch1);
    });

    it('should subscribe channel ' + ch1, function () {
      return client.subscribe(ch1)
      .then(() => {
        return client.isSubscribed(ch1);
      })
      .then((result) => {
        assert.equal(result, true);
        return client.subscriptions();
      })
      .then((result) => {
        assert(JSON.stringify(result) === JSON.stringify([ch1]));
      });
    });

    it('should stay in the subscribed state if the second subscribe request fails for channel ' + ch3, function () {
      var error = null;
      return client.subscribe(ch3)
      .then(() => {
        return client.isSubscribed(ch3);
      })
      .then((result) => {
        assert.equal(result, true);
        return client.subscribe(ch3);
      })
      .catch((err) => {
        error = err;
        assert.equal(err.name, 'OnlyOnceError');
      })
      .then(() => {
        assert.notEqual(error, null);
        return client.isSubscribed(ch3);
      })
      .then((result) => {
        assert.equal(result, true);
        return client.unsubscribe(ch3);
      })
    });

    it('can be blocked by middleware', function () {
      return client.subscribe(badChannel)
      .catch((err) => {
        assert(/bad channel/.test(err.message));
      })
      .then(() => {
        return client.isSubscribed(badChannel);
      })
      .then((result) => {
        assert.strictEqual(result, false);
        return client.subscriptions();
      })
      .then((result) => {
        assert(!result.some((channel) => { return channel === badChannel; }));
      });
    });

    it('can be delayed by middleware', function () {
      var start = Date.now();
      return client.subscribe(delayedChannel)
      .then(() => {
        var duration = Date.now() - start;
        assert.equal(duration >= 500, true);
        return client.unsubscribe(delayedChannel);
      });
    });

    it('should recover subscriptions after regaining lost connection to server', function () {
      var start = Date.now();
      var originalSubs = [];
      var receivedMessages = [];
      return client.subscribe(ch1)
      .then(() => {
        return client.subscribe(ch2);
      })
      .then(() => {
        return client.subscribe(badChannel)
        .catch(() => {});
      })
      .then(() => {
        return client.subscribe(delayedChannel);
      })
      .then(() => {
        return client.subscriptions();
      })
      .then((result) => {
        originalSubs = result;
        client._socket.end();
      })
      .then(() => {
        return Promise.all([
          Promise.resolve()
          .then(() => {
            client.on('message', (channel, value) => {
              receivedMessages.push({
                channel: channel,
                value: value
              });
            });
          })
          .then(() => {
            // Add a delay before sending publish to allow the client
            // to disconnect.
            return wait(100);
          })
          .then(() => {
            assert.equal(client.state, client.DISCONNECTED);
            return client.publish(delayedChannel, 'delayedMessage');
          }),
          new Promise((resolve, reject) => {
            client.on('ready', () => {
              resolve();
            });
          })
        ]);
      })
      .then(() => {
        return client.subscriptions();
      })
      .then((result) => {
        assert.equal(JSON.stringify(result), JSON.stringify(originalSubs));
      })
      .then(() => {
        return wait(1000);
      })
      .then(() => {
        assert.equal(receivedMessages.some((message) => { return message && message.value === 'delayedMessage'; }), true);
      });
    });
  });

  describe('client#unsubscriptions', function () {
    it('should not reject', function () {
      return client.unsubscribe(ch2);
    });
  });

  describe('client#publish', function () {
    it('should not reject', function () {
      return client.publish(ch2, ['a','b']);
    });

    it('can be blocked by middleware', function () {
      return client.publish(silentChannel, ['a','b'])
      .then(() => {
        throw new Error('Previous action should have been rejected');
      })
      .catch((err) => {
        assert(/silent channel/.test(err.message));
      });
    });

    it('can be delayed by middleware', function () {
      var start = Date.now();
      return client.publish(delayedChannel, ['a','b'])
      .then(() => {
        var duration = Date.now() - start;
        assert.equal(duration >= 500, true);
      });
    });

    it('can be transformed by middleware', function () {
      return client.publish(ch2, 'test message')
      .then((value) => {
        assert.strictEqual(value, 'transformed test message');
      });
    });
  });

  describe('client#sendRequest', function () {
    it('can send a request to broker and receive a response', function () {
      return client.sendRequest(10)
      .then((result) => {
        assert.equal(result, 11);
      });
    });
  });

  describe('client#sendMessage', function () {
    it('can send data to broker', function () {
      var startTime;
      return client.sendMessage('hello')
      .then(() => {
        startTime = Date.now();
        return client.sendMessage('world');
      })
      .then(() => {
        // sendMessage should resolve on the next tick.
        assert.equal(Date.now() - startTime < 10, true);
        return wait(100);
      })
      .then(() => {
        return client.sendRequest({getDataBuffer: true})
      })
      .then((dataBuffer) => {
        assert.equal(JSON.stringify(dataBuffer), JSON.stringify(['hello', 'world']));
      });
    });
  });

  var etsec = 1;
  describe('client#expire', function () {
    it('value should be expired 1000ms after the given time.', function (done) {
      client.set(['check', 'expire', 'key'], 'some data')
      .then(() => {
        client.expire([['check', 'expire', 'key']], etsec);
        setTimeout(() => {
          client.get(['check'])
          .then((value) => {
            var expected = {
              expire: {}
            };
            assert(JSON.stringify(value) === JSON.stringify(expected));
            done();
          });
        }, etsec * 1000 * 2.1);
      });
    });
  });


  var val9 = 'This is a value';
  var path9 = ['a', 'b', 'c'];
  var path10 = ['d', 'e', 'f'];
  var path11 = ['that', '8a788b9c-c50e-0b3f-bd47-ec0c63327bf1'];
  var path12 = ['g', 'h', 'i'];
  var somePath = ['jlkfjsl'];
  var someObject123 = {hello: 'world'};

  describe('client#set', function () {
    it('should provide client.set', function (done) {
      assert.equal(_.isFunction(client.set), true);
      done();
    });

    it('should set and return values', function () {
      return client.set(path9, val9, {getValue: true})
      .then((value) => {
        assert.equal(value, val9);
      });
    });

    it('should set and return object values', function () {
      return client.set(somePath, someObject123, {getValue: true})
      .then((value) => {
        assert.equal(JSON.stringify(value), JSON.stringify(someObject123));
      });
    });

    it('should return null if no value is demanded', function () {
      return client.set(path10, val9)
      .then((value) => {
        assert.equal(value, undefined);
        assert.equal(value, null);
      });
    });

    it('should set properly in callbacks (double set to the same path)', function () {
      return client.set(path11, [1, 2, 3, 4, 5])
      .then(() => {
        return client.set(path11, [6, 7, 8]);
      })
      .then(() => {
        return client.get('that');
      })
      .then((value) => {
        var expected = {
          '8a788b9c-c50e-0b3f-bd47-ec0c63327bf1': [6, 7, 8]
        };
        assert(JSON.stringify(value) === JSON.stringify(expected));
      });
    });

    it('should set value inside the callback of a .get()', function () {
      return client.get(path12)
      .then((value) => {
        return client.set(path12, val9);
      });
    });
  });

  describe('client#remove', function () {
    it('should remove the value at keyChain', function () {
      return client.set(['a', 'b', 'c'], [1, 2, 3])
      .then(() => {
        return client.get(['a', 'b', 'c']);
      })
      .then((value) => {
        assert.equal(value[2], 3);
        return client.remove(['a', 'b', 'c'], {getValue: true});
      })
      .then((value) => {
        assert.equal(_.isArray(value), true);
        assert.equal(value.length, 3);
        return client.get(['a', 'b', 'c']);
      })
      .then((value) => {
        assert.equal(_.isUndefined(value), true);
      });
    });
  });

  describe('client#pop', function () {
    it('should remove the last numerically-indexed entry at keyChain', function () {
      return client.set(['a', 'b', 'c'], [1, 2, 3])
      .then(() => {
        return client.get(['a', 'b', 'c']);
      })
      .then((value) => {
        assert.equal(value[2], 3);
        return client.pop(['a', 'b', 'c'], {getValue: true});
      })
      .then((value) => {
        assert.equal(_.isArray(value), true);
        assert.equal(value.length, 1);
        assert.equal(value[0], 3);
        return client.get(['a', 'b', 'c']);
      })
      .then((value) => {
        assert.equal(_.isArray(value), true);
        assert.equal(value.length, 2);
        assert.equal(value[0], 1);
        assert.equal(value[1], 2);
      });
    });
  });

  describe('client#registerDeathQuery', function () {
   it('should not reject', function () {
     return client.registerDeathQuery(function () {}, {something: 123});
   });
  });

  describe('client#end', function () {
    it('should not reject', function () {
      var client = scBroker.createClient(conf);
      return client.end();
    });
  });
});
