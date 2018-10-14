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

var server;
var client;
var testFinished = false;

describe('sc-broker failure handling and recovery', function () {

  before('run the server before start', function (done) {
    // Set up the server to auto-relaunch on crash
    var launchServer = () => {
      if (testFinished) {
        return;
      }
      server = scBroker.createServer(conf);
      server.on('error', (err) => {
        console.error('server error:', err);
      });
      server.on('exit', launchServer);
    };
    launchServer();

    client = scBroker.createClient(conf);
    client.on('error', (err) => {
      console.error('client error', err);
    });
    server.on('ready', () => {
      done();
    });
  });

  after('shut down server afterwards', function (done) {
    testFinished = true;
    server.destroy();
    done();
  });

  it('should be able to handle failure and gracefully recover from it', function (done) {
    var pubIntervalHandle = null;
    var pubInterval = 1;
    var pubTargetNum = 2000;

    var pubCount = 0;
    var receivedCount = 0;

    var finish = () => {
      assert.equal(receivedCount, pubCount);
      done();
    };

    var handleMessage = (channel, data) => {
      if (channel === 'foo') {
        receivedCount++;

        if (receivedCount >= pubTargetNum) {
          console.log('receivedCount vs pubTargetNum:', receivedCount, pubTargetNum);
          finish();
        }
      }
    };

    client.on('message', handleMessage);
    client.subscribe('foo')
    .then(() => {
      var doPublish = () => {
        if (pubCount < pubTargetNum) {
          var singlePublish = (pCount) => {
            client.publish('foo', 'hello ' + pCount)
            .catch((err) => {
              // If error, retry.
              setTimeout(singlePublish.bind(this, pCount), 100);
            });
          };
          singlePublish(pubCount);
          pubCount++;
          // Kill the server at 30% of the way.
          if (pubCount === Math.round(pubTargetNum * 0.3)) {
            server.sendDataToBroker({killBroker: true});
          }
        } else {
          clearInterval(pubIntervalHandle);
        }
      };
      pubIntervalHandle = setInterval(doPublish, pubInterval);
    })
    .catch((err) => {
      throw err;
    });
  });
});
