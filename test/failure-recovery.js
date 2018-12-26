const scBroker = require('../index');
const assert = require('assert');

let conf = {
  port: 9002,
  timeout: 2000,
  ipcAckTimeout: 1000,
  pubSubAckTimeout: 2000,
  brokerOptions: {
    ipcAckTimeout: 1000
  }
};

if (process.env.TEST_TYPE === 'es6') {
  conf.brokerControllerPath = __dirname + '/stubs/broker-controller-stub.mjs';
} else {
  conf.brokerControllerPath = __dirname + '/stubs/broker-controller-stub.js';
}

let server;
let client;
let testFinished = false;

describe('sc-broker failure handling and recovery', function () {

  before('run the server before start', async function () {
    // Set up the server to auto-relaunch on crash
    let launchServer = () => {
      if (testFinished) {
        return;
      }
      server = scBroker.createServer(conf);
      (async () => {
        for await (let {error} of server.listener('error')) {
          console.error('server error:', error);
        }
      })();

      (async () => {
        await server.listener('exit').once();
        launchServer();
      })();
    };
    launchServer();

    client = scBroker.createClient(conf);
    (async () => {
      for await (let {error} of client.listener('error')) {
        console.error('client error', error);
      }
    })();
    await server.listener('ready').once();
  });

  after('shut down server afterwards', async function () {
    testFinished = true;
    client.closeListener('error');
    server.closeListener('error');
    server.destroy();
  });

  it('should be able to handle failure and gracefully recover from it', function (done) {
    let pubIntervalHandle = null;
    let pubInterval = 1;
    let pubTargetNum = 2000;

    let pubCount = 0;
    let receivedCount = 0;

    let finish = () => {
      assert.equal(receivedCount >= pubCount, true);
      done();
    };

    let handleMessage = (channel, data) => {
      if (channel === 'foo') {
        receivedCount++;
        if (data === 'hello ' + (pubTargetNum - 1)) {
          console.log('receivedCount vs pubTargetNum:', receivedCount, pubTargetNum);
          finish();
        }
      }
    };

    (async () => {
      for await (let {channel, data} of client.listener('message')) {
        handleMessage(channel, data);
      }
    })();

    client.subscribe('foo')
    .then(() => {
      let doPublish = () => {
        if (pubCount < pubTargetNum) {
          let singlePublish = (pCount) => {
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
            server.sendMessageToBroker({killBroker: true});
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
