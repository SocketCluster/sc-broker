// To test with .mjs (ES6 modules)

import SCBroker from '../../scbroker';
import scErrors from 'sc-errors';
import addMiddleware from './middleware';

class BrokerControllerStub extends SCBroker {
  run() {
    console.log('Start broker');
    addMiddleware(this);

    let dataBuffer = [];

    (async () => {
      for await (let req of this.listener('request')) {
        let data = req.data;
        if (data && data.getDataBuffer) {
          req.end(dataBuffer);
          dataBuffer = [];
        } else {
          req.end(data + 1);
        }
      }
    })();

    (async () => {
      for await (let event of this.listener('message')) {
        dataBuffer.push(event);
      }
    })();

    (async () => {
      for await (let {data} of this.listener('masterMessage')) {
        if (data.killBroker) {
          console.log('Broker is shutting down');
          process.exit();
        } else {
          if (data.brokerTest) {
            if (data.brokerTest === 'test1') {
              this.sendRequestToMaster({
                brokerSubject: 'there'
              })
              .then((data) => {
                this.sendMessageToMaster({
                  brokerTestResult: 'test1',
                  data
                });
              })
              .catch((err) => {
                this.sendMessageToMaster({
                  brokerTestResult: 'test1',
                  err: scErrors.dehydrateError(err, true)
                });
              });
            } else if (data.brokerTest === 'test2') {
              this.sendRequestToMaster({
                sendBackError: true
              })
              .then((data) => {
                this.sendMessageToMaster({
                  brokerTestResult: 'test2',
                  data
                });
              })
              .catch((err) => {
                this.sendMessageToMaster({
                  brokerTestResult: 'test2',
                  err: scErrors.dehydrateError(err, true)
                });
              });
            } else if (data.brokerTest === 'test3') {
              this.sendRequestToMaster({
                doNothing: true
              })
              .then((data) => {
                this.sendMessageToMaster({
                  brokerTestResult: 'test3',
                  data
                });
              })
              .catch((err) => {
                this.sendMessageToMaster({
                  brokerTestResult: 'test3',
                  err: scErrors.dehydrateError(err, true)
                });
              });
            } else if (data.brokerTest === 'test4') {
              this.sendMessageToMaster({
                doNothing: true
              });
              setTimeout(() => {
                this.sendMessageToMaster({
                  brokerTestResult: 'test4',
                  err: null,
                  data: null
                });
              }, 1500);
            }
          }
        }
      }
    })();

    (async () => {
      for await (let req of this.listener('masterRequest')) {
        let data = req.data;
        if (data.sendBackError) {
          let err = new Error('This is an error');
          err.name = 'CustomBrokerError';
          req.error(err);
        } else if (!data.doNothing) {
          let responseData = {
            hello: data.subject
          };
          req.end(responseData);
        }
      }
    })();
  }
}

new BrokerControllerStub();
