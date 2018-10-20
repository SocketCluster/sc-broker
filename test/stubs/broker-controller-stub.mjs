// To test with .mjs (ES6 modules)

import SCBroker from '../../scbroker';
import scErrors from 'sc-errors';
import addMiddleware from './middleware';

class BrokerControllerStub extends SCBroker {
  run() {
    var self = this;

    console.log('Start broker');
    addMiddleware(self);

    var dataBuffer = [];

    self.on('request', (value, respond) => {
      if (value && value.getDataBuffer) {
        respond(null, dataBuffer);
        dataBuffer = [];
      } else {
        respond(null, value + 1);
      }
    });

    self.on('message', (value) => {
      dataBuffer.push(value);
    });

    self.on('masterMessage', (data) => {
      if (data.killBroker) {
        console.log('Broker is shutting down');
        process.exit();
      } else {
        if (data.brokerTest) {
          if (data.brokerTest === 'test1') {
            self.sendRequestToMaster({
              brokerSubject: 'there'
            })
            .then((data) => {
              self.sendMessageToMaster({
                brokerTestResult: 'test1',
                data: data
              });
            })
            .catch((err) => {
              self.sendMessageToMaster({
                brokerTestResult: 'test1',
                err: scErrors.dehydrateError(err, true)
              });
            });
          } else if (data.brokerTest === 'test2') {
            self.sendRequestToMaster({
              sendBackError: true
            })
            .then((data) => {
              self.sendMessageToMaster({
                brokerTestResult: 'test2',
                data: data
              });
            })
            .catch((err) => {
              self.sendMessageToMaster({
                brokerTestResult: 'test2',
                err: scErrors.dehydrateError(err, true)
              });
            });
          } else if (data.brokerTest === 'test3') {
            self.sendRequestToMaster({
              doNothing: true
            })
            .then((data) => {
              self.sendMessageToMaster({
                brokerTestResult: 'test3',
                data: data
              });
            })
            .catch((err) => {
              self.sendMessageToMaster({
                brokerTestResult: 'test3',
                err: scErrors.dehydrateError(err, true)
              });
            });
          } else if (data.brokerTest === 'test4') {
            self.sendMessageToMaster({
              doNothing: true
            });
            setTimeout(() => {
              self.sendMessageToMaster({
                brokerTestResult: 'test4',
                err: null,
                data: null
              });
            }, 1500);
          }
        }
      }
    });

    self.on('masterRequest', (data, respond) => {
      if (data.sendBackError) {
        var err = new Error('This is an error');
        err.name = 'CustomBrokerError';
        respond(err);
      } else if (!data.doNothing) {
        var responseData = {
          hello: data.subject
        };
        respond(null, responseData);
      }
    });
  }
}

new BrokerControllerStub();
