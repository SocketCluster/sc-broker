// To test with .mjs (ES6 modules)

import SCBroker from '../../scbroker';
import scErrors from 'sc-errors';
import addMiddleware from './middleware';

class BrokerControllerStub extends SCBroker {
  run() {
    var self = this;

    console.log('Start broker');
    addMiddleware(this);

    self.on('masterRequest', function (data, respond) {
      if (data.killBroker) {
        console.log('Broker is shutting down');
        process.exit();
      } else {
        if (data.brokerTest) {
          if (data.brokerTest === 'test1') {
            self.sendRequestToMaster({
              brokerSubject: 'there'
            }, function (err, data) {
              self.sendDataToMaster({
                brokerTestResult: 'test1',
                err: scErrors.dehydrateError(err, true),
                data: data
              });
            });
          } else if (data.brokerTest === 'test2') {
            self.sendRequestToMaster({
              sendBackError: true
            }, function (err, data) {
              self.sendDataToMaster({
                brokerTestResult: 'test2',
                err: scErrors.dehydrateError(err, true),
                data: data
              });
            });
          } else if (data.brokerTest === 'test3') {
            self.sendRequestToMaster({
              doNothing: true
            }, function (err, data) {
              self.sendDataToMaster({
                brokerTestResult: 'test3',
                err: scErrors.dehydrateError(err, true),
                data: data
              });
            });
          } else if (data.brokerTest === 'test4') {
            self.sendDataToMaster({
              doNothing: true
            });
            setTimeout(function () {
              self.sendDataToMaster({
                brokerTestResult: 'test4',
                err: null,
                data: null
              });
            }, 1500);
          }
        } else if (data.sendBackError) {
          var err = new Error('This is an error');
          err.name = 'CustomBrokerError';
          respond(err);
        } else if (!data.doNothing) {
          var responseData = {
            hello: data.subject
          };
          respond(null, responseData);
        }
      }
    });
  }
}

new BrokerControllerStub();
