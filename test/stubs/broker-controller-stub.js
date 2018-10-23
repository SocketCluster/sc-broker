var SCBroker = require('../../scbroker');
var scErrors = require('sc-errors');
var addMiddleware = require('./middleware');

class BrokerControllerStub extends SCBroker {
  run() {
    console.log('Start broker');
    addMiddleware(this);

    var dataBuffer = [];

    this.on('request', (value, respond) => {
      if (value && value.getDataBuffer) {
        respond(null, dataBuffer);
        dataBuffer = [];
      } else {
        respond(null, value + 1);
      }
    });

    this.on('message', (value) => {
      dataBuffer.push(value);
    });

    this.on('masterMessage', (data) => {
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
                data: data
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
                data: data
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
                data: data
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
    });

    this.on('masterRequest', (data, respond) => {
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
