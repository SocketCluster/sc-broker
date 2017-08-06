var scErrors = require('sc-errors');

module.exports.run = function (scBroker) {
  console.log('Start broker');

  scBroker.on('masterMessage', function (data, respond) {
    if (data.killBroker) {
      console.log('Broker is shutting down');
      process.exit();
    } else {
      if (data.brokerTest) {
        if (data.brokerTest == 'test1') {
          scBroker.sendToMaster({
            brokerSubject: 'there'
          }, function (err, data) {
            scBroker.sendToMaster({
              brokerTestResult: 'test1',
              err: scErrors.dehydrateError(err, true),
              data: data
            });
          });
        } else if (data.brokerTest == 'test2') {
          scBroker.sendToMaster({
            sendBackError: true
          }, function (err, data) {
            scBroker.sendToMaster({
              brokerTestResult: 'test2',
              err: scErrors.dehydrateError(err, true),
              data: data
            });
          });
        } else if (data.brokerTest == 'test3') {
          scBroker.sendToMaster({
            doNothing: true
          }, function (err, data) {
            scBroker.sendToMaster({
              brokerTestResult: 'test3',
              err: scErrors.dehydrateError(err, true),
              data: data
            });
          });
        } else if (data.brokerTest == 'test4') {
          scBroker.sendToMaster({
            doNothing: true
          });
          setTimeout(function () {
            scBroker.sendToMaster({
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
};
