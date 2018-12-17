module.exports = function (scBroker) {
  let hasSeenAllowOnceChannelAlready = false;

  scBroker.addMiddleware(scBroker.MIDDLEWARE_SUBSCRIBE, async (req) => {
    if (req.channel === 'allowOnce') {
      if (hasSeenAllowOnceChannelAlready) {
        let onlyOnceError = new Error('Can only subscribe once to the allowOnce channel')
        onlyOnceError.name = 'OnlyOnceError';
        throw onlyOnceError;
      }
      hasSeenAllowOnceChannelAlready = true;
    }
    if (req.channel === 'badChannel') {
      throw new Error('bad channel');
    }

    if (req.channel === 'delayedChannel') {
      await wait(500);
    }
  });

  scBroker.addMiddleware(scBroker.MIDDLEWARE_PUBLISH_IN, async (req) => {
    if (req.channel === 'silentChannel') {
      throw new Error('silent channel');
    } else if (req.command.value === 'test message') {
      req.command.value = 'transformed test message';
    }

    if (req.channel === 'delayedChannel') {
      await wait(500);
    }
  });

  // Ensure middleware can be removed
  let badMiddleware = async (req) => {
    throw new Error('This code should be unreachable!');
  };
  scBroker.addMiddleware(scBroker.MIDDLEWARE_SUBSCRIBE, badMiddleware);
  scBroker.addMiddleware(scBroker.MIDDLEWARE_PUBLISH_IN, badMiddleware);
  scBroker.removeMiddleware(scBroker.MIDDLEWARE_SUBSCRIBE, badMiddleware);
  scBroker.removeMiddleware(scBroker.MIDDLEWARE_PUBLISH_IN, badMiddleware);
};

function wait(duration) {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, duration);
  });
}
