sc-broker
======

*scBroker* is a lightweight key-value store and message broker.
It is written entirely in node.js for maximum portability.

## Installation

```bash
npm install sc-broker
```

## Overview

To use it call:

```js
var scBroker = require('sc-broker');
```

Firstly, launch a new *scBroker* server. If you're using the node cluster module,
you might want to launch the *scBroker* server once from the master process and
then interact with it using *scBroker* clients.

## Server

To launch the **server**, use:

```js
var dataServer = scBroker.createServer({port: 9000, secretKey: 'mySecretKey'})
```

The ```secretKey``` argument is optional; you should use it if you want to
restrict access to the server. If you're running a node cluster, you may want
to use a random key and distribute it to all the workers so that only your
application can interact with the *scBroker* server.

Once the server is setup, you should create clients to interact with it.

**Make sure that the server is running before creating clients**

This ca be done in the following way:

```js
var conf = {port: 9000}
  , server = scBroker.createServer(conf);

server.on('ready', function () {

 console.log('Server ready, create client');
 var client = scBroker.createClient(conf);

 // do client stuff

});
```
After all the server provides a destroy function:

```js
server.destroy()
```

## Client

To create a **client** use:

```js
var dataClient = scBroker.createClient({port: 9000, secretKey: 'mySecretKey'});
```

The ```port``` and ```secretKey``` must match those supplied to the
createServer function.

### Client methods

The client exposes the following methods:
(Please see the [section on keys ](https://github.com/SocketCluster/sc-broker#keys) to
see how you can use keys in *scBroker*.


#### exec

```js
exec(code,[ data])
```
Run a special JavaScript function
declaration (code) as a query *on the scBroker server*. This function declaration
accepts the DataMap as a parameter.
This is the most important function in *scBroker*, all the other functions are
basically utility functions to make things quicker. Using exec() offers the
most flexibility. Returns a Promise; on success resolves to the return value of the query function.

**Example:**

```js
var queryFn = function (DataMap) {
    // The myMessage variable comes from queryFn.data
    DataMap.set(['main', 'message'], myMessage);
    return DataMap.get(['main']);
};

queryFn.data = {
    myMessage: 'This is an important message'
};

client.exec(queryFn)
.then((data) => {
    console.log(data); // outputs {message: "This is an important message"}
})
.catch((err) => {
  // ...
});
```
**Note**

The *query functions* are **not** regular functions. Query functions are
executed remotely (on the *scBroker* server), therefore, you cannot access
variables from the outer parent scope while inside them.

To pass data from the current process to use inside your query functions, you
need to set them through the data  property (see ```queryFn.data```) in
example above. Properties of ```queryFn.data``` will be available as regular
variables inside the query function when it gets executed on the server.
All query data is escaped automatically, so it's safe to supply user
input. The ```queryFn.data``` property is optional.


#### set

```js
set(keyChain, value, options)
```
Set a key-value pair. Returns a Promise.

#### add

```js
add(keyChain, value)
```
Append a value at the given ```keyChain```; the object at ```keyChain``` will
be *treated as an array*. If a value already exists at that ```keyChain``` and
is not an array, this existing value will be placed inside an empty array and
the specified value argument will be appended to that array. Returns a Promise.

#### concat

```js
concat(keyChain, value,[ options])
```
Concatenate the array or object at ```keyChain``` with the specified array or
object (```value```). Returns a Promise.

#### remove

```js
remove(keyChain,[ options])
```
Remove the value at ```keyChain```. If value is an array, it will remove the
entire array. The optional ```options.getValue``` is a *boolean* which indicates
whether or not to *return* the removed value *in the Promise*. Returns a Promise.

#### removeRange

```js
removeRange(keyChain, options)
```
Remove a range of values at ```keyChain``` between ```options.fromIndex``` and
```options.toIndex```. This function assumes that the value at ```keyChain``` is an
object or array. The optional ```options.getValue``` argument specifies whether or not
to *return* the removed section as an *argument to the Promise*. Returns a Promise.

#### removeAll

```js
removeAll()
```
Clear *scBroker* *completely*. Returns a Promise.

#### splice

```js
splice(keyChain,[ options])
```

This operation is designed to work on Arrays (the keyChain argument should point to an Array).
It is similar to JavaScript's Array.splice() function. It can be used to remove and insert elements
within an Array.
The options argument is an object which can have the following properties:
- fromIndex // The index at which to start inserting/deleting
- count // The number of items to delete starting from index
- items // An Array of items to insert at index

Returns a Promise.

#### pop

```js
pop(keyChain,[ options])
```

Remove the *last numerically-indexed entry* at ```keyChain```. The optional
```options.getValue``` is a *boolean* which indicates whether or not to *return* the
removed value in the Promise. Returns a Promise.

#### get

```js
get(keyChain)
```
Get the value at ```keyChain```. Returns a Promise.

#### getRange

```js
getRange(keyChain,[ options])
```
This function assumes that the value at ```keyChain``` is an Array or Object.
Capture all values starting at ```options.fromIndex``` and finishing at ```options.toIndex```
but **not including** ```options.toIndex```. If ```options.toIndex``` is not specified, all
values from ```options.fromIndex``` until the end of the Array or Object will be
included. Returns a Promise.

#### getAll

```js
getAll()
```
Get all the values in *scBroker*. Returns a Promise.

#### count

```js
count(keyChain)
```
Count the number of elements at ```keyChain```. Returns a Promise.

## publish subscribe

*scBroker* provides [publish and subscribe](http://redis.io/topics/pubsub)
 functionality.


#### subscribe

```js
subscribe(channel)
```
Watch a ```channel``` on *scBroker*. This is the *scBroker* equivalent to
[Redis' ```subscribe()```](http://redis.io/commands/subscribe). When an event
happens on any watched channel, you can handle it using
```js
scBrokerClient.on('message', function (channel, data) {
    // ...
})
```
Returns a Promise.

#### unsubscribe

```js
unsubscribe(channel)
```
Unwatch the specified ```channel```. If ```channel``` is not specified, it
will unsubscribe from all channels. Returns a Promise.

#### on

```js
on(event, listener)
```
Listen to events on *scBroker*, you should listen to the 'message' event to handle
messages from subscribed channels. Events are:

* ```'ready'```: Triggers when *scBroker* is initialized and connected. You often
    don't need to wait for that event though. The *scBroker* client will buffer
    actions until the *scBroker* server ready.
* ```'exit'``` This event carries two arguments to it's listener: ```code```
    and ```signal```. It gets triggered when the *scBroker* **server** process
    dies.
* ```'connect_failed'``` This happens if the *scBroker* **client** fails to
    connect to the server after the maximum number of retries have been
    attempted.
* ```'message'``` Captures data published to a channel which the client is
    subscribed to.
* ```'subscribe'``` Triggers whenever a successful subscribe operations occurs.
* ```'subscribefail'``` Triggers whenever a subscribtion fails.
* ```'unsubscribe'``` Triggers on a successful unsubscribe operation.
* ```'unsubscribefail'``` Triggers whenever a unsubscribtion fails.
* ```'error'``` Triggers whenever a error occurs.

#### publish

```js
publish(channel, message)
```
Publish data to a channel - Can be any JSON-compatible JavaScript object.
Returns a Promise.

**Example:**

After starting the server (*server.js*):

```js
var scBroker = require('sc-broker');
var dss = scBroker.createServer({port: 9000});
```

a first client (*client1.js*) can subscribe to channel ```foo``` and listen
to ```messages```:

```js
var scBroker = require('sc-broker');
var dc = scBroker.createClient({port: 9000});
var ch = 'foo';
var onMsgFn = function (ch, data) {
  console.log('message on channel ' + ch);
  console.log('data:');
  console.log(data);
};
dc.subscribe(ch)
.then(() => {
  console.log('client 1 subscribed channel ' + ch);
})
.catch((err) => {
  console.error(err);
})
dc.on('message', onMsgFn)
```

If a second client (*client2.js*) publishes a message, the first client will
execute the ```onMsgFn``` function:

```js
var scBroker = require('sc-broker');
var dc = scBroker.createClient({port: 9000});
var data = {a: 'b'};
var ch = 'foo';

dc.publish(ch,data)
.then(() => {
  console.log('client 2 published data:');
  console.log(data);
})
.catch((err) => {
  console.error(err);
});
```

## Keys

*scBroker* is very flexible with how you can use keys. It lets you set key chains
of any dimension without having to manually create each link in the chain.

A key chain is an array of keys - Each subsequent key in the chain is a child
of the previous key.
For example, consider the following object:
```js
{'this': {'is': {'a': {'key': 123}}}}
```
The key chain ```['this', 'is', 'a', 'key']``` would reference the number
```123```. The key chain ```['this', 'is']``` would reference the object
```{'a': {'key': 123}}```, etc.

When you start, *scBroker* will be empty, but this code is perfectly valid:
```js
dataClient.set(['this', 'is', 'a', 'deep', 'key'], 'Hello world');
```
In this case, *scBroker* will *create* the necessary key chain and set the
bottom-level 'key' to 'Hello World'.
If you were to call:
```js
dataClient.get(['this', 'is', 'a'], function (err, val) {
  console.log(val);
});
```
The above would output:
```js
{deep:{key:'Hello world'}}
```

*scBroker* generally doesn't restrict you from doing anything you want. Following
from the previous example, it is perfectly OK to call this:
```js
dataClient.add(['this', 'is', 'a'], 'foo');
```
In this case, the key chain ```['this', 'is', 'a']``` would evaluate to:
```js
{0:'foo', deep:{key:'Hello world'}}
```
In this case, *scBroker* will add the value at the next numeric index in the
specified key path (which in this case is 0).

You can access numerically-indexed values like this:
```js
dataClient.get(['this', 'is', 'a', 0])
.then((val) => {
  console.log(val);
})
.catch((err) => {
  console.error(err);
});
```
The output here will be 'foo'.
You can also add entire JSON-compatible objects as value.


## Tests

To run tests, go to the sc-broker module directory then run:

```bash
npm test
```

If you get an error, make sure that you have mocha installed:

```bash
npm install mocha
```
