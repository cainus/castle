//require('kafka');
require('mongodb');


var kafka = require('kafka');
var Consumer = kafka.Consumer;
var Producer = kafka.Producer;
var TOPIC = 'test';


var consumer = new Consumer();
consumer.on('message', function(message) {
  console.log('got a message');
  if (message != 'hey too!'){
    console.log("parsed: ", JSON.parse(message));
  }
});

consumer.on('lastmessage', function() {
  console.log("consumer: last message");
});
consumer.on('closed', function() {
  console.log("consumer: closed");
});
consumer.on('messageerror', function() {
  console.log("consumer: messageerror");
});
consumer.on('parseerror', function() {
  console.log("consumer: parseerror");
});
consumer.on('offset', function() {
  console.log("consumer: offset");
});

consumer.connect(function() {
  consumer.subscribeTopic({name: TOPIC, partition: 0});
  console.log("there are ", consumer.topics(), " topics.");
  //consumer.close();
});




var producer = new Producer(
    {
        // these are also the default values
        host:         'localhost',
        port:         9092,
        topic:        TOPIC,
        partition:    0
    }
);


console.log('about to produce...');
producer.connect().on('connect', function() {
  setInterval(function(){
    console.log('producing');
    var message = {
      text : "this is a message",
      when : new Date()
    };
    strmessage = JSON.stringify(message);
    console.log(strmessage);
    producer.send(strmessage);
  }, 4000);
  // producer.close();
});



