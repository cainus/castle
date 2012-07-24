var kafka = require('kafka');
var Consumer = kafka.Consumer;
var Producer = kafka.Producer;
var TOPIC = 'messages';
var MONGO_HOST = '127.0.0.1';
var MONGO_PORT = 27017;
var MONGO_DB = 'messages';
var KAFKA_HOST = '127.0.0.1';
var KAFKA_PORT = 9092;
var Mongo = require('mongodb');
var consumer = new Consumer();


// MONGO ---------------------
console.log("Connecting to mongo at ", MONGO_HOST, ":", MONOGO_PORT, '/', MONGO_DB);
var mongoClient = new Mongo.Db(MONGO_DB, new Mongo.Server(MONGO_HOST, MONGO_PORT, {}));


function mongoInsert(mongoClient, doc){
  mongoClient.collection('test_insert', function(err, collection){
      collection.insert(doc, function(err, docs) {
        if (err){
          console.log("mongo insert error: ", err);
          return;
        }
        console.log("inserted...", docs);
      });
  });
}

consumer.on('message', function(message) {
  var parsed = JSON.parse(message);
  console.log('got a message');
  console.log("parsed: ", parsed);
  mongoInsert(mongoClient, parsed);
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




mongoClient.open(function(err, p_client) {

  consumer.connect(function() {
    consumer.subscribeTopic({name: TOPIC, partition: 0});
    console.log("there are ", consumer.topics(), " topics.");
    //consumer.close();
  });

});




var producer = new Producer(
    {
        // these are also the default values
        host:         KAFKA_HOST,
        port:         KAFKA_PORT,
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





