var Castle = require('./index').Castle;

var config = {
  'http_port' : 8080,
  'kafka_port' : 9092,
  'kafka_host' : 'cfkafka01.stg.cotweet.com'
};
var server = new Castle(config);
server.addTopic('suckerfishstaging01', 1);
server.listen(function(err){
  console.log("server listening on port ", server.port);
});


