var Consumer = require('prozess').Consumer;
var Producer = require('prozess').Producer;
var Percolator = require('Percolator').Percolator;
var util = require('util');
var bignum = require('bignum');

var Castle = function(options){
  options = options || {};
  options.http_port = options.http_port || 8080;
  options.kafka_port = options.kafka_port || 9092;
  options.kafka_host = options.kafka_host || 'localhost';
  this.options = options;
  this.port = options.http_port;
  this.topics = [];
};

Castle.prototype.addTopic = function(name, partitions){
  if (partitions < 1){
    throw "a topic must have more than one partition.";
  }
  var partitionList = [];
  for (var i = 0; i < partitions; i++){
    partitionList.push(i);
  }
  this.topics.push({name : name, partitions : partitionList});
};

Castle.prototype.listen = function(cb){
  if (this.topics.length === 0){
    throw "you must add topics for the server to serve.";
  }
  this.percolator = new Percolator({ port : this.port,
                                     kafka_host : this.options.kafka_host,
                                     kafka_port : this.options.kafka_port,
                                     topics : this.topics });
  this.percolator.route('/', { 
    GET : function(req, res){
      res.object({})
        .link("topics", req.uri.child("topic"))
        .link("browser", req.uri.child("browser"))
        .send();
    }
  });
  this.percolator.route('/topic', { 
    GET : function(req, res){
      res.collection(req.app.topics)
        .linkEach("self", function(topic){ 
          return req.uri.child(topic.name);
        })
        .link("create", 
                req.uri.rawChild('{topic}').rawChild('{partition}'),
                { method : "POST" })
        .send();
    }
  });

  this.percolator.route('/topic/:topic', {
    GET : function(req, res){
      console.log("partitions: ", req.fetched.partitions);
      var partitions = [];
      req.fetched.partitions.forEach(function(partition){
        partitions.push({ number : partition });
      });
      res.collection(partitions)
        .linkEach("self", function(partition){ 
          return req.uri.child('' + partition.number);
        })
        .send();
    },

    fetch : function(req, res, cb){
      var found = false;
      req.app.topics.forEach(function(topic){
        if (topic.name === req.uri.child()){
          found = topic;
        }
      });
      if (!found){
        return cb(true);
      } else {
        return cb(null, found);
      }
    }
  });

  this.percolator.route('/topic/:topic/:partition', {
    
    POST : function(req, res){
      req.onBody(function(err, body){
        if (err){
          return res.status.internalServerError(err);
        }
        var topic = req.fetched.name;
        var partition = req.fetched.partition;
        console.log(body);
        writeMessage(req.app.kafka_host, 
                     req.app.kafka_port, 
                     topic, 
                     partition, 
                     body, 
                     function(err){
                       if (err){
                         return res.status.internalServerError(err);
                       } else {
                         return res.status.accepted();
                       }
                     });

      });
    },

    GET : function(req, res){
      console.log('fetched: ', req.fetched);
      var frontOffset = 0;
                    // TODO: make this the beginning instead
                    // 0 isn't always correct
      var offset = req.uri.query().offset || frontOffset;  
      offset = bignum(offset);
      var partition = req.fetched.partition;
      var topic = req.fetched.name;
      console.log("getting offsets...");
      
      getOffsets(req.app.kafka_host, topic, partition, function(err, offsets){
        if (err){
          if (err.code === "ECONNREFUSED"){
            return res.status.internalServerError("Can't connect to Kafka");
          }
          return res.status.internalServerError(err);
        }
        console.log("result: ", err, offsets);
        var back = offsets.offsets[0];
      
        getMessages(req.app.kafka_host, topic, partition, offset, function(err, messages){
          if (err){
            res.status.internalServerError(err);
          }
          var offsetDelta = 0;
          var items = messages.map(function(item){
            console.log("ITEM: ", item);
            offsetDelta += item.bytesLengthVal;
            return item.payload.toString("UTF8");
          });
          var nextOffset = offset.add(offsetDelta);
          res.object({ topic : topic,
                       partition : partition,
                       offset : offset.toString(),
                       _items : items })
                       .link("next", req.uri.query({offset:nextOffset.toString()}))
                       .link("front", req.uri.query({offset:frontOffset.toString()}))
                       .link("back", req.uri.query({offset:back.toString()}))
                       .link("self", req.uri)
                       .link("create", req.uri.query(false), { method : "POST" })
                       .send();
        });
      });
    },

    fetch : function(req, res, cb){
      var partition = parseInt(req.uri.child(), 10);
      if (isNaN(partition)){
        return res.status.badRequest("partition must be an integer: ", req.uri.child());
      }
      if (req.method === "POST"){
        return cb(null, { partition : partition, name : req.uri.parent().child() });
      }
      var found = false;
      req.app.topics.forEach(function(topic){
        if (topic.name === req.uri.parent().child()){
          found = topic;
        }
      });
      if (!found){
        console.log("not found due to topic");
        return cb(true);
      } else {
        if (found.partitions.indexOf(partition) !== -1){
          found.partition = partition;
          return cb(null, found);
        } else {
          console.log("not found due to partition: ", found);
          return cb(true);
        }
      }
    }
  });

  this.percolator.listen(function(err){
    cb(err);
  });
};

module.exports = Castle;

var getOffsets = function(host, topic, partition, cb){
  var options = {host : host, topic : topic, partition : partition};
  console.log("getOffsets options: ", options);
  var consumer = new Consumer(options);
  consumer.connect(function(err){
    if (err) {  
      return cb(err); 
    }
    consumer.getOffsets(function(err, offsets){
      return cb(err, offsets);
    });
  });
};
var getMessages = function(host, topic, partition, offset, cb){
  var options = {host : host, topic : topic, partition : partition, offset : offset};
  console.log(options);
  var consumer = new Consumer(options);
  consumer.connect(function(err){
    if (err) {  
      return cb(err); 
    }
    consumer.consume(function(err, messages){
      return cb(err, messages);
    });
  });
};

var writeMessage = function(host, port, topic, partition, message, cb){

  var producer = new Producer(topic, {host : host, 
                                      partition : partition, 
                                      port : port});
  producer.connect();
  console.log("producing for ", producer.topic);
  producer.on('error', function(err){
    console.log("some general error occurred: ", err);  
  });
  producer.on('brokerReconnectError', function(err){
    console.log("could not reconnect: ", err);  
    console.log("will retry on next send()");  
  });

  producer.send(message, function(err){
    if (err){
      return cb(err);
    } else {
      return cb();
    }
  });

};
