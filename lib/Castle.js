var Consumer = require('prozess').Consumer;
var Percolator = require('Percolator').Percolator;
var util = require('util');

var Castle = function(options){
  options = options || {};
  options.http_port = options.http_port || 8080;
  options.kafka_port = options.kafka_port || 9092;
  options.kafka_host = options.kafka_host || 'localhost'; 
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
  this.percolator = new Percolator({ port : this.port, topics : this.topics });
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
    GET : function(req, res){
      var offset = req.uri.query().offset;
      var partition = req.uri.child();
      var topic = req.uri.parent().child();
      if (!offset){
        console.log("getting offsets...");
        getOffsets(req.app.kafka_host, topic, partition, function(err, offsets){
          console.log("result: ", err, offsets);
          return res.status.redirect( 
                    req.uri.query({offset : offsets.offsets[0].toString()}) );
        });
        return; 
      }
      getMessages(req.app.kafka_host, topic, partition, offset, function(err, messages){
        if (err){
          res.status.internalServer(err);
        }
        var items = messages;
        res.object({ topic : topic, 
                     partition : partition, 
                     offset : offset, 
                     _items : items })
                     .link("next", req.uri.query({offset:9999}))
                     .send();   
      });
    }
  });

  this.percolator.listen(function(err){
    cb(err);
  });
};

module.exports = Castle;

var getOffsets = function(host, topic, partition, cb){
  var options = {host : host, topic : 'social', partition : 0};
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
  var options = {host : host, topic : 'social', partition : 0, offset : 0};
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
