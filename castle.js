var forever = require('forever-monitor');

  var child = new (forever.Monitor)('core.js', {
    silent: false,
    options: []
  });

  child.on('exit', function () {
    console.log('Castle has stopped.');
  });

  child.start();
