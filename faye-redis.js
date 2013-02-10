// Constructor for multiRedis. It sets up two connections for each provided
// Redis URL and adds them to a ketema ring. One connection is used for
// commands and the other is used for pub/sub subscriptions.
var multiRedis = function(urls) {
  var consistentHashing = require('consistent-hashing'),
      self = this;

  self.ring          = new consistentHashing(urls);
  self.urls          = urls;
  self.servers       = {};
  self.connections   = {};
  self.subscriptions = {};

  urls.forEach(function(url) {
    var options = self.parse(url);

    self.servers[url] = options;
    self.connections[url] = self.connect(options);
    self.subscriptions[url] = self.connect(options);
  });
};

// Commands that are shardable (i.e., take a key as the first argument) and
// used by the faye-redis engine.
multiRedis.COMMANDS = ['zadd', 'zscore', 'smembers', 'del', 'zrem', 'sadd',
  'srem', 'rpush', 'expire', 'setnx', 'get', 'getset', 'zrangebyscore']

multiRedis.prototype = {
  // Grab the connection from the ring for the pub/sub server for the message
  // and delegate a publish call to it.
  publish: function(topic, message) {
    var connection = this.connectionFor(message);
    connection.publish.apply(connection, arguments);
  },

  // Subscribe to the topic on all of the subscription connections and call
  // the callback on a new message.
  subscribe: function(topic, callback) {
    var self = this;

    self.urls.forEach(function(url) {
      var subscription = self.subscriptions[url];

      subscription.subscribe(topic);
      subscription.on('message', callback);
    });
  },

  // Returns a connection based on a single key for dispatching multiple
  // connections atomically. You should only commit operations against a single
  // key during a multi due to the sharding.
  multi: function(key) {
    return this.connectionFor(key).multi();
  },

  // Returns a new Redis connection. Expects a server configuration object,
  // e.g.:
  //
  //   { port: 6379,
  //   hostname: 'localhost',
  //   database: 0,
  //   password: 'chunkybacon' }
  connect: function(server) {
    var redis = require('redis');
    var connection = redis.createClient(server.port, server.hostname);

    connection.select(server.database);

    if (server.password)
      connection.auth(server.password);

    return connection;
  },

  // Parses a URL and returns a server configuration object, e.g.:
  //
  // redis://:chunkybacon@localhost:6379/0
  parse: function(url) {
    var url = require('url').parse(url),
        connection = { hostname: url.hostname, port: url.port };

    if (url.auth)
      connection.password = url.auth.split(":")[1];

    if (url.path) {
      connection.database = url.path.substring(1);
    } else {
      connection.database = 0;
    }

    return connection;
  },

  // Closes all connections to Redis.
  end: function() {
    var self = this;

    self.urls.forEach(function(url) {
      self.connections[url].end();

      self.subscriptions[url].unsubscribe();
      self.subscriptions[url].end();
    });
  },

  // Returns a connection for a given key.
  connectionFor: function(key) {
    return this.connections[this.ring.getNode(key)];
  }
};

// Loops through the commands and adds each one to multiRedis.
multiRedis.COMMANDS.forEach(function(command) {
  multiRedis.prototype[command] = function() {
    var connection = this.connectionFor(arguments[0]);
    return connection[command].apply(connection, arguments);
  }
});

var Engine = function(server, options) {
  this._options = options || {};

  var gc   = this._options.gc || this.DEFAULT_GC,
      self = this;

  this._server     = server;
  this._ns         = this._options.namespace || '';
  this._redis      = new multiRedis(options.servers);

  this._redis.subscribe(this._ns + '/notifications', function(topic, message) {
    self.emptyQueue(message);
  });

  this._gc = setInterval(function() { self.gc() }, gc * 1000);
};

Engine.create = function(server, options) {
  return new this(server, options);
};

Engine.prototype = {
  DEFAULT_GC:       60,
  LOCK_TIMEOUT:     120,

  disconnect: function() {
    this._redis.end();
    clearInterval(this._gc);
  },

  createClient: function(callback, context) {
    var clientId = this._server.generateId(), self = this;
    this._redis.zadd(this._ns + '/clients', 0, clientId, function(error, added) {
      if (added === 0) return self.createClient(callback, context);
      self._server.debug('Created new client ?', clientId);
      self.ping(clientId);
      self._server.trigger('handshake', clientId);
      callback.call(context, clientId);
    });
  },

  clientExists: function(clientId, callback, context) {
    var timeout = this._server.timeout;

    this._redis.zscore(this._ns + '/clients', clientId, function(error, score) {
      if (timeout) {
        callback.call(context, score > new Date().getTime() - 1000 * 1.75 * timeout);
      } else {
        callback.call(context, score !== null);
      }
    });
  },

  destroyClient: function(clientId, callback, context) {
    var timeout = this._server.timeout, self = this;

    if (timeout) {
      this._redis.zadd(this._ns + '/clients', 0, clientId, function() {
        self._removeClient(clientId, callback, context);
      });
    } else {
      this._removeClient(clientId, callback, context);
    }
  },

  _removeClient: function(clientId, callback, context) {
    var self = this;

    this._redis.smembers(this._ns + '/clients/' + clientId + '/channels', function(error, channels) {
      var n = channels.length, i = 0;
      if (i === n) return self._afterSubscriptionsRemoved(clientId, callback, context);

      channels.forEach(function(channel) {
        self.unsubscribe(clientId, channel, function() {
          i += 1;
          if (i === n) self._afterSubscriptionsRemoved(clientId, callback, context);
        });
      });
    });
  },

  _afterSubscriptionsRemoved: function(clientId, callback, context) {
    var self = this;
    this._redis.del(this._ns + '/clients/' + clientId + '/messages', function() {
      self._redis.zrem(self._ns + '/clients', clientId, function() {
        self._server.debug('Destroyed client ?', clientId);
        self._server.trigger('disconnect', clientId);
        if (callback) callback.call(context);
      });
    });
  },

  ping: function(clientId) {
    var timeout = this._server.timeout;
    if (typeof timeout !== 'number') return;

    var time = new Date().getTime();

    this._server.debug('Ping ?, ?', clientId, time);
    this._redis.zadd(this._ns + '/clients', time, clientId);
  },

  subscribe: function(clientId, channel, callback, context) {
    var self = this;
    this._redis.sadd(this._ns + '/clients/' + clientId + '/channels', channel, function(error, added) {
      if (added === 1) self._server.trigger('subscribe', clientId, channel);
    });
    this._redis.sadd(this._ns + '/channels' + channel, clientId, function() {
      self._server.debug('Subscribed client ? to channel ?', clientId, channel);
      if (callback) callback.call(context);
    });
  },

  unsubscribe: function(clientId, channel, callback, context) {
    var self = this;
    this._redis.srem(this._ns + '/clients/' + clientId + '/channels', channel, function(error, removed) {
      if (removed === 1) self._server.trigger('unsubscribe', clientId, channel);
    });
    this._redis.srem(this._ns + '/channels' + channel, clientId, function() {
      self._server.debug('Unsubscribed client ? from channel ?', clientId, channel);
      if (callback) callback.call(context);
    });
  },

  publish: function(message, channels) {
    this._server.debug('Publishing message ?', message);

    var self        = this,
        notified    = [],
        jsonMessage = JSON.stringify(message),
        keys        = channels.map(function(c) { return self._ns + '/channels' + c });

    var notify = function(error, clients) {
      clients.forEach(function(clientId) {
        if (notified.indexOf(clientId) == -1) {
          self._server.debug('Queueing for client ?: ?', clientId, message);
          self._redis.rpush(self._ns + '/clients/' + clientId + '/messages', jsonMessage);
          self._redis.publish(self._ns + '/notifications', clientId);
          self._redis.expire(self._ns + '/clients/' + clientId + '/messages', 3600)
          self._checkClient(clientId);

          notified.push(clientId);
        }
      });
    };

    keys.forEach(function(key) {
      if (key.indexOf("*") == -1)
        self._redis.smembers(key, notify);
    });

    this._server.trigger('publish', message.clientId, message.channel, message.data);
  },

  _checkClient: function(clientId) {
    var self = this;

    this.clientExists(clientId, function(exists) {
      if (!exists) self.destroyClient(clientId);
    });
  },

  emptyQueue: function(clientId) {
    if (!this._server.hasConnection(clientId)) return;

    var key   = this._ns + '/clients/' + clientId + '/messages',
        multi = this._redis.multi(key),
        self  = this;

    multi.lrange(key, 0, -1, function(error, jsonMessages) {
      var messages = jsonMessages.map(function(json) { return JSON.parse(json) });
      self._server.deliver(clientId, messages);
    });
    multi.del(key);
    multi.exec();
  },

  gc: function() {
    var timeout = this._server.timeout;
    if (typeof timeout !== 'number') return;

    this._withLock('gc', function(releaseLock) {
      var cutoff = new Date().getTime() - 1000 * 2 * timeout,
          self   = this;

      this._redis.zrangebyscore(this._ns + '/clients', 0, cutoff, function(error, clients) {
        var i = 0, n = clients.length;
        if (i === n) return releaseLock();

        clients.forEach(function(clientId) {
          this.destroyClient(clientId, function() {
            i += 1;
            if (i === n) releaseLock();
          }, this);
        }, self);
      });
    }, this);
  },

  _withLock: function(lockName, callback, context) {
    var lockKey     = this._ns + '/locks/' + lockName,
        currentTime = new Date().getTime(),
        expiry      = currentTime + this.LOCK_TIMEOUT * 1000 + 1,
        self        = this;

    var releaseLock = function() {
      if (new Date().getTime() < expiry) self._redis.del(lockKey);
    };

    this._redis.setnx(lockKey, expiry, function(error, set) {
      if (set === 1) return callback.call(context, releaseLock);

      self._redis.get(lockKey, function(error, timeout) {
        if (!timeout) return;

        var lockTimeout = parseInt(timeout, 10);
        if (currentTime < lockTimeout) return;

        self._redis.getset(lockKey, expiry, function(error, oldValue) {
          if (oldValue !== timeout) return;
          callback.call(context, releaseLock);
        });
      });
    });
  }
};

module.exports = Engine;
