// Constructor for multiRedis. It sets up two connections for each provided
// Redis URL and adds them to a ketema ring. One connection is used for
// commands and the other is used for pub/sub subscriptions.
var multiRedis = function(urls) {
  var hasher = require('consistent-hashing'),
      self   = this;

  self.ring          = new hasher(urls);
  self.urls          = urls;
  self.connections   = {};
  self.subscriptions = {};

  urls.forEach(function(url) {
    var options = self.parse(url);

    var connection   = self.connect(options);
    var subscription = self.connect(options);

    self.connections[url]   = connection;
    self.subscriptions[url] = subscription;
  });
};

// [ command, argument-to-shard-against ]
multiRedis.COMMANDS = [
  ['smembers', 0],
  ['del', 0],
  ['sadd', 0],
  ['srem', 0],
  ['rpush', 0],
  ['expire', 0],
  ['get', 0],
  ['getset', 0],
  ['zrem', 1],
  ['zadd', 2],
  ['zscore', 1]
];

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
    var redis      = require('redis'),
        connection = redis.createClient(server.port, server.hostname);

    connection.select(server.database);

    if (server.password)
      connection.auth(server.password);

    return connection;
  },

  // Parses a URL and returns a server configuration object, e.g.:
  //
  // redis://:chunkybacon@localhost:6379/0
  parse: function(url) {
    var url        = require('url').parse(url),
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
  var redisCommand = command[0],
      argument = command[1];

  multiRedis.prototype[redisCommand] = function() {
    var connection = this.connectionFor(arguments[argument]);
    return connection[redisCommand].apply(connection, arguments);
  }
});

// Creates a new Faye Redis engine.
//
// Options:
//   disable_subscriptions If set to `true`, then this engine will not subscribe
//                         to the notifications channel.
//
//   gc                    By default, this engine prunes inactive clients by
//                         running a GC function on an interval. If this option
//                         is omitted, the GC runs every DEFAULT_GC seconds.
//                         To change the interval, set this option to an integer
//                         number of seconds. To completely disable GC, set this
//                         to `false`.
//
//   gc_limit              By default, the GC tries to prune all expired
//                         clients. This can be set to an integer, however, to
//                         limit the number of expired clients to process in a
//                         given GC cycle. Highly recommended.
//
var Engine = function(server, options) {
  this._options = options || {};

  var self = this, gc;

  if (this._options.gc === false) {
    gc = false;
  } else {
    gc = this._options.gc || this.DEFAULT_GC;
  }

  this._server     = server;
  this._ns         = this._options.namespace || '';
  this._redis      = new multiRedis(options.servers);

  if (!this._options.disable_subscriptions) {
    this._redis.subscribe(this._ns + '/notifications', function(topic, message) {
      self.emptyQueue(message);
    });
  }

  if (gc) {
    this._gc_limit = this._options.gc_limit;
    this._gc = setInterval(function() { self.gc() }, gc * 1000);
  }
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
    var clientId = this._server.generateId(),
        score = new Date().getTime(),
        self = this;

    this._redis.zadd(this._ns + '/clients', score, clientId, function(error, added) {
      if (added === 0) return self.createClient(callback, context);
      self._server.debug('Created new client ? with score ?', clientId, score);
      self._server.trigger('handshake', clientId);
      callback.call(context, clientId);
    });
  },

  clientExists: function(clientId, callback, context) {
    var timeout = this._server.timeout;

    if (clientId === undefined) {
      this._server.debug("[RedisEngine#clientExists] undefined clientId, returning false");
      return callback.call(context, false);
    }

    this._redis.zscore(this._ns + '/clients', clientId, function(error, score) {
      if (timeout) {
        callback.call(context, score > new Date().getTime() - 1000 * 1.75 * timeout);
      } else {
        callback.call(context, score !== null);
      }
    });
  },

  // Destroy a client.
  //
  // The first part of cleaning up a client is removing subscriptions, which
  // removes the client ID from all the channels that it's a member of. This
  // prevents messages from being published to that client.
  //
  // For any Redis failures, we simply return without calling any further
  // callbacks. This stops the client cleanup, but that's okay. Since the
  // client ID is still in the sorted set, it will get mopped up in the next
  // GC cycle (hopefully).
  destroyClient: function(clientId, callback, context) {
    var self = this;
    var clientChannelsKey = this._ns + "/clients/" + clientId + "/channels";

    this._redis.smembers(clientChannelsKey, function(error, channels) {
      if (error) {
        return self._server.error("Failed to fetch channels: ?", error);
      }

      var numChannels = channels.length, numUnsubscribes = 0;

      if (numChannels == 0) {
        return self._deleteClient(clientId, callback, context);
      }

      channels.forEach(function(channel) {
        var channelsKey = self._ns + "/channels" + channel;
        self._redis.srem(channelsKey, clientId, function(error, res) {
          if (error) {
            return self._server.error("Failed to remove client ? from ?: ?", clientId, channelsKey, error);
          }
          numUnsubscribes += 1;
          self._server.trigger("unsubscribe", clientId, channel);
          if (numUnsubscribes == numChannels) {
            self._deleteClient(clientId, callback, context);
          }
        });
      });
    });
  },

  // Removes the client bookkeeping records.
  //
  // Finishes client cleanup by removing the mailbox, channel set, and finally
  // the client ID from the sorted set. Once again, any Redis errors shut down
  // the callback chain, and we'll rely on GC to pick it back up again.
  _deleteClient: function(clientId, callback, context) {
    var self = this,
        clientChannelsKey = this._ns + "/clients/" + clientId + "/channels",
        clientMessagesKey = this._ns + "/clients/" + clientId + "/messages";

    this._redis.del(clientChannelsKey, function(error, res) {
      if (error) {
        return self._server.error("Failed to remove client channels ?: ?", clientChannelsKey, error);
      }
      self._redis.del(clientMessagesKey, function(error, res) {
        if (error) {
          return self._server.error("Failed to remove client messages ?: ?", clientMessagesKey, error);
        }
        self._redis.zrem(self._ns + "/clients", clientId, function(error, res) {
          if (error) {
            return self._server.error("Failed to remove client ID ? from /clients: ?", clientId, error);
          }
          self._server.debug("Destroyed client ? successfully", clientId);
          self._server.trigger("disconnect", clientId);
          if (callback) {
            callback.call(context);
          }
        });
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
      if (error) {
        return self._server.error("Failed to fetch clients, candidate channels ?: ?", keys, error);
      }
      clients.forEach(function(clientId) {
        if (notified.indexOf(clientId) == -1) {
          self._server.debug('Queueing for client ?: ?', clientId, message);
          self._redis.rpush(self._ns + '/clients/' + clientId + '/messages', jsonMessage);
          self._redis.publish(self._ns + '/notifications', clientId);
          self._redis.expire(self._ns + '/clients/' + clientId + '/messages', 3600)

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

    this._redis.urls.forEach(function(url) {
      this._server.debug("Running GC for ?", url);

      var connection = this._redis.connections[url],
          cutoff = new Date().getTime() - 1000 * 2 * timeout,
          self = this,
          args;

      var pruneClientsCallback = function pruneClientsCallback(error, clients) {
        if (error) {
          return self._server.error("Failed to fetch clients to GC: ?", error);
        }
        self._server.info("Running GC for ? clients ...", clients.length);
        clients.forEach(function(clientId) {
          self._server.debug("GC time for ? ...", clientId);
          self.destroyClient(clientId);
        });
      };

      if (this._gc_limit) {
        args = [this._ns + "/clients", 0, cutoff, "LIMIT", 0, this._gc_limit, pruneClientsCallback];
      } else {
        args = [this._ns + "/clients", 0, cutoff, pruneClientsCallback];
      }

      connection.zrangebyscore.apply(connection, args);
    }, this);
  }
};

module.exports = Engine;
