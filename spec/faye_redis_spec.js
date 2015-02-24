var RedisEngine = require('../faye-redis')

JS.ENV.FayeRedisSpec = JS.Test.describe("Redis engine", function() { with(this) {
  before(function() {
    this.engineOpts = {
      type: RedisEngine,
      namespace: new Date().getTime().toString(),
      servers: [
        "redis://user:foobared@localhost:16379/0"
      ]
    }
  })

  after(function(resume) { with(this) {
    sync(function() {
      engine.disconnect()
      var redis = require('redis').createClient(16379, 'localhost', {no_ready_check: true})
      redis.auth('foobared')
      redis.flushall(function() {
        redis.end()
        resume()
      })
    })
  }})

  itShouldBehaveLike("faye engine")

  describe("distribution", function() { with(this) {
    itShouldBehaveLike("distributed engine")
  }})

  describe("custom engine options", function() { with(this) {
    it("disables GC if the 'gc' option is set to false", function() { with(this) {
      this.engineOpts.gc = false;
      this.engine = new Faye.Engine.Proxy(this.engineOpts);
      var redisEngine = this.engine._engine;
      this.assertEqual(undefined, redisEngine._gc);
    }})

    it("does not disable GC if the 'gc' option is simply unset", function() { with(this) {
      this.engineOpts.gc = null;
      this.engine = new Faye.Engine.Proxy(this.engineOpts);
      var redisEngine = this.engine._engine;
      this.assertNotNull(redisEngine._gc);
    }})

    it("calls a custom Redis error callback if provided", function(resume) { with(this) {
      var errorArgs;
      this.engineOpts.namespace = null;
      this.engineOpts.onRedisError = function(msg) {
        errorArgs = Array.prototype.slice.call(arguments);
      };
      this.engine = new Faye.Engine.Proxy(this.engineOpts);
      var redisEngine = this.engine._engine;

      // Fake the error, since creating a real one is hard.
      redisEngine._redis.smembers = function(key, callback) {
        callback.call(this, new Error("Oops!"), null);
      };

      redisEngine.destroyClient("CLIENT-ID", function() {
        resume(function() {
          assertEqual([new Error("Oops!"), "Failed to fetch channels /clients/CLIENT-ID/channels"], errorArgs);
        });
      }, this);
    }})
  }})
}})
