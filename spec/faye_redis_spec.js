var RedisEngine = require('../faye-redis')

JS.ENV.FayeRedisSpec = JS.Test.describe("Redis engine", function() { with(this) {
  before(function() {
    var pw = process.env.TRAVIS ? undefined : "foobared"
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
}})
