# rimcu - Go In Memory Cache with Redis Client Side Caching - WIP

[![Build Status](https://travis-ci.org/iwanbk/rimcu.svg?branch=master)](https://travis-ci.org/iwanbk/rimcu)
[![codecov](https://codecov.io/gh/iwanbk/rimcu/branch/master/graph/badge.svg)](https://codecov.io/gh/iwanbk/rimcu)
[![godoc](https://godoc.org/github.com/iwanbk/rimcu?status.svg)](http://godoc.org/github.com/iwanbk/rimcu)

Rimcu is Go in memory cache library which use  Redis to synchronize cache data with other nodes.
In other words, it is a combination of Redis cient library and in memory cache library.

**It is a work in progress, the API and the code can break any time**.

It caches the Redis data in your server's RAM and sync it to Redis server when the data changed.
So you don't need to always ask the Redis server to get your cache data. 


## StringsCache

StringsCache is cache for redis [`strings`](https://redis.io/topics/data-types#strings) data type with RESP3 protocol.

It needs Redis server with RESP3 support which currently only available in Redis 6 (unstable)

### TODO

- improve reliability of the connection pool

## StringsCacheResp2

StringsCacheResp2 is cache for redis [`strings`](https://redis.io/topics/data-types#strings) data type with RESP2 protocol, which means it can be used with any Redis versions.

It syncs the data between client using redis pubsub. It uses Lua script to guarantee the atomicity of set/del the key with publishing the change.

### TODO

- add option to also publish the data instead of only the key
- consider to sync using slot instead of key, similar to how RESP3 doing the sync