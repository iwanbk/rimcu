# rimcu - Go Redis Client with In Memory Caching - WIP

[![Build Status](https://travis-ci.org/iwanbk/rimcu.svg?branch=master)](https://travis-ci.org/iwanbk/rimcu)
[![codecov](https://codecov.io/gh/iwanbk/rimcu/branch/master/graph/badge.svg)](https://codecov.io/gh/iwanbk/rimcu)
[![godoc](https://godoc.org/github.com/iwanbk/rimcu?status.svg)](http://godoc.org/github.com/iwanbk/rimcu)

Rimcu is Go Redis client library which also sync and cache the data inside your application memory.

**It is a work in progress, the API and the code can break any time**.

It caches the Redis data in your server's RAM and sync it to Redis server when the data changed.
So you don't need to always ask the Redis server to get your cache data. 


## StringsCache

StringsCache is cache for redis [`strings`](https://redis.io/topics/data-types#strings) data type with RESP3 protocol.
It needs Redis server with RESP3 support which currently only available in Redis 6 (unstable)

## StringsCacheResp2

StringsCacheResp2 is cache for redis [`strings`](https://redis.io/topics/data-types#strings) data type with RESP2 protocol.
It can be used with any Redis versions.
It is under re-designing work.

