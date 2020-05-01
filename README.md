# rimcu - Redis server-assisted client side caching Go library

[![Build Status](https://travis-ci.org/iwanbk/rimcu.svg?branch=master)](https://travis-ci.org/iwanbk/rimcu)
[![codecov](https://codecov.io/gh/iwanbk/rimcu/branch/master/graph/badge.svg)](https://codecov.io/gh/iwanbk/rimcu)
[![godoc](https://godoc.org/github.com/iwanbk/rimcu?status.svg)](http://godoc.org/github.com/iwanbk/rimcu)
[![Maintainability](https://api.codeclimate.com/v1/badges/edbfa2013d2a8d2b74ce/maintainability)](https://codeclimate.com/github/iwanbk/rimcu/maintainability)

Rimcu is Go library for Redis server-assisted client side caching.
In other words, it is a combination of Redis cient library and in memory cache library.

**It is a work in progress, the API and the code can break any time**.

It caches the Redis data in your server's RAM and sync it to Redis server when the data changed.
So you don't need to always ask the Redis server to get your cache data. 


## StringsCache

[![godoc](https://godoc.org/github.com/iwanbk/rimcu?status.svg)](http://godoc.org/github.com/iwanbk/rimcu#StringsCache)

StringsCache is cache for redis [`strings`](https://redis.io/topics/data-types#strings) data type with RESP3 protocol.

It needs Redis server with RESP3 support which currently only available in Redis 6

### Implemented Commands

- [x] Setex
- [x] Get
- [x] Del
- [x] MSet
- [x] MGet
- [ ] Append

### TODO

improve the connection pool:
- [x] maximum number of connections
- [x] waiting for connection with timeout
- [ ] idle connection checking
- [ ] health checking 


## StringsCache (RESP2)

StringsCache (RESP2) is Redis server-assisted client side caching for redis [`strings`](https://redis.io/topics/data-types#strings) data type with RESP2 protocol.

It syncs the data between client using redis pubsub.

It needs Redis server with RESP3 support which currently only available in Redis 6

### Implemented Commands

- [x] Setex
- [x] Get
- [x] Del
- [ ] MSet
- [ ] MGet


## ListCache (RESP2)

**IT IS UNDER REWORK**

Old implementation can be found at https://github.com/iwanbk/rimcu/blob/v0.01/resp2/listcache.go#L33

```
ListCache is cache for Redis list data type which uses RESP2 protocol. It is still in very early development phase. See the godoc page for more explanation.
 
Implemented commands:
- [x]LPOP
- [x]RPUSH
- [x]GET (it is Rimcu specific command)
- [ ]...
```

# CREDITS

- [redigo](https://github.com/gomodule/redigo) redis package is copied and modified to this repo. It is used to provide RESP2 support.