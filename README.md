# rimcu - Redis server-assisted client side caching Go library

![Test and Linter](https://github.com/github/docs/actions/workflows/test_lint.yml/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/iwanbk/rimcu)](https://pkg.go.dev/github.com/iwanbk/rimcu)
[![codecov](https://codecov.io/gh/iwanbk/rimcu/branch/master/graph/badge.svg)](https://codecov.io/gh/iwanbk/rimcu)
[![Maintainability](https://api.codeclimate.com/v1/badges/edbfa2013d2a8d2b74ce/maintainability)](https://codeclimate.com/github/iwanbk/rimcu/maintainability)

Rimcu is Go library for Redis server-assisted client side caching.
In other words, it is a combination of Redis cient library and in memory cache library.

## System Requirements

Redis 6, with it's client side caching feature

## How it works

It caches the Redis data in your server's RAM and sync it to Redis server when the data changed.
So you don't need to always ask the Redis server to get your cache data.

It supports two kind of Redis protocols:
- RESP2: it is the default one
- RESP3: still under development

## Caches
We categorize the cache based on the Redis data types mention in https://redis.io/docs/manual/data-types/.
We need to do this because each type of cache will be stored differently.


### StringsCache

StringsCache is cache for redis [`strings`](https://redis.io/topics/data-types#strings) data type.

### Implemented Commands

- [x] Setex
- [x] Get
- [x] Del
- [ ] MSet (waiting support at RESP2)
- [ ] MGet (waiting support at RESP2)
- [ ] Append

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

## TODO

improve the connection pool:
- [x] maximum number of connections
- [x] waiting for connection with timeout
- [ ] idle connection checking
- [ ] health checking


# CREDITS

- [redigo](https://github.com/gomodule/redigo) redis package is copied and modified to this repo. It is used to provide RESP2 support.