API and examples
================

Shared memory achieves low communication latency. Latency has its tradeoffs - to achieve maximum latency, a single thread is working at 100% load, actively reading for changes in memory. So, you must understand this (see more at [redis-module-shm#Performance](https://github.com/edgarsi/redis-module-shm#performance)), and start by calling `redisUseSharedMemory`.

### Synchronous API

```
redisContext *c = redisConnect("127.0.0.1", 6379);
redisReply *reply = redisUseSharedMemory(c);
if (reply != NULL && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1) {
	printf("Using shared memory from now on!\n");
}
// typical hiredis use from now on, nothing special.
```

### Asynchronous API

```
void OnSharedMemoryReply(redisAsyncContext *ac, void *r, void *privdata) {
	redisReply *reply = r;
	if (reply != NULL && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1) {
		printf("Using shared memory from now on!\n");
	}
}

int main (int argc, char **argv) {
	redisAsyncContext *ac = redisAsyncConnect("127.0.0.1", 6379);
	redisLibevAttach(EV_DEFAULT_ ac);
	redisAsyncUseSharedMemory(ac,OnSharedMemoryReply,NULL);
	// typical hiredis use from now on, nothing special.
	ev_loop(EV_DEFAULT_ 0);
	return 0;
}
```

## Full function reference

### Synchronous API

```
/**
 * Try to use shared memory for communicating with the server.
 * 
 * In a blocking context, a successful initialization returns a reply 
 * containing the integer 1. If there was an error in performing
 * the request, may return either 1) a REDIS_REPLY_ERROR reply,
 * or 2) NULL and c->err and c->errstr describing the error.
 * In a non-blocking context, always returns NULL, but the corresponding
 * redisAppendCommand is called. The non-blocking context requires
 * that, at the time of the call, no unprocessed commands exist, and
 * no other appear until you consume the result.
 * 
 * Note that, unlike socket writes/reads, a blocking shared memory communication 
 * can't be aborted by issuing a signal.
 */
redisReply *redisUseSharedMemory(redisContext *c);

/* Use this version of the above function when the default shared memory 
 * file permissions 00700 are insufficient. */
redisReply *redisUseSharedMemoryWithMode(redisContext *c, mode_t mode);

/* If shared memory initialized, returns 1. If not, returns 0.
 * In a non-blocking context, shared memory is only initialized when the 
 * result of the command initiated by redisUseSharedMemory is consumed. */
int redisIsSharedMemoryInitialized(redisContext *c);
```

### Asynchronous API

```
/* Use shared memory. These functions must be called immediately after connect. */
int redisAsyncUseSharedMemory(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata);
int redisAsyncUseSharedMemoryWithMode(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, mode_t mode);

/* Also, see redisIsSharedMemoryInitialized above. */
```