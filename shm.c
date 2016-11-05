/* Shared memory support for hiredis.
 * 
 * Copyright (c) 2009-2011, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2010-2014, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2015, Matt Stancliff <matt at genges dot com>,
 *                     Jan-Erik Rediger <janerik at fnordig dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <sys/mman.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include "shm.h"
#include "hiredis.h"



#define X(...)
//#define X printf



/*TODO: Use reason when choosing the size!*/
#define SHARED_MEMORY_BUF_SIZE (1024-8-8-8) /* ...the last should be 1, for buffer always leaves a byte unused. i just prefer sizes divide by 8. */

typedef struct sharedMemoryBuffer {
    char buf[SHARED_MEMORY_BUF_SIZE];
    size_t read_idx;
    size_t write_idx;
} sharedMemoryBuffer;

typedef struct sharedMemory {
    sharedMemoryBuffer to_server;
    sharedMemoryBuffer to_client;
} sharedMemory;

void sharedMemoryBufferInit(sharedMemoryBuffer *b) {
    b->read_idx = 0;
    b->write_idx = 0;
}

/*TODO: hiredis conforms to the ancient rules of declaring c variables 
 * at the beginning of functions! */
void sharedMemoryContextInit(sharedMemoryContext *shm_context) {
    shm_context->in_use = 0;
    shm_context->fd = -1;
    shm_context->mem = MAP_FAILED;
//    return;
    /* Use standard UUID to distinguish among clients. */ //TODO 
    memset(shm_context->name, 'a', sizeof(shm_context->name));
    shm_context->name[0] = '/';
    shm_context->name[sizeof(shm_context->name)-1] = '\0';
//    /proc/sys/kernel/random/uuid
    /* Create the semaphores. */
    /* Does the server see them? */ //TODO
    /* Get that shared memory up and running! */
    shm_unlink(shm_context->name);
    shm_context->fd = shm_open(shm_context->name,(O_RDWR|O_CREAT|O_EXCL),00700); /*TODO: mode needs config, similar to 'unixsocketperm' */
    if (shm_context->fd < 0) {
        /*TODO: Communicate the error to user. */
        sharedMemoryContextFree(shm_context);
        return;
    }
    if (ftruncate(shm_context->fd,sizeof(sharedMemory)) != 0) {
        /*TODO: Communicate the error to user. */
        sharedMemoryContextFree(shm_context);
        return;
    }
    shm_context->mem = mmap(NULL,sizeof(sharedMemory),
                            (PROT_READ|PROT_WRITE),MAP_SHARED,shm_context->fd,0);
    if (shm_context->mem == MAP_FAILED) {
        /*TODO: Communicate the error to user. */
        sharedMemoryContextFree(shm_context);
        return;
    }
    sharedMemoryBufferInit(&shm_context->mem->to_server);
    sharedMemoryBufferInit(&shm_context->mem->to_client);
    shm_context->in_use = 1;
}

void sharedMemoryContextFree(sharedMemoryContext *shm_context) {
    if (shm_context->mem != MAP_FAILED) {
        munmap(shm_context->mem,sizeof(sharedMemory)); /*TODO: What if failed?*/
    }
    if (shm_context->fd != -1) {
        close(shm_context->fd); /*TODO: What if failed?*/
        shm_unlink(shm_context->name); /*TODO: What if failed?*/
    }
    shm_context->in_use = 0;
}

void sharedMemoryAfterConnect(redisContext *c) {
    if (!c->shm_context.in_use) {
        return;
    }
    if (c->err) {
        sharedMemoryContextFree(&c->shm_context);
        return;
    }
    int version = 1;
    c->shm_context.in_use = 0;
    /*TODO: Allow the user to communicate through user's channels, not require TCP or socket. */
    redisReply *reply = redisCommand(c,"SHM.OPEN %d %s",version,c->shm_context.name);
    c->shm_context.in_use = 1;
    if (reply->type == REDIS_REPLY_INTEGER) {
        if (reply->integer == 1) {
            /* We got ourselves a shared memory! */
        } else {
            /* Module loaded but some error happened. This may be unsupported
             * version, lack of systemwide file descriptors, etc. */
            /*TODO: Communicate the error to user. */
            sharedMemoryContextFree(&c->shm_context);
        }
    } else {
        /*TODO: Communicate the error to user. */
        sharedMemoryContextFree(&c->shm_context);
    }
    freeReplyObject(reply);
}

static size_t bufFreeSpace(sharedMemoryBuffer *target) {
    size_t read_idx = target->read_idx;
    ssize_t free = (ssize_t)read_idx - target->write_idx - 1;
    if (read_idx <= target->write_idx) {
        free = free + sizeof(target->buf);
    }
    return free;
}

static void bufWrite(sharedMemoryBuffer *target, char *buf, size_t btw) {
    X("bufWrite(read_ptr=%d write_ptr=%d btw=%d\n", target->read_idx, target->write_idx, btw);
    size_t write_idx = target->write_idx;
    /* TODO: Need an atomic read and write for 32bit systems. See if atomicvar.h in the redis/src can help. */
    __sync_synchronize();
    if (write_idx >= target->read_idx) {
        size_t bytes_to_eob = sizeof(target->buf) - write_idx;
        if (bytes_to_eob <= btw) {
            /* Write needs to rotate target->write_idx to the buffer beginning */
            memcpy(target->buf + write_idx, buf, bytes_to_eob);
            write_idx = 0;
            btw -= bytes_to_eob;
            buf += bytes_to_eob;
        }
    }
    memcpy(target->buf + write_idx, buf, btw);
    write_idx += btw;
    __sync_synchronize();
    target->write_idx = write_idx;
    X("bufWrite fin (read_ptr=%d write_ptr=%d btw=%d\n", target->read_idx, target->write_idx, btw);
}

/* TODO: Less duplication, maybe? */
static size_t bufUsedSpace(sharedMemoryBuffer *source) {
    size_t write_idx = source->write_idx;
    ssize_t used = write_idx - source->read_idx;
    if (write_idx < source->read_idx) {
        used = used + sizeof(source->buf);
    }
    return used;
}

/* TODO: Less duplication, maybe? */
static void bufRead(sharedMemoryBuffer *source, char *buf, size_t btr) {
    X("bufRead(read_ptr=%d write_ptr=%d btr=%d\n", source->read_idx, source->write_idx, btr);
    size_t read_idx = source->read_idx;
    __sync_synchronize();
    if (read_idx > source->write_idx) {
        size_t bytes_to_eob = sizeof(source->buf) - read_idx;
        if (bytes_to_eob <= btr) {
            /* Read needs to rotate source->read_idx to the buffer beginning */
            memcpy(buf, source->buf + read_idx, bytes_to_eob);
            read_idx = 0;
            btr -= bytes_to_eob;
            buf += bytes_to_eob;
        }
    }
    memcpy(buf, source->buf + read_idx, btr);
    read_idx += btr;
    __sync_synchronize();
    source->read_idx = read_idx;
    X("bufRead fin (read_ptr=%d write_ptr=%d btr=%d\n", source->read_idx, source->write_idx, btr);
}

/* Return the UNIX time in microseconds */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

int sharedMemoryWrite(redisContext *c, char *buf, size_t btw) {
    X("%lld write \n", ustime());
    int err;
    int btw_chunk;
    int bw = 0;
    do {
        sharedMemoryBuffer *target = &c->shm_context.mem->to_server;
        size_t free = bufFreeSpace(target);
        err = EAGAIN;
        if (free > 0) {
            if (btw - bw > free) {
                btw_chunk = free;
                /* Blocking is better for latency when part written, so EAGAIN. */
                /* TODO: Shouldn't I allow for the caller process to abort by issuing a signal? */
            } else {
                btw_chunk = btw - bw;
                err = 0;
            }
            bufWrite(target,buf+bw,btw_chunk);
            bw += btw_chunk;
        }
        /*TODO: Don't hog up CPU when no free space and REDIS_BLOCK is on.*/ 
    } while (bw < btw && err == EAGAIN && (c->flags & REDIS_BLOCK));
    if (err == 0) {
        return btw;
    } else {
        return -err; /* see .h */
    }
}

/* TODO: Less duplication, maybe? */
ssize_t sharedMemoryRead(redisContext *c, char *buf, size_t btr) {
    X("%lld read start \n", ustime());
    int err;
    do {
        sharedMemoryBuffer *source = &c->shm_context.mem->to_client;
        size_t used = bufUsedSpace(source);
//        printf("flags: %d btr: %d used: %d\n", c->flags, btr, used);
        err = EAGAIN;
        if (used > 0) {
            err = 0; /* btr is optimistic. I need to return with whatever I get. */
            if (btr > used) {
                btr = used;
            }
            bufRead(source,buf,btr);
        }
        /*TODO: Don't hog up CPU when no free space and REDIS_BLOCK is on.*/ 
    } while (err == EAGAIN && (c->flags & REDIS_BLOCK));
    X("%lld read end \n", ustime());
    if (err == 0) {
        return btr;
    } else {
        return -err; /* see .h */
    }
}
