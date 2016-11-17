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

#include "lockless-char-fifo/charfifo.h"


#define X(...)
//#define X printf



/* redisBufferRead thinks 16k is best for a temporary buffer reading replies.
 * A good guess is this will do well with shared memory buffer size too. */
#define SHARED_MEMORY_BUF_SIZE (1024*16)

typedef CHARFIFO(SHARED_MEMORY_BUF_SIZE) sharedMemoryBuffer;

typedef volatile struct sharedMemory {
    sharedMemoryBuffer to_server;
    sharedMemoryBuffer to_client;
} sharedMemory;

static void sharedMemoryBufferInit(sharedMemoryBuffer *b) {
    CharFifo_Init(b, SHARED_MEMORY_BUF_SIZE);
}

/*TODO: hiredis conforms to the ancient rules of declaring c variables 
 * at the beginning of functions! */
void sharedMemoryContextInit(redisContext *c) {
    
    c->shm_context = malloc(sizeof(redisSharedMemoryContext));
    if (c->shm_context == NULL) {
        return;
    }
    
    c->shm_context->mem = MAP_FAILED;
    c->shm_context->name[0] = '\0';
    /* Use standard UUID to distinguish among clients. */
    FILE *fp = fopen("/proc/sys/kernel/random/uuid", "r");
    if (fp == NULL) {
        /*TODO: Communicate the error to user. */
        sharedMemoryContextFree(c);
        return;
    }
    size_t btr = sizeof(c->shm_context->name)-2;
    size_t br = fread(c->shm_context->name+1, 1, btr, fp);
    fclose(fp);
    if (br != btr) {
        /*TODO: Communicate the error to user. */
        sharedMemoryContextFree(c);
        return;
    }
    c->shm_context->name[0] = '/';
    c->shm_context->name[sizeof(c->shm_context->name)-1] = '\0';
    
    /* Get that shared memory up and running! */
    shm_unlink(c->shm_context->name);
    int fd = shm_open(c->shm_context->name,(O_RDWR|O_CREAT|O_EXCL),00700); /*TODO: mode needs config, similar to 'unixsocketperm' */
    if (fd < 0) {
        /*TODO: Communicate the error to user. */
        sharedMemoryContextFree(c);
        return;
    }
    if (ftruncate(fd,sizeof(sharedMemory)) != 0) {
        /*TODO: Communicate the error to user. */
        sharedMemoryContextFree(c);
        return;
    }
    c->shm_context->mem = mmap(NULL,sizeof(sharedMemory),
                            (PROT_READ|PROT_WRITE),MAP_SHARED,fd,0);
    if (c->shm_context->mem == MAP_FAILED) {
        /*TODO: Communicate the error to user. */
        sharedMemoryContextFree(c);
        return;
    }
    close(fd);
    
    sharedMemoryBufferInit(&c->shm_context->mem->to_server);
    sharedMemoryBufferInit(&c->shm_context->mem->to_client);
}

void sharedMemoryContextFree(redisContext *c) {
    if (c->shm_context == NULL) {
        return;
    }
    
    if (c->shm_context->mem != MAP_FAILED) {
        munmap(c->shm_context->mem,sizeof(sharedMemory));
    }
    if (c->shm_context->name[0] != '\0') {
        shm_unlink(c->shm_context->name);
    }
    
    free(c->shm_context);
    c->shm_context = NULL;
}

void sharedMemoryAfterConnect(redisContext *c) {
    if (c->shm_context == NULL) {
        return;
    }
    if (c->err) {
        sharedMemoryContextFree(c);
        return;
    }
    /* Temporarily disabling the shm context, so the command does not attempt to
     * be sent through the shared memory. */
    redisSharedMemoryContext *tmp = c->shm_context;
    c->shm_context = NULL;
    int version = 1;
    /*TODO: Allow the user to communicate through user's channels, not require TCP or socket. */
    redisReply *reply = redisCommand(c,"SHM.OPEN %d %s",version,tmp->name);
    c->shm_context = tmp;
    if (reply->type == REDIS_REPLY_INTEGER) {
        if (reply->integer == 1) {
            /* We got ourselves a shared memory! */
        } else {
            /* Module loaded but some error happened. This may be unsupported
             * version, lack of systemwide file descriptors, etc. */
            /*TODO: Communicate the error to user. */
            sharedMemoryContextFree(c);
        }
    } else {
        /*TODO: Communicate the error to user. */
        sharedMemoryContextFree(c);
    }
    freeReplyObject(reply);
    /* Unlink the shared memory file now. This limits the possibility to leak an shm file on crash. */
    shm_unlink(c->shm_context->name);
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
    size_t bw = 0;
    do {
        sharedMemoryBuffer *target = &c->shm_context->mem->to_server;
        size_t free = CharFifo_FreeSpace(target);
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
            CharFifo_Write(target,buf+bw,btw_chunk);
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
        sharedMemoryBuffer *source = &c->shm_context->mem->to_client;
        size_t used = CharFifo_UsedSpace(source);
//        printf("flags: %d btr: %d used: %d\n", c->flags, btr, used);
        err = EAGAIN;
        if (used > 0) {
            err = 0; /* btr is optimistic. I need to return with whatever I get. */
            if (btr > used) {
                btr = used;
            }
            CharFifo_Read(source,buf,btr);
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
