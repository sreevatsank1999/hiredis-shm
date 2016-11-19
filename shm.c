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
#include <limits.h>
#include <sys/socket.h>

#include "shm.h"
#include "hiredis.h"

#include "lockless-char-fifo/charfifo.h"

void __redisSetError(redisContext *c, int type, const char *str);


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

typedef struct redisSharedMemoryContext {
    char name[38]; /* Shared memory file name. */
    struct sharedMemory *mem;
} redisSharedMemoryContext;


static void sharedMemoryBufferInit(sharedMemoryBuffer *b) {
    CharFifo_Init(b, SHARED_MEMORY_BUF_SIZE);
}

/*TODO: hiredis conforms to the ancient rules of declaring c variables 
 * at the beginning of functions! */
static int sharedMemoryContextInit(redisContext *c) {
    
    c->shm_context = malloc(sizeof(redisSharedMemoryContext));
    if (c->shm_context == NULL) {
        __redisSetError(c,REDIS_ERR_OOM,"Out of memory");
        return 0;
    }
    
    c->shm_context->mem = MAP_FAILED;
    c->shm_context->name[0] = '\0';
    /* Use standard UUID to distinguish among clients. */
    FILE *fp = fopen("/proc/sys/kernel/random/uuid", "r");
    if (fp == NULL) {
        sharedMemoryFree(c);
        __redisSetError(c,REDIS_ERR_OTHER,
                "Can't read /proc/sys/kernel/random/uuid");
        return 0;
    }
    size_t btr = sizeof(c->shm_context->name)-2;
    size_t br = fread(c->shm_context->name+1, 1, btr, fp);
    fclose(fp);
    if (br != btr) {
        sharedMemoryFree(c);
        __redisSetError(c,REDIS_ERR_OTHER,
                "Incomplete read of /proc/sys/kernel/random/uuid");
        return 0;
    }
    c->shm_context->name[0] = '/';
    c->shm_context->name[sizeof(c->shm_context->name)-1] = '\0';
    
    /* Get that shared memory up and running! */
    shm_unlink(c->shm_context->name);
    int fd = shm_open(c->shm_context->name,(O_RDWR|O_CREAT|O_EXCL),00700); /*TODO: mode needs config, similar to 'unixsocketperm' */
    if (fd < 0) {
        sharedMemoryFree(c);
        __redisSetError(c,REDIS_ERR_OTHER,
                        "Can't create shared memory file");
        return 0;
    }
    if (ftruncate(fd,sizeof(sharedMemory)) != 0) {
        sharedMemoryFree(c);
        __redisSetError(c,REDIS_ERR_OOM,"Out of shared memory");
        return 0;
    }
    c->shm_context->mem = mmap(NULL,sizeof(sharedMemory),
                            (PROT_READ|PROT_WRITE),MAP_SHARED,fd,0);
    if (c->shm_context->mem == MAP_FAILED) {
        sharedMemoryFree(c);
        __redisSetError(c,REDIS_ERR_OTHER,
                        "Can't mmap the shared memory file");
        return 0;
    }
    close(fd);
    
    sharedMemoryBufferInit(&c->shm_context->mem->to_server);
    sharedMemoryBufferInit(&c->shm_context->mem->to_client);
    
    return 1;
}

static void sharedMemoryProcessShmOpenReply(redisContext *c, redisReply *reply)
{
    /* Unlink the shared memory file now. This limits the possibility to leak 
     * an shm file on crash. */
    shm_unlink(c->shm_context->name);
    c->shm_context->name[0] = '\0';

    if (reply != NULL && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1) {
        /* We got ourselves a shared memory! */
    } else {
        sharedMemoryFree(c);
    }
}

static redisReply *sharedMemoryEstablishCommunication(redisContext *c) {
    
    int version = 1;
    
    /* Temporarily disabling the shm context, so the command does not attempt to
     * be sent through the shared memory. */
    redisSharedMemoryContext *tmp = c->shm_context;
    c->shm_context = NULL;
    /*TODO: Allow the user to communicate through user's channels, not require TCP or socket. */
    redisReply *reply = redisCommand(c,"SHM.OPEN %d %s",version,tmp->name);
    c->shm_context = tmp;

    if (c->flags & REDIS_BLOCK) {
        sharedMemoryProcessShmOpenReply(c, reply);
    }

    return reply;
}

redisReply *sharedMemoryInit(redisContext *c) {
    int ok = sharedMemoryContextInit(c);
    if (!ok) {
        return NULL;
    }
    return sharedMemoryEstablishCommunication(c);
}

void sharedMemoryInitAfterReply(struct redisContext *c, redisReply *reply)
{
    if (!(c->flags & REDIS_BLOCK) && c->shm_context != NULL
            && c->shm_context->name[0] != '\0') {
        /* A non-blocking context has received the acknowledgement
         * that the shared memory communication was successful or failed. */
        sharedMemoryProcessShmOpenReply(c, reply);
    }
}

void sharedMemoryFree(redisContext *c) {
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

static int fdSetBlocking(int fd, int blocking) {
    int flags;

    if ((flags = fcntl(fd, F_GETFL)) == -1) {
        return 0;
    }

    if (blocking)
        flags &= ~O_NONBLOCK;
    else
        flags |= O_NONBLOCK;

    if (fcntl(fd, F_SETFL, flags) == -1) {
        return 0;
    }
    return 1;
}


static int isConnectionBroken(redisContext *c, size_t iteration)
{
    /* select() is relatively slow, and even gettimeofday() is. Just skip iterations 
     * on count, delaying the recognition of broken connections, but keeping normal
     * latency good. On my reference computer, an iteration takes ~5ns. */
    if (iteration == 0 || iteration % 10000 != 0) {
        return 0;
    }
    
    /* Checking for connection failure with select(). */
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(c->fd, &rfds);
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    int selret = select(c->fd + 1, &rfds, NULL, NULL, &tv);
    if (selret == 0 || (selret == -1 && errno == EINTR)) {
        return 0;
    }
    if (selret == -1) {
        return 1; /* Even at ENOMEM, it's safest to drop the connection than not know
                     if the connection is failed, blocking indefinitely. */ 
    }
    
    /* Read under O_NONBLOCK. Man pages warn of oddities which could cause blocking. 
     * This is only needed to read the very likely EOF, so not a load on performance. */
    if (c->flags & REDIS_BLOCK) {
        fdSetBlocking(c->fd, 0);
    }
    char tmp;
    ssize_t readret = read(c->fd, &tmp, 1);
    if (c->flags & REDIS_BLOCK) {
        fdSetBlocking(c->fd, 1);
    }
    
    /* Check for EOF and unexpected behaviour. */
    return (readret >= 0 || (readret == -1 && errno != EAGAIN && errno != EINTR)); 
}

/* PIPE_BUF is usually 4k, but there are no guarantees, therefore I'm being 
 * slightly paranoid. Attempting to comply with POSIX atomic writes needs this.
 * I don't really need those atomic writes because hiredis uses a single writer,
 * but pretty code and stuff. */ 
#if PIPE_BUF > SHARED_MEMORY_BUF_SIZE
#error "PIPE_BUF > SHARED_MEMORY_BUF_SIZE"
#endif

ssize_t sharedMemoryWrite(redisContext *c, char *buf, size_t btw) {
    size_t iteration = 0;
    int btw_chunk;
    size_t bw = 0;
    int conn_broken = 0;
    sharedMemoryBuffer *target = &c->shm_context->mem->to_server;
    do {
        conn_broken = isConnectionBroken(c, iteration++);
        if (conn_broken) {
            break;
        }
        size_t free = CharFifo_FreeSpace(target);
        if (btw <= PIPE_BUF && free < btw) { /* POSIX atomic write incomplete? */
            if (c->flags & REDIS_BLOCK) {
                continue;
            } else {
                break;
            }
        }
        if (free > 0) {
            btw_chunk = (free < btw-bw ? free : btw-bw);
            CharFifo_Write(target,buf+bw,btw_chunk);
            bw += btw_chunk;
        }
        /* This hogs up CPU when no free space and REDIS_BLOCK is on, but 
         * latency is best if done this way, and the server will likely
         * free some space soon. */ 
    } while (bw < btw && (c->flags & REDIS_BLOCK));
    
    if (bw != 0 || btw == 0) {
        /* Return written bytes even if conn_broken, as write() would due to SIGPIPE. */
        return bw;
    } else {
        if (conn_broken) {
            errno = EPIPE;
        } else {
            errno = EAGAIN;
        }
        return -1;
    }
}

ssize_t sharedMemoryRead(redisContext *c, char *buf, size_t btr) {
    size_t iteration = 0;
    size_t br;
    int conn_broken = 0;
    sharedMemoryBuffer *source = &c->shm_context->mem->to_client;
    do {
        conn_broken = isConnectionBroken(c, iteration++);
        if (conn_broken) {
            break;
        }
        size_t used = CharFifo_UsedSpace(source);
        if (used > 0) {
            br = (used < btr ? used : btr);
            CharFifo_Read(source,buf,br);
        }
        /* This hogs up CPU when no free space and REDIS_BLOCK is on, but 
         * latency is best if done this way, and the server will likely
         * send a reply soon. */ 
    } while (br == 0 && (c->flags & REDIS_BLOCK));
    if (br != 0) {
        return br;
    } else {
        if (conn_broken) {
            return 0;
        } else {
            errno = EAGAIN;
            return -1;
        }
    }
}
