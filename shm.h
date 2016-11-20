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

#ifndef __SHM_H
#define __SHM_H

#include <unistd.h> /* ssize_t, etc */
#include <semaphore.h> /* sem_t */

struct redisContext;
struct redisReply;

/* The shared memory file is created with these permissions, by default. */
#define SHARED_MEMORY_DEFAULT_MODE 00700

/* Initializes the shared memory communication. In a non-blocking context,
 * this only partially initializes, and needs to be completed by a call
 * to sharedMemoryInitAfterReply. This call is implicit in a blocking context. */
struct redisReply *sharedMemoryInit(struct redisContext *c, mode_t mode);
void sharedMemoryInitAfterReply(struct redisContext *c, struct redisReply *reply);

/* Formats the command sent by sharedMemoryInit. Only works after a successful
 * call to sharedMemoryInit! */
int sharedMemoryFormatShmOpen(struct redisContext *c, char **cmd);

/* Returns true if the shared memory communication is completely initialized. */
int sharedMemoryIsInitialized(struct redisContext *c);

void sharedMemoryFree(struct redisContext *c);

/* These act as write()/read(), with the same rules and returns and errno. */
ssize_t sharedMemoryWrite(struct redisContext *c, char *buf, size_t btw);
ssize_t sharedMemoryRead(struct redisContext *c, char *buf, size_t btr);


#endif /* __SHM_H */
