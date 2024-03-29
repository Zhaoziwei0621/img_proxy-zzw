#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include <semaphore.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <errno.h>

#include "image-remote-pvt.h"
#include "lz4.h"
//#include "criu-log.h"

typedef struct wthread {
    pthread_t tid;
    struct list_head l;
} worker_thread;

static LIST_HEAD(rimg_head);
static pthread_mutex_t rimg_lock;
static sem_t rimg_semph;

static LIST_HEAD(workers_head);
static pthread_mutex_t workers_lock;
static sem_t workers_semph;

static int finished = 0;
static int putting = 0;

static void* (*get_func)(void*);
static void* (*put_func)(void*);

static remote_image* get_rimg_by_name(const char* namespace, const char* path)
{
        remote_image* rimg = NULL;
        pthread_mutex_lock(&rimg_lock);
        list_for_each_entry(rimg, &rimg_head, l) {
                if( !strncmp(rimg->path, path, PATHLEN) &&
                    !strncmp(rimg->namespace, namespace, PATHLEN)) {
                        pthread_mutex_unlock(&rimg_lock);
                        return rimg;
                }
        }
        pthread_mutex_unlock(&rimg_lock);
        return NULL;
}

int init_sync_structures()
{
        if (pthread_mutex_init(&rimg_lock, NULL) != 0) {
                perror("Remote image connection mutex init failed");
                return -1;
        }

        if (sem_init(&rimg_semph, 0, 0) != 0) {
                perror("Remote image connection semaphore init failed");
                return -1;
        }

        if (pthread_mutex_init(&workers_lock, NULL) != 0) {
                perror("Workers mutex init failed");
                return -1;
        }

        if (sem_init(&workers_semph, 0, 0) != 0) {
                perror("Workers semaphore init failed");
                return -1;
        }
        return 0;
}

void* get_remote_image(void* fd)
{
        int cli_fd = (long) fd;
        remote_image* rimg = NULL;
        char path_buf[PATHLEN];
        char namespace_buf[PATHLEN];

        if(read_header(cli_fd, namespace_buf, path_buf) < 0) {
                perror("Error reading header");
                return NULL;
        }

        printf("Received GET for %s:%s.\n", path_buf, namespace_buf);

        rimg = wait_for_image(cli_fd, namespace_buf, path_buf);
        if (!rimg)
                return NULL;

        rimg->dst_fd = cli_fd;
        send_remote_image(rimg->dst_fd, rimg->path, &rimg->buf_head);
        return NULL;
}

void prepare_put_rimg()
{
        pthread_mutex_lock(&rimg_lock);
        putting++;
        pthread_mutex_unlock(&rimg_lock);
}

void finalize_put_rimg(remote_image* rimg)
{
        pthread_mutex_lock(&rimg_lock);
        list_add_tail(&(rimg->l), &rimg_head);
        putting--;
        pthread_mutex_unlock(&rimg_lock);
        sem_post(&rimg_semph);
}

int init_proxy()
{
        get_func = get_remote_image;
        put_func = proxy_remote_image;
        return init_sync_structures();
}

/*
int init_cache()
{
        get_func = get_remote_image;
        put_func = cache_remote_image;
        return init_sync_structures();
}
*/

int prepare_server_socket(int port)
{
        struct sockaddr_in serv_addr;
        int sockopt = 1;

        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
                perror("Unable to open image socket");
                return -1;
        }

        bzero((char *) &serv_addr, sizeof (serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(port);

        if (setsockopt(
            sockfd, SOL_SOCKET, SO_REUSEADDR, &sockopt, sizeof (sockopt)) == -1) {
                perror("Unable to set SO_REUSEADDR");
                return -1;
        }

        if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof (serv_addr)) < 0) {
                perror("Unable to bind image socket");
                return -1;
        }

        if (listen(sockfd, DEFAULT_LISTEN)) {
                perror("Unable to listen image socket");
                return -1;
        }

        return sockfd;
}

int prepare_client_socket(char* hostname, int port)
{
        struct hostent *server;
        struct sockaddr_in serv_addr;

        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
                perror("Unable to open recover image socket");
                return -1;
        }

        server = gethostbyname(hostname);
        if (server == NULL) {
                printf("Unable to get host by name (%s)", hostname);
                return -1;
        }

        bzero((char *) &serv_addr, sizeof (serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *) server->h_addr,
              (char *) &serv_addr.sin_addr.s_addr,
              server->h_length);
        serv_addr.sin_port = htons(port);

        if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
                printf("Unable to connect to remote restore host %s", hostname);
                return -1;
        }

        return sockfd;
}

static void add_worker(pthread_t tid)
{
        worker_thread* wthread = malloc(sizeof(worker_thread));
        if(!wthread) {
                perror("Unable to allocate worker thread structure");
        }
        wthread->tid = tid;
        pthread_mutex_lock(&workers_lock);
        list_add_tail(&(wthread->l), &workers_head);
        pthread_mutex_unlock(&workers_lock);
        sem_post(&workers_semph);
}

void join_workers()
{
        worker_thread* wthread = NULL;
        while(1) {
            if(list_empty(&workers_head)) {
                    sem_wait(&workers_semph);
                    continue;
            }
            wthread = list_entry(workers_head.next, worker_thread, l);
            if(pthread_join(wthread->tid, NULL)) {
                    printf("Could not join thread %lu", (unsigned long) wthread->tid);
            }
            else {
                    //pr_info("Joined thread %lu\n", (unsigned long) wthread->tid);
                    list_del(&(wthread->l));
                    free(wthread);
            }

        }
}

remote_image* wait_for_image(int cli_fd, char* namespace, char* path)
{
        remote_image *result;

        while (1) {
                result = get_rimg_by_name(namespace, path);
                // The file exists
                if(result != NULL) {
                        if(write_header(cli_fd, namespace, path) < 0) {
                                printf("Error writing header for %s:%s",
                                        path, namespace);
                                close(cli_fd);
                                return NULL;
                        }
                        return result;
                }
                // The file does not exist and we do not expect new files
                if(finished && !putting) {
                        if(write_header(cli_fd, NULL_NAMESPACE, DUMP_FINISH) < 0) {
                                printf("Error writing header for %s:%s",
                                        DUMP_FINISH, NULL_NAMESPACE);
                        }
                        close(cli_fd);
                        return NULL;
                }
                // The file does not exist but the request is for a parent file.
                // A parent file may not exist for the first process.
                if(!putting && !strncmp(path, PARENT_IMG, PATHLEN)) {
                    if(write_header(cli_fd, namespace, path) < 0) {
                            printf("Error writing header for %s:%s",
                                        path, namespace);
                    }
                    close(cli_fd);
                    return NULL;
                }
                sem_wait(&rimg_semph);
        }
}

void* accept_get_image_connections(void* port)
{
        socklen_t clilen;
        long cli_fd;
        pthread_t tid;
        int get_fd = *((int*) port);
        struct sockaddr_in cli_addr;
        clilen = sizeof (cli_addr);

        while (1) {

                cli_fd = accept(get_fd, (struct sockaddr *) &cli_addr, &clilen);
                if (cli_fd < 0) {
			perror("Unable to accept get image connection");
                        return NULL;
                }

                if (pthread_create(
                    &tid, NULL, get_func, (void*) cli_fd)) {
                        perror("Unable to create put thread");
                        return NULL;
                }

                add_worker(tid);
        }
}

void* accept_put_image_connections(void* port)
{
        socklen_t clilen;
        int cli_fd;
        pthread_t tid;
        int put_fd = *((int*) port);
        struct sockaddr_in cli_addr;
        clilen = sizeof(cli_addr);
        char path_buf[PATHLEN];
        char namespace_buf[PATHLEN];

        while (1) {

                cli_fd = accept(put_fd, (struct sockaddr *) &cli_addr, &clilen);
                if (cli_fd < 0) {
                        perror("Unable to accept put image connection");
                        return NULL;
                }

                if(read_header(cli_fd, namespace_buf, path_buf) < 0) {
                    perror("Error reading header");
                    continue;
                }

                remote_image* rimg = get_rimg_by_name(namespace_buf, path_buf);

                printf("Reveiced PUT request for %s:%s\n", path_buf, namespace_buf);

                if(rimg == NULL) {
                        rimg = malloc(sizeof (remote_image));
                        if (rimg == NULL) {
                                perror("Unable to allocate remote_image structures");
                                return NULL;
                        }

                        remote_buffer* buf = malloc(sizeof (remote_buffer));
                        if(buf == NULL) {
                                perror("Unable to allocate remote_buffer structures");
                                return NULL;
                        }

                        strncpy(rimg->path, path_buf, PATHLEN);
                        strncpy(rimg->namespace, namespace_buf, PATHLEN);
                        buf->nbytes = 0;
                        INIT_LIST_HEAD(&(rimg->buf_head));
                        list_add_tail(&(buf->l), &(rimg->buf_head));
                }
                // NOTE: we implement a PUT by clearing the previous file.
                else {
                    printf("Clearing previous images for %s:%s\n",
                            path_buf, namespace_buf);
                        pthread_mutex_lock(&rimg_lock);
                        list_del(&(rimg->l));
                        pthread_mutex_unlock(&rimg_lock);
                        while(!list_is_singular(&(rimg->buf_head))) {
                                list_del(rimg->buf_head.prev);
                        }
                        list_entry(rimg->buf_head.next, remote_buffer, l)->nbytes = 0;
                }
                rimg->src_fd = cli_fd;
                rimg->dst_fd = -1;

                if (pthread_create(
                    &tid, NULL, put_func, (void*) rimg)) {
                        perror("Unable to create put thread");
                        return NULL;
                }

                printf("Serving PUT request for %s:%s (tid=%lu)\n",
                        rimg->path, rimg->namespace, (unsigned long) tid);

                add_worker(tid);

                if (!strncmp(path_buf, DUMP_FINISH, sizeof (DUMP_FINISH))) {
                        finished = 1;
                        printf("Received DUMP FINISH\n");
                        sem_post(&rimg_semph);
                }
        }
}

int recv_remote_image(int fd, char* path, struct list_head* rbuff_head)
{
        remote_buffer* curr_buf = list_entry(rbuff_head->next, remote_buffer, l);
        int n, nblocks;

        nblocks = 0;
        while(1) {
                n = read(fd,
                         curr_buf->buffer + curr_buf->nbytes,
                         BUF_SIZE - curr_buf->nbytes);
                if (n == 0) {
                        printf("Finished receiving %s (%d full blocks, %d bytes on last block)\n",
                                path, nblocks, curr_buf->nbytes);
						if (curr_buf->nbytes == 0) {
							remote_buffer* prev_buf = list_entry(curr_buf->l.prev, remote_buffer, l);
							prev_buf->is_end = 1;
						}
						else
							curr_buf->is_end = 1;
                        close(fd);
                        return nblocks*BUF_SIZE + curr_buf->nbytes;
                }
                else if (n > 0) {
                        curr_buf->nbytes += n;
                        if(curr_buf->nbytes == BUF_SIZE) {
                                remote_buffer* buf = malloc(sizeof(remote_buffer));
                                if(buf == NULL) {
                                        perror("Unable to allocate remote_buffer structures");
                                        return -1;
                                }
                                buf->nbytes = 0;
								buf->is_end = 0;
                                list_add_tail(&(buf->l), rbuff_head);
                                curr_buf = buf;
                                nblocks++;
                        }
                }
                else {
                        printf("Read on %s socket failed", path);
                        return -1;
                }
        }
}

size_t send_remote_obj(int fd, char* buff, size_t size) 
{
	size_t n = 0;
	size_t curr = 0;
	while(1) {
		n = send(fd, buff + curr, size - curr, MSG_NOSIGNAL);
		if( n < 1) {
			return n;
		}
		
		curr += n;
		
		if(curr == size) {
			return size;
		}
	}
}

int send_remote_image(int fd, char* path, struct list_head* rbuff_head)
{
        remote_buffer* curr_buf = list_entry(rbuff_head->next, remote_buffer, l);
        int nblocks;

		nblocks = 0;
		
        while(1) {
				// msg head
				struct msgInfo msg;
				msg.nbytes = curr_buf->nbytes;
				msg.cbytes = 0;
				msg.is_compressed = false;
				// means msg will end
				msg.is_end = curr_buf->is_end;

				char* buf = malloc(sizeof(msgInfo));
				memcpy(buf, &msg, sizeof(msgInfo));
				if (send_remote_obj(fd, buf, sizeof(msgInfo)) != sizeof(msgInfo)) {
                    	printf("Write on %s msgInfo failed\n", path);
						return -1;
				}

				if (send_remote_obj(fd, curr_buf->buffer, msg.nbytes) == msg.nbytes) {
						if (msg.is_end) {
								printf("Finished forwarding %s (%d full blocks, %d bytes on last block) total %d\n",
										path, nblocks, curr_buf->nbytes, nblocks*BUF_SIZE + curr_buf->nbytes);
                                		close(fd);
                                		return nblocks*BUF_SIZE + curr_buf->nbytes;
						}
                        nblocks++;
						//if (!strncmp(path, "pages-", 6))
						//		printf("we forwarding %s (%d full blocks, %d bytes on last block) total %d\n",
						//				path, nblocks, curr_buf->nbytes, nblocks*BUF_SIZE);
				} else {
                    	printf("Write on %s msgData failed\n", path);
						return -1;
				}
               	curr_buf = list_entry(curr_buf->l.next, remote_buffer, l);
        }
}

/*
int send_remote_image(int fd, char* path, struct list_head* rbuff_head)
{
        remote_buffer* curr_buf = list_entry(rbuff_head->next, remote_buffer, l);
        int n, curr_offset, memory_len;

        curr_offset = 0;
		
		// first malloc 20MB memory to store transferred pages
		memory_len = 0;
		int max_memory_len = 3 * 1024 * 1024;
		char* memory_buf = (char *)malloc(max_memory_len * sizeof(char));
        while(curr_buf->nbytes) {
				
				memcpy(memory_buf + memory_len, curr_buf->buffer, curr_buf->nbytes);

				memory_len += curr_buf->nbytes;

				if (curr_buf->nbytes < BUF_SIZE) 
					break;
					
				curr_buf = list_entry(curr_buf->l.next, remote_buffer, l);

		}
		
		// second malloc 20MB memory to store compressed transfer pages
		char* lz4_buf = (char *)malloc(max_memory_len * sizeof(char));
		int max_lz4_nbytes = LZ4_compressBound(memory_len);
		
		const int lz4_nbytes = LZ4_compress_default(memory_buf, lz4_buf, memory_len, max_lz4_nbytes);
		
		// transfer
		while (curr_offset < lz4_nbytes) {
				n = send(
               	    	fd,
                    	lz4_buf + curr_offset,
                    	lz4_nbytes - curr_offset,
                    	MSG_NOSIGNAL);
                if(n > -1) {
                		curr_offset += n;
                    	if(curr_offset == lz4_nbytes) {
                        		printf("Finished forwarding %s (Before compress %d MB transfered, After compress %d MB transfered)\n",
                            		path, memory_len/(1024*1024), lz4_nbytes/(1024*1024));
                                close(fd);
								free(memory_buf);
								free(lz4_buf);
								return lz4_nbytes;
                        }
               	}
                else if(errno == EPIPE || errno == ECONNRESET) {
                        	printf("Connection for %s was closed early than expected\n",
                                path);
                        	return 0;
                }
                else {
                    		printf("Write on %s socket failed", path);
                        	return -1;
                }
		}
		
		close(fd);
		free(memory_buf);
		free(lz4_buf);
		return lz4_nbytes;
		//
        while(1) {
                n = send(
                    fd,
                    curr_buf->buffer + curr_offset,
                    MIN(BUF_SIZE, curr_buf->nbytes) - curr_offset,
                    MSG_NOSIGNAL);
                if(n > -1) {
                        curr_offset += n;
                        if(curr_offset == BUF_SIZE) {
                                curr_buf =
                                    list_entry(curr_buf->l.next, remote_buffer, l);
                                nblocks++;
                                curr_offset = 0;
                        }
                        else if(curr_offset == curr_buf->nbytes) {
                                printf("Finished forwarding %s (%d full blocks, %d bytes on last block)\n",
                                        path, nblocks, curr_offset);
                                close(fd);
                               return nblocks*BUF_SIZE + curr_buf->nbytes;
                        }
                }
                else if(errno == EPIPE || errno == ECONNRESET) {
                        printf("Connection for %s was closed early than expected\n",
                                path);
                        return 0;
                }
                else {
                        printf("Write on %s socket failed", path);
                        return -1;
                }
        }

}*/
