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

static LIST_HEAD(rimg_head); // 生成双向循环列表结构的空的头结点
static pthread_mutex_t rimg_lock; // 线程互斥锁
static sem_t rimg_semph; // 信号量

static LIST_HEAD(workers_head);
static pthread_mutex_t workers_lock;
static sem_t workers_semph;

static int finished = 0;
static int putting = 0;

static void* (*get_func)(void*);
static void* (*put_func)(void*);

/* 根据path和namespace获取镜像数据 */
static remote_image* get_rimg_by_name(const char* namespace, const char* path)
{
        remote_image* rimg = NULL;
        pthread_mutex_lock(&rimg_lock); // 线程互斥锁
        list_for_each_entry(rimg, &rimg_head, l) { //通过双向循环链表l循环获取rimg结构的地址
                if( !strncmp(rimg->path, path, PATHLEN) &&
                    !strncmp(rimg->namespace, namespace, PATHLEN)) {
                        pthread_mutex_unlock(&rimg_lock);
                        return rimg;
                }
        }
        pthread_mutex_unlock(&rimg_lock);  // 线程互斥锁解锁
        return NULL;
}

/* 初始化线程锁和信号量 */
int init_sync_structures()
{
        if (pthread_mutex_init(&rimg_lock, NULL) != 0) { //线程互斥锁的初始化
                perror("Remote image connection mutex init failed");
                return -1;
        }

        if (sem_init(&rimg_semph, 0, 0) != 0) { //信号量的初始化
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

/* 通过套接字从CRIU获取镜像文件 */
/* fd 是通信套接字 */
void* get_remote_image(void* fd)
{
        int cli_fd = (long) fd;
        remote_image* rimg = NULL; // 保存镜像文件结构体
        char path_buf[PATHLEN];
        char namespace_buf[PATHLEN];

        // 从远程socket读namespace和path，并将其保存在各自变量中
        if(read_header(cli_fd, namespace_buf, path_buf) < 0) {
                perror("Error reading header");
                return NULL;
        }

        printf("Received GET for %s:%s.\n", path_buf, namespace_buf);

        // 等待镜像的到来
        rimg = wait_for_image(cli_fd, namespace_buf, path_buf);
        if (!rimg)
                return NULL;

        // 发送镜像数据
        rimg->dst_fd = cli_fd;
        send_remote_image(rimg->dst_fd, rimg->path, &rimg->buf_head);
        return NULL;
}

/* 准备发送镜像 */
void prepare_put_rimg()
{
        pthread_mutex_lock(&rimg_lock);
        putting++;
        pthread_mutex_unlock(&rimg_lock);
}

/* 开始发送镜像 */
void finalize_put_rimg(remote_image* rimg)
{
        pthread_mutex_lock(&rimg_lock);
        list_add_tail(&(rimg->l), &rimg_head);
        putting--;
        pthread_mutex_unlock(&rimg_lock);
        sem_post(&rimg_semph);
}

/* 初始化代理模块的函数指针：获取镜像、发送镜像 */
int init_proxy()
{
        get_func = get_remote_image; //函数指针
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

/* 准备服务端socket连接 */
int prepare_server_socket(int port)
{
        struct sockaddr_in serv_addr;
        int sockopt = 1;

        // 函数原型：int socket(int domain, int type, int protocol)
        // domain指定协议域，type选择socket类型，protocol指定协议（为0自动对应type的默认协议）
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
                perror("Unable to open image socket");
                return -1;
        }

        bzero((char *) &serv_addr, sizeof (serv_addr)); //每个字节用0填充
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY; //监听本地任意IP（多网卡）
        serv_addr.sin_port = htons(port);

        // 设置套接字的选项值，这里是打开地址和端口复用
        // SOL_SOCKET：套接字层次；SO_REUSERADDR：允许重用本地地址和端口
        if (setsockopt(
            sockfd, SOL_SOCKET, SO_REUSEADDR, &sockopt, sizeof (sockopt)) == -1) {
                perror("Unable to set SO_REUSEADDR");
                return -1;
        }

        // 将套接字和IP、端口绑定
        if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof (serv_addr)) < 0) {
                perror("Unable to bind image socket");
                return -1;
        }

        // 监听，等待用户发起请求
        if (listen(sockfd, DEFAULT_LISTEN)) {
                perror("Unable to listen image socket");
                return -1;
        }

        return sockfd;
}

/* 准备客户端socket连接 */
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

/* 等待镜像 */
remote_image* wait_for_image(int cli_fd, char* namespace, char* path)
{
        remote_image *result;

        while (1) {
                // 通过namespace和path查找到相应镜像文件
                result = get_rimg_by_name(namespace, path);
                // The file exists
                if(result != NULL) {
                        // 将path和namespace写入通信套接字
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
                
                //给rimg结构体赋空间
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

/* 实际获取镜像数据 */
int recv_remote_image(int fd, char* path, struct list_head* rbuff_head)
{
        // 找到了rbuff_head->next所在的remote_buffer结构体的地址
        remote_buffer* curr_buf = list_entry(rbuff_head->next, remote_buffer, l);
        int n, nblocks;

        nblocks = 0;
        while(1) {
                // 调用read函数实际从socket中读取数据
                n = read(fd,
                curr_buf->buffer + curr_buf->nbytes,
                BUF_SIZE - curr_buf->nbytes);
                if (n == 0) { // 如果没有读到数据
                        printf("Finished receiving %s (%d full blocks, %d bytes on last block)\n",
                                path, nblocks, curr_buf->nbytes);
                        // 如果buffer里面没有数据，那么前一个buf中is_end置true
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

/* 发送实际数据 */
size_t send_remote_obj(int fd, char* buff, size_t size) 
{
	size_t n = 0;
	size_t curr = 0;
	while(1) {
                //send a message on a socket
                //ssize_t send(int sockfd, const void *buf, size_t len, int flags);
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

/* 发送镜像数据，返回发送的数据量，调用send_remote_obj() */
int send_remote_image(int fd, char* path, struct list_head* rbuff_head)
{
        remote_buffer* curr_buf = list_entry(rbuff_head->next, remote_buffer, l);
        int nblocks;

	nblocks = 0;
		
        // 循环发送镜像数据
        while(1) {
                // msg head
                struct msgInfo msg; //？msg里记录的是数据的元数据
                msg.nbytes = curr_buf->nbytes;
                msg.cbytes = 0;
                msg.is_compressed = false;
                msg.is_end = curr_buf->is_end; // means msg will end

                char* buf = malloc(sizeof(msgInfo));
                memcpy(buf, &msg, sizeof(msgInfo));
                //fd是通信套接字
                // ？先发送元数据，再发送数据
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


/* 发送压缩后的镜像数据，返回发送的数据量，调用send_remote_obj() */
int send_remote_image_lz4(int fd, char* path, struct list_head* rbuff_head)
{
        remote_buffer* curr_buf = list_entry(rbuff_head->next, remote_buffer, l);
        int nblocks, bytes;

        nblocks = 0;
        bytes = 0;
		
        while(1) {
                // msg head
                struct msgInfo msg;
                msg.nbytes = curr_buf->nbytes;
                msg.cbytes = 0;
                msg.is_compressed = false;
                // means msg will end
                msg.is_end = curr_buf->is_end;

                int max_possible_lz4_bytes = LZ4_compressBound(curr_buf->nbytes);
                char* lz4_buf = (char *)malloc(max_possible_lz4_bytes * sizeof(char));
                msg.cbytes = LZ4_compress_fast(curr_buf->buffer, lz4_buf, curr_buf->nbytes, max_possible_lz4_bytes, 3);
                msg.is_compressed = true;
                
                char* buf = malloc(sizeof(msgInfo));
                memcpy(buf, &msg, sizeof(msgInfo));
                if (send_remote_obj(fd, buf, sizeof(msgInfo)) != sizeof(msgInfo)) {
                        printf("Write on %s msgInfo failed\n", path);
                        return -1;
                }

                if (send_remote_obj(fd, lz4_buf, msg.cbytes) == msg.cbytes) {
                        bytes += msg.cbytes;
                        if (msg.is_end) {
                                printf("Finished forwarding %s (%d full blocks) total %d\n",
                                        path, nblocks, bytes);
                        close(fd);
                        return bytes;
                }
                        nblocks++;
                } else {
                        printf("Write on %s msgData failed\n", path);
                        return -1;
                }
               	curr_buf = list_entry(curr_buf->l.next, remote_buffer, l);
        }
}
