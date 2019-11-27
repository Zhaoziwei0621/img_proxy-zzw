/* 
 * File:   image-remote.c
 * Author: underscore
 *
 * Created on July 7, 2015, 12:46 AM
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include <pthread.h>
#include <semaphore.h>

//#include "criu-log.h"
#include "image-remote.h"

// TODO - fix space limitation
static char parents[PATHLEN][PATHLEN];
static int  parents_occ = 0;
static char* namespace = NULL;
// TODO - not used for now. It will be used if we implement a shared cache and proxy.
static char* parent = NULL;

int setup_local_client_connection(int port)
{
        int sockfd;
        struct sockaddr_in serv_addr;
        struct hostent *server;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
                perror("Unable to open remote image socket to img cache");
                return -1;
        }

        server = gethostbyname(DEFAULT_HOST);
        if (server == NULL) {
                printf("Unable to get host by name (%s)", DEFAULT_HOST);
                return -1;
        }

        bzero((char *) &serv_addr, sizeof (serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *) server->h_addr,
              (char *) &serv_addr.sin_addr.s_addr,
              server->h_length);
        serv_addr.sin_port = htons(port);

        if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
                printf("Unable to connect to remote restore host %s", DEFAULT_HOST);
                return -1;
        }

        return sockfd;
}

/* 写远程镜像（namespace和path） */
int write_header(int fd, char* namespace, char* path)
{
        if (write(fd, path, PATHLEN) < 1) {
                perror("Unable to send path to remote image connection");
                return -1;
        }

        if (write(fd, namespace, PATHLEN) < 1) {
                perror("Unable to send namespace to remote image connection");
                return -1;
        }
        return 0;
}

/* 读取远程镜像对应的namespace和path */
int read_header(int fd, char* namespace, char* path)
{
        int n = read(fd, path, PATHLEN);
        if (n < 0) {
                perror("Error reading from remote image socket");
                return -1;
        } else if (n == 0) {
                perror("Remote image socket closed before receiving path");
                return -1;
        }
        n = read(fd, namespace, PATHLEN);
        if (n < 0) {
                perror("Error reading from remote image socket");
                return -1;
        } else if (n == 0) {
                perror("Remote image socket closed before receiving namespace");
                return -1;
        }
    return 0;
}

/* 由restore调用，获取对应于特定路径的fd，该调用将在收到连接时阻塞 */
int get_remote_image_connection(char* namespace, char* path)
{
        int sockfd;
        char path_buf[PATHLEN], ns_buf[PATHLEN];;

        sockfd = setup_local_client_connection(CACHE_GET_PORT);
        if(sockfd < 0) {
               return -1;
        }

        if(write_header(sockfd, namespace, path) < 0) {
                printf("Error writing header for %s:%s", path, namespace);
                return -1;
        }

        if(read_header(sockfd, ns_buf, path_buf) < 0) {
                printf("Error reading header for %s:%s", path, namespace);
                return -1;
        }

        if(!strncmp(path_buf, path, PATHLEN) && !strncmp(ns_buf, namespace, PATHLEN)) {
                printf("Image cache does have %s:%s\n", path, namespace);
                return sockfd;
        }
        else if(!strncmp(path_buf, DUMP_FINISH, PATHLEN)) {
                printf("Image cache does not have %s:%s\n", path, namespace);
                close(sockfd);
                return -1;
        }
        else {
                printf("Image cache returned erroneous name %s\n", path);
                close(sockfd);
                return -1;
        }
}

/* 由dump调用，创建到restore端的套接字连接，返回的套接字fd用于进一步的写操作 */
int open_remote_image_connection(char* namespace, char* path)
{
        int sockfd = setup_local_client_connection(PROXY_PUT_PORT);
        if(sockfd < 0) {
                return -1;
        }

        if(write_header(sockfd, namespace, path) < 0) {
                printf("Error writing header for %s:%s", path, namespace);
                return -1;
        }

        return sockfd;
}

/* 由dump在完成时调用，该函数使用特别控制名创建新的连接，恢复端使用它来ack不再有新的文件到来 */
int finish_remote_dump()
{
        printf("Dump side is calling finish\n");
        int fd = open_remote_image_connection(NULL_NAMESPACE, DUMP_FINISH);
        if (fd == -1) {
                printf("Unable to open finish dump connection");
                return -1;
        }
        close(fd);
        return 0;
}

/* 从fd读取（丢弃）len字节，用于模拟函数lseek-用于推进文件指针 */
int skip_remote_bytes(int fd, unsigned long len)
{
    static char buf[4096];
    int n = 0;
    unsigned long curr = 0;

    for(; curr < len; ) {
            n = read(fd, buf, MIN(len - curr, 4096));
            if(n == 0) {
                printf("Unexpected end of stream (skipping %lx/%lx bytes)",
                        curr, len);
                return -1;
            }
            else if(n > 0) {
                    curr += n;
            }
            else {
                printf("Error while skipping bytes from stream (%lx/%lx)",
                        curr, len);
                return -1;
            }
    }
    if( curr != len) {
            printf("Unable to skip the current number of bytes: %lx instead of %lx",
                    curr, len);
            return -1;
    }
    return 0;
}

static int push_namespaces()
{
        int n;
        int sockfd = open_remote_image_connection(NULL_NAMESPACE, PARENT_IMG);
        if(sockfd < 0) {
                printf("Unable to open namespace push connection");
                return -1;
        }
        for(n = 0; n < parents_occ; n++) {
                if (write(sockfd, parents[n], PATHLEN) < 1) {
                        printf("Could not write namespace %s to socket", parents[n]);
                        close(sockfd);
                        return -1;
                }
        }

        close(sockfd);
        return 0;
}

static int fetch_namespaces() {
        int n, sockfd;
        parents_occ = 0;
        // Read namespace hierarchy
        sockfd = get_remote_image_connection(NULL_NAMESPACE, PARENT_IMG);
        if(sockfd < 0) {
                printf("Unable to open namespace get connection");
                return -1;
        }
        while(1) {
                n = read(sockfd, parents[parents_occ], PATHLEN);
                if(n == 0) {
                        close(sockfd);
                        break;
                }
                else if(n > 0) {
                        if(++parents_occ > PATHLEN) {
                                printf("Parent sequence above the size limit");
                                return -1;
                        }
                }
                else {
                        printf("Failed to read namespace from socket");
                }
        }
        return parents_occ;
}

/* 将当前命名空间推送到命名空间层次结构中，该层次结构可被读、修改、写 */
int push_namespace()
{
    if(fetch_namespaces() < 0) {
            printf("Failed to push namespace");
            return -1;
    }
    strncpy(parents[parents_occ++], namespace, PATHLEN);
    if(push_namespaces()) {
            printf("Failed to push namespaces");
            return -1;
    }
    return parents_occ;
}

/* 设置当前命名空间和父命名空间 */
void init_namespace(char* ns, char* p)
{
        namespace = ns;
        parent = p;
}

/* 返回一个整数（虚拟fd），它代表当前命名空间的整数 */
int get_current_namespace_fd()
{
        int i = 0;

        if(parents_occ == 0) {
                if(fetch_namespaces() < 0) {
                    return -1;
                }
        }

        for(; i < parents_occ; i++) {
            if(!strncmp(parents[i], namespace, PATHLEN))
                return i;
        }
        printf("Error, could not find current namespace fd"); 
        return -1;
}

/* 返回一个指针，它指向包括当前命名空间的char数组 */
char* get_current_namespace()
{
        return namespace;
}

/* 返回一个与虚拟fd（作为参数）相关联的命名空间 */
char* get_namespace(int dfd)
{
        if(parents_occ == 0) {
                if(fetch_namespaces() < 0) {
                        printf("No namespace in parent hierarchy (%s:%s)",
                                namespace, parent);
                        return NULL;
                }
        }
        if(dfd >= parents_occ || dfd < 0)
                return NULL;
        else
                return parents[dfd];
}
