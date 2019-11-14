#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>
#include <sys/time.h>

#include <google/protobuf-c/protobuf-c.h>

#include "image-remote-pvt.h"
#include "image-remote.h"
//#include "criu-log.h"
//#include "asm/types.h"
//#include "protobuf.h"
//#include "protobuf/pagemap.pb-c.h"
//#include "image-desc.h"

static char* dst_host;
static unsigned short dst_port = CACHE_PUT_PORT;

void* proxy_remote_image(void* ptr)
{
        remote_image* rimg = (remote_image*) ptr;
        // 作为客户端，连接的是目的端的机器
        rimg->dst_fd = prepare_client_socket(dst_host, dst_port);
        if (rimg->dst_fd < 0) {
                perror("Unable to open recover image socket");
                return NULL;
        }

        // 将namespace和path写入socket
        if(write_header(rimg->dst_fd, rimg->namespace, rimg->path) < 0) {
                printf("Error writing header for %s:%s",
                        rimg->path, rimg->namespace);
                return NULL;
        }

        // 准备传输镜像，putting++
        prepare_put_rimg();

        if (!strncmp(rimg->path, DUMP_FINISH, sizeof(DUMP_FINISH)))
        {
            close(rimg->dst_fd);
            finalize_put_rimg(rimg);
            return NULL;
	}

        // 得到镜像数据
        if (recv_remote_image(rimg->src_fd, rimg->path, &(rimg->buf_head)) < 0) {
                return NULL;
        }
        finalize_put_rimg(rimg);
        // 计时
        struct timeval start, end;
        gettimeofday(&start, NULL);
       	//if (!strncmp(rimg->path, "pages-", 6))
        //	send_remote_image(rimg->dst_fd, rimg->path, &(rimg->buf_head));
		//else

        // lz4压缩
        send_remote_image_lz4(rimg->dst_fd, rimg->path, &(rimg->buf_head));
		// else
        //	send_remote_image(rimg->dst_fd, rimg->path, &(rimg->buf_head));
        gettimeofday(&end, NULL);
        printf("%s send start %ld:%ld, end %ld:%ld\n", rimg->path, start.tv_sec, start.tv_usec, end.tv_sec, end.tv_usec);
        return NULL;
}

// fwd_host : forward host 发送主机
// fwd_host 对应 cache_host
// fwd_port 对应 cache_port
int image_proxy(char* fwd_host, unsigned short fwd_port)
{
        pthread_t get_thr, put_thr;
        int put_fd, get_fd;

        dst_host = fwd_host;
        dst_port = fwd_port;

        printf("Proxy Get Port %d, Put Port %d, Destination Host %s:%hu\n",
                PROXY_GET_PORT, PROXY_PUT_PORT, fwd_host, fwd_port);

        put_fd = prepare_server_socket(PROXY_PUT_PORT); //9996
        get_fd = prepare_server_socket(PROXY_GET_PORT); //9995
        if(init_proxy()) // 初始化，函数指针赋值
                return -1;

        // 创建线程执行 accept_put_image_connections() 函数
        if (pthread_create(
            &put_thr, NULL, accept_put_image_connections, (void*) &put_fd)) {
                perror("Unable to create put thread");
                return -1;
        }
        // 创建线程执行 accept_get_image_connections() 函数
        if (pthread_create(
            &get_thr, NULL, accept_get_image_connections, (void*) &get_fd)) {
                perror("Unable to create get thread");
                return -1;
        }

        join_workers();

        // NOTE: these joins will never return...
        pthread_join(put_thr, NULL);
        pthread_join(get_thr, NULL);
        return 0;
}
