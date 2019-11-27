// header files for log
#include <stdio.h>
#include <errno.h>

// header files for network
#include <sys/types.h>
#include <sys/socket.h>

#include <netinet/in.h>
#include <netdb.h>

// header files for string
#include <stdlib.h>
#include <string.h>

// file operator
#include <unistd.h>

#include "image-remote.h"

// #define IMAGE_DIR "/tmp/transport/dump/pagemap-2344.img"
// ./main <cntr_dst ip addr> 9996 传入的是目标机器的ip 和 端口号
// cntr_dst被宏定义修改为默认的ip地址：192.168.6.146

int main(int argc, char *argv[])
{
	if (argc < 3) {
		printf("Please input ip and port\n");
		return -1;
	}

	printf("Client ip=%s , port=%d\n", argv[1], atoi(argv[2]));

	if (image_proxy(argv[1], atoi(argv[2]))) { //调用代理函数，传入参数目的端IP + 端口号
		printf("Send image failed\n");
		return -1;
	}

	return 0;
}
