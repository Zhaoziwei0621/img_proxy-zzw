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

int main(int argc, char *argv[])
{
	if (argc < 3) {
		printf("Please input ip and port\n");
		return -1;
	}

	printf("Client ip=%s , port=%d\n", argv[1], atoi(argv[2]));

	if (image_proxy(argv[1], atoi(argv[2]))) {
		printf("Send image failed\n");
		return -1;
	}

	return 0;
}
