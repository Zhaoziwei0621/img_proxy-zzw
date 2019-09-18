
main: main.c
	gcc main.c image-proxy.c image-remote.c image-remote-pvt.c lz4.c -lpthread -Wall -o main

tags:
	echo "  Gen       " $@
	rm -rf tags
	find . -name '*.[hcS]' ! -path './.*' | xargs ctags -a

cscope:
	echo "  Gen       " $@
	find . -name '*.[hcS]' ! -path './.*' ! -type l -print > cscope.files
	cscope -bkqu

image-proxy: image-proxy.c read_image.c
	gcc image-proxy.c read_image.c -o image-proxy

.PHONY: clean
clean:
	-rm main
