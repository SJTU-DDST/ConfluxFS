
owner:
	gcc -g dcache_owner.c thpool.c\
		-Wno-format \
		-libverbs -lpthread -lpmem -lrt\
		-o ./bin/dcache_owner

backgr:
	gcc -g qdfs_backgr.c\
		-Wno-format \
		-lpthread -libverbs -lpmem -lrt\
		-o ./bin/qdfs_backgr


clean:
	rm -f ./bin/*
