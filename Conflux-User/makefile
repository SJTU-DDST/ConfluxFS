
simple_test:
	gcc -g ./TestProg/simpleTest.c\
		-Wno-format \
		-lpthread\
		-o ./bin/simple_test

conflux_call:
	gcc -g conflux_syscall.c thpool.c\
		-lsyscall_intercept -lpthread -lrt -ldl -libverbs -lpmem -ltensorflow -ltensorflowlite_c -lm\
		-Wno-format -fPIC -shared\
		-o ./bin/libconflux.so

manager:
	gcc -g dcache_manager.c thpool.c\
		-Wno-format \
		-libverbs -lpthread -lpmem -lrt -ltensorflow -ltensorflowlite_c -lm -lsyscall_intercept\
		-o ./bin/dcache_manager

aiagent:
	gcc -g conflux_AIagent.c\
		-Wno-format \
		-lpthread -ltensorflow -ltensorflowlite_c -lrt -lm\
		-o ./bin/conflux_AIagent

clean:
	rm -f ./bin/*
