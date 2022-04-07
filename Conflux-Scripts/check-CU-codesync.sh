ls ../Conflux-Center/
ls ../Conflux-User/
echo "[Code Check] In dcache_tcpfunc.c:"
diff ../Conflux-Center/dcache_tcpfunc.c ../Conflux-User/dcache_tcpfunc.c
echo "[Code Check] In dcache_timer.c:"
diff ../Conflux-Center/dcache_timer.c ../Conflux-User/dcache_timer.c
echo "[Code Check] In thpool.h:"
diff ../Conflux-Center/thpool.h ../Conflux-User/thpool.h
echo "[Code Check] In thpool.c:"
diff ../Conflux-Center/thpool.c ../Conflux-User/thpool.c
echo "[Code Check] In hashmapstr.h:"
diff ../Conflux-Center/hashmapstr.h ../Conflux-User/hashmapstr.h
