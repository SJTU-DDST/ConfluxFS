ls ../QDcache-Center/
ls ../QDcache-User/
echo "[Code Check] In dcache_tcpfunc.c:"
diff ../QDcache-Center/dcache_tcpfunc.c ../QDcache-User/dcache_tcpfunc.c
echo "[Code Check] In dcache_timer.c:"
diff ../QDcache-Center/dcache_timer.c ../QDcache-User/dcache_timer.c
echo "[Code Check] In thpool.h:"
diff ../QDcache-Center/thpool.h ../QDcache-User/thpool.h
echo "[Code Check] In thpool.c:"
diff ../QDcache-Center/thpool.c ../QDcache-User/thpool.c
echo "[Code Check] In hashmapstr.h:"
diff ../QDcache-Center/hashmapstr.h ../QDcache-User/hashmapstr.h
