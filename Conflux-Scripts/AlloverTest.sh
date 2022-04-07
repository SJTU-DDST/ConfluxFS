
username=The-System-UserName
testprog="fio"
testname=mytest.fio
centerPath=/home/$username/nfs/Band-Aggr/Conflux-Center
clientPath=/home/$username/nfs/Band-Aggr/Conflux-User
pythonPath=/home/$username/anaconda3/envs/tf2/bin/python
mallocPath=/usr/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4
cpumaskS=0x00AAAA
cpumaskL=0xAAAAAAAAAAAAAAAAAAAAAAAA0000

echo "Start the test"

ssh $username@Center-IP sudo LD_PRELOAD=$mallocPath taskset $cpumaskS unbuffer $centerPath/bin/qdfs_backgr >$centerPath/logs/output_backgr.log 2>&1 &

sleep 10

ssh $username@User-IP sudo LD_PRELOAD=$mallocPath taskset $cpumaskS unbuffer $pythonPath $clientPath/PyTfcode/Latency_InferModel.py >$clientPath/logs/output_aipart.log 2>&1 &

sleep 10

ssh $username@Center-IP sudo LD_PRELOAD=$mallocPath taskset $cpumaskL unbuffer $centerPath/bin/dcache_owner >$centerPath/logs/output_owner.log 2>&1 &

sleep 6

ssh $username@User-IP sudo LD_PRELOAD=$clientPath/bin/libconflux.so:$mallocPath taskset $cpumaskL unbuffer $testprog $clientPath/TestProg/$testname >$clientPath/logs/output_conflux.log 2>&1 &

echo "Test deployed"
