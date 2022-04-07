sudo kill -9 $(ps aux | grep '[Q]Dcache-User/TestProg' | awk '{print $2}')
sudo kill -9 $(ps aux | grep '[L]atency_InferModel' | awk '{print $2}')
sudo rm -f /dev/shm/*
ls -la /dev/shm/
