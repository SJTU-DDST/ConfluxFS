#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include "../dcache_timer.c"

const int SEGMT_LEN = (1 << 20) * 80;
const int BLOCK_SIZE = (1 << 20);
const int TEST_SEC = 25;
const int WORK_PROC = 24;


void rw_test(int fdnum, int rw_flag, uint64_t start){
    srand(time(NULL)); rand(); rand();

    int fd = fdnum;
    int i = 0;
    void* buf = malloc(BLOCK_SIZE);
    memset(buf, 97, BLOCK_SIZE);

    struct caltime_nano* timer = init_caltime_nano();
    start_caltime_nano(timer);

    if (rw_flag == 0){
        uint64_t offs = 0;
        while (1){
            offs = start + ((uint64_t)i * BLOCK_SIZE) % SEGMT_LEN;
            pread(fd, buf, BLOCK_SIZE, offs);
            i += 1;
            uint64_t etime = stop_caltime_nano(timer);
            if (etime >= (uint64_t)TEST_SEC * 1000000000) break;
        }
        fprintf(stdout, "Read bandwidth in proc: %f MBps.\n", ((float)BLOCK_SIZE * i) / ((1<<20)*TEST_SEC) );
    }

    if (rw_flag == 1){
        uint64_t offs = 0;
        while (1){
            offs = ((uint64_t)i * BLOCK_SIZE) % SEGMT_LEN;
            pwrite(fd, buf, BLOCK_SIZE, offs);
            i += 1;
            uint64_t etime = stop_caltime_nano(timer);
            if (etime >= (uint64_t)TEST_SEC * 1000000000) break;
        }
        fprintf(stdout, "Write bandwidth in proc: %f MBps.\n", ((float)BLOCK_SIZE * i) / ((1<<20)*TEST_SEC) );
    }
    
    free(buf);
    return;
}

int main(){

    int p = 0;
    for (int p = 0; p < WORK_PROC; ++p) {

        int pid = fork();
        if (pid > 0) continue;

        int fd2 = open("/confluxFS/MyTestDir/rwfile.txt", O_RDWR);
        fprintf(stdout, "myfs opened rwfile.txt's fd: %d\n", fd2);
        int fd3 = open("/confluxFS/MyTestDir/rwfile.txt", O_RDWR);
        fprintf(stdout, "myfs opened rwfile.txt's fd: %d\n", fd3);
        int fd4 = open("/confluxFS/MyTestDir/rwfile.txt", O_RDWR);
        fprintf(stdout, "myfs opened rwfile.txt's fd: %d\n", fd4);

        fprintf(stdout, "\nrw test on pmem file\n\n");
        rw_test(fd2, 0, SEGMT_LEN * p);

        close(fd2);
        close(fd3);
        close(fd4);

        exit(0);
    }
    for (p = 0; p < WORK_PROC; p++) wait(NULL);
    
    return 0;
}
