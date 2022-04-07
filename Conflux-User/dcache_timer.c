#ifndef __DCACHE_TIMER
#define __DCACHE_TIMER

#include <sys/time.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int waitMiliSec(const uint32_t x){
    unsigned long start_time_msec;
	unsigned long cur_time_msec;
    struct timeval cur_time;
    gettimeofday(&cur_time, NULL);
	start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    do{
        gettimeofday(&cur_time, NULL);
		cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    }while (cur_time_msec - start_time_msec < x);
    return 0;
}

int waitMicroSec(const uint32_t x){
    unsigned long start_time_usec;
	unsigned long cur_time_usec;
    struct timeval cur_time;
    gettimeofday(&cur_time, NULL);
	start_time_usec = (cur_time.tv_sec * 1000000) + cur_time.tv_usec;
    do{
        gettimeofday(&cur_time, NULL);
		cur_time_usec = (cur_time.tv_sec * 1000000) + cur_time.tv_usec;
    }while (cur_time_usec - start_time_usec < x);
    return 0;
}

int waitNanoSec(const uint64_t x){
    uint64_t start_time_nsec;
    uint64_t cur_time_nsec;
    struct timespec cur_time;
    clock_gettime(CLOCK_BOOTTIME, &cur_time);
    start_time_nsec = cur_time.tv_sec * (uint64_t)1000000000 + cur_time.tv_nsec;
    do{
        clock_gettime(CLOCK_BOOTTIME, &cur_time);
        cur_time_nsec = cur_time.tv_sec * (uint64_t)1000000000 + cur_time.tv_nsec;
    }while (cur_time_nsec - start_time_nsec < x);
    return 0;
}

struct caltime_nano{
    uint64_t init_time_nsec;
    uint64_t start_time_nsec;
    uint64_t cur_time_nsec;
    struct timespec cur_time;
};

struct caltime_nano* init_caltime_nano(){
    struct caltime_nano* res = (struct caltime_nano*)malloc(sizeof(struct caltime_nano));
    memset(res, 0, sizeof(struct caltime_nano));
    clock_gettime(CLOCK_BOOTTIME, &(res->cur_time));
    res->init_time_nsec = res->cur_time.tv_sec * (uint64_t)1000000000 + res->cur_time.tv_nsec;
    return res;
}

int start_caltime_nano(struct caltime_nano* timer_nano){
    clock_gettime(CLOCK_BOOTTIME, &(timer_nano->cur_time));
    timer_nano->start_time_nsec = timer_nano->cur_time.tv_sec * (uint64_t)1000000000 + timer_nano->cur_time.tv_nsec;
    return 0;
}

uint64_t stop_caltime_nano(struct caltime_nano* timer_nano){
    clock_gettime(CLOCK_BOOTTIME, &(timer_nano->cur_time));
    timer_nano->cur_time_nsec = timer_nano->cur_time.tv_sec * (uint64_t)1000000000 + timer_nano->cur_time.tv_nsec;
    uint64_t resx = timer_nano->cur_time_nsec - timer_nano->start_time_nsec;
    return resx;
}

char* genTimeInfo(const char * username){
    char* ret = (char*)malloc(80);
    memset(ret, 0, 80);
    struct timeval cur_time;
    gettimeofday(&cur_time, NULL);
    time_t now_time = cur_time.tv_sec;
    suseconds_t now_usec = cur_time.tv_usec;
    fprintf(stdout, "DEBUG_INFO: now time is: %u.%06u\n", now_time, now_usec);
    struct tm *time_info = gmtime(&now_time);
    sprintf(ret, "(%s)%d-%02d-%02d,%02d:%02d:%02d.%06u", username,
        time_info->tm_year+1900, time_info->tm_mon+1, time_info->tm_mday, 
        time_info->tm_hour, time_info->tm_min, time_info->tm_sec, now_usec);
    return ret;
}

#endif 
