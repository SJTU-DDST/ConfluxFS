#ifndef __QDFS_ALGORI
#define __QDFS_ALGORI

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "dcache_timer.c"
#include "hashmapstr.h"
#include "dcache_rdmaconx.c"

#define SEG_PER_BLOCK 507
const int CPY_MAX_DTBLK = (1 << 20) * 14;
const int CPY_DTBLK_SIZE = (1 << 12);


uint64_t get_next_newseg(uint8_t* bm, uint32_t *bm_id, uint32_t ablk, uint32_t *blknum){
    // Remember, before using this function you need to lock dtblk_bm_mutex
    uint64_t res = 0;
    uint32_t beg = 0;
    int cnt = 0;
    while (cnt < CPY_MAX_DTBLK){
        if (bm[*bm_id] == 0) break;
        *bm_id = (*bm_id) + 1;
        if (*bm_id >= CPY_MAX_DTBLK) *bm_id = 0;
        cnt += 1;
    } 
    if (bm[*bm_id] != 0) return res;
    beg = (*bm_id);
    uint32_t len = 0;
    while (len < ablk){
        if (bm[*bm_id] != 0) break;
        bm[*bm_id] = 1;
        *bm_id = (*bm_id) + 1;
        if (*bm_id >= CPY_MAX_DTBLK) *bm_id = 0;
        len += 1;
    } 
    *blknum = (*blknum) + len;
    res = res | ((uint64_t)(*blknum) << 32);
    res = res + (uint64_t)beg;
    return (res);
} 

// search target in arr, with interval [left, right], where target 
// might be beyond right's seg range(return -1).
int arr_binary_find1011(uint32_t* arr, uint32_t left, uint32_t right, uint32_t target){
    if (arr[right*2+1] <= target)
        return -1;
    uint32_t mid = 0;
    while (right > left){
        mid = (left + right) / 2;
        if (arr[mid*2+1] > target) right = mid;
        else left = mid + 1;
    }
    return left;
}

// keep in mind that length of a dir-file is multiple of 4K
char* dirbuf_add_term(char* content, const char* filename, uint64_t *max_len, uint32_t *pos){
    char* ret = NULL;
    int name_len = strlen(filename) + 1;
    if (*max_len == 0){
        uint32_t addblk = name_len / CPY_DTBLK_SIZE + 1;
        *max_len = (*max_len) + (uint64_t)addblk * CPY_DTBLK_SIZE;
        ret = (char*)malloc(*max_len);
        memset(ret, 0, *max_len);
        strcpy(ret, filename);
        *pos = 1;
        return ret;
    }
    int64_t pre_tail = -1;
    uint64_t cur_head = 0;
    uint64_t cur_tail = 0;
    uint8_t flag = 0;
    while (1){
        while (content[cur_head] == '\0'){
            cur_head += 1;
            if (cur_head == *max_len){
                cur_head = pre_tail + 1;
                flag = 1;
                break;
            }
        }
        *pos += 1;
        if (flag) break;

        // remove 0s between filename terms
        uint64_t res_len = *max_len - cur_head;
        char* cpy_buf = (char*)malloc(res_len);
        memcpy(cpy_buf, content+cur_head, res_len);
        memcpy(content+pre_tail+1, cpy_buf, res_len);
        memset(content+pre_tail+1+res_len, 0, (*max_len-pre_tail-1-res_len));
        cur_head = pre_tail + 1;

        cur_tail = cur_head;
        while (cur_tail < *max_len){
            cur_tail += 1;
            if (content[cur_tail] == '\0') break;
        }
        int namediff = strcmp(filename, content+cur_head);
        if (!namediff) return ret;
        pre_tail = cur_tail;
        cur_head = pre_tail;
    }
    uint64_t newlen = cur_head + name_len;
    newlen = newlen - (newlen % CPY_DTBLK_SIZE) + CPY_DTBLK_SIZE;
    if (newlen > *max_len) *max_len = newlen;
    ret = (char*)malloc(*max_len);
    memset(ret, 0, *max_len);
    int i;
    for (i = 0; i < cur_head; ++i) ret[i] = content[i];
    strcpy(ret+cur_head, filename);
    return ret;
}

int32_t find_fname_inbuf(char* buf, const char* fname, uint64_t buflen){
    uint64_t p1 = 0;
    uint64_t p2 = 0;
    int32_t ret = 0;

    int fname_len = strlen(fname);
    while (1){
        while (buf[p1] == '\0' && p1 < buflen) p1 += 1;
        if (p1 >= buflen) break;
        ret += 1;
        p2 = p1;
        do {
            p2 += 1;
            if (p2 >= buflen) break;
        } while (buf[p2] != '\0');
        if (p2 - p1 == fname_len){
            int diff = strcmp(fname, buf+p1);
            if (!diff) return ret;
        }
        p1 = p2;
        if (p1 >= buflen) break;
    }
    return -1;
}

// Note that this function will return an allocated int array, in which are
// all the isxlocks' id (on data block) for [beg, end] memory interval. 
int* get_blklock_list(uint64_t beg, uint64_t end, int locknum){
    int *ret = (int*)malloc(sizeof(int)*128);
    memset(ret, 0, sizeof(int)*128);
    int retcnt = 0;
    int begblk = beg / CPY_DTBLK_SIZE + 1;
    int endblk = end / CPY_DTBLK_SIZE + 1;
    int bit = 1;
    int tmp;
    while (1){
        if (endblk - bit < begblk) break;
        if (endblk & bit){
            tmp = (locknum / bit - 1) + (endblk / bit);
            ret[retcnt] = tmp;
            retcnt += 1;
            endblk -= bit;
        }
        bit *= 2;
    }
    while (bit){
        if (endblk - bit + 1 < begblk) {
            bit /= 2;
            continue;
        }
        tmp = (locknum / bit - 1) + (endblk / bit);
        ret[retcnt] = tmp;
        retcnt += 1;
        endblk -= bit;
        bit /= 2;
    }
    return ret;
}

struct myISXlockStruct{
    pthread_mutex_t acc;
    uint32_t IS_cnt;
    uint32_t IX_cnt;
    uint32_t S_cnt;
    uint8_t X_cnt;
    uint8_t IX_pre;
    uint16_t X_pre; 
};

const uint32_t ISX_I_MAX = (1<<28);
const uint32_t ISX_S_MAX = (1<<22);
const uint8_t ISX_i_tol = 8;
const uint16_t ISX_x_tol = 64;
const int TRYCNT_UPPER = 5;


int isxlock_IS_lock(struct myISXlockStruct* isxlock){
    int try_cnt = 0;
    while (1){
        pthread_mutex_lock(&(isxlock->acc));
        if (isxlock->X_pre >= ISX_x_tol || isxlock->IS_cnt > ISX_I_MAX){
            pthread_mutex_unlock(&(isxlock->acc));
            try_cnt += 1;
            if (try_cnt >= TRYCNT_UPPER) return -1;
            waitNanoSec(50);
            continue;
        }
        isxlock->IS_cnt += 1;
        pthread_mutex_unlock(&(isxlock->acc));
        break;
    }
    return 0;
}

int isxlock_IS_unlock(struct myISXlockStruct* isxlock){
    pthread_mutex_lock(&(isxlock->acc));

    if (isxlock->IS_cnt == 0){
        // fprintf(stderr, "[ERR]: try to unlock a non-exist IS lock\n");
        pthread_mutex_unlock(&(isxlock->acc));
        return -1;
    }
    isxlock->IS_cnt -= 1;
    pthread_mutex_unlock(&(isxlock->acc));

    return 0;
}

int isxlock_IX_lock(struct myISXlockStruct* isxlock){
    int try_cnt = 0;
    while (1){
        pthread_mutex_lock(&(isxlock->acc));
        if (isxlock->S_cnt > 0 || isxlock->X_pre >= ISX_x_tol || isxlock->IX_cnt > ISX_I_MAX){
            if (isxlock->IX_pre >= ISX_i_tol) isxlock->IX_pre = ISX_i_tol;
            else isxlock->IX_pre += 1;
            pthread_mutex_unlock(&(isxlock->acc));
            try_cnt += 1;
            if (try_cnt >= TRYCNT_UPPER) return -1;
            waitNanoSec(50);
            continue;
        }
        isxlock->IX_cnt += 1;
        isxlock->IX_pre = 0;
        pthread_mutex_unlock(&(isxlock->acc));
        break;
    }
    return 0;
}

int isxlock_IX_unlock(struct myISXlockStruct* isxlock){
    pthread_mutex_lock(&(isxlock->acc));

    if (isxlock->IX_cnt == 0){
        // fprintf(stderr, "[ERR]: try to unlock a non-exist IX lock\n");
        pthread_mutex_unlock(&(isxlock->acc));
        return -1;
    }
    isxlock->IX_cnt -= 1;
    pthread_mutex_unlock(&(isxlock->acc));

    return 0;
}

int isxlock_S_lock(struct myISXlockStruct* isxlock){
    int try_cnt = 0;
    while (1){
        pthread_mutex_lock(&(isxlock->acc));
        if (isxlock->X_pre >= ISX_x_tol || isxlock->IX_cnt > 0 
                || isxlock->IX_pre >= ISX_i_tol || isxlock->S_cnt > ISX_S_MAX) {
            pthread_mutex_unlock(&(isxlock->acc));
            try_cnt += 1;
            if (try_cnt >= TRYCNT_UPPER) return -1;
            waitNanoSec(50);
            continue;
        }
        isxlock->S_cnt += 1;
        pthread_mutex_unlock(&(isxlock->acc));
        break;
    }
    return 0;
}

int isxlock_S_unlock(struct myISXlockStruct* isxlock){
    pthread_mutex_lock(&(isxlock->acc));

    if (isxlock->S_cnt == 0){
        // fprintf(stderr, "[ERR]: try to unlock a non-exist S lock\n");
        pthread_mutex_unlock(&(isxlock->acc));
        return -1;
    }
    isxlock->S_cnt -= 1;
    pthread_mutex_unlock(&(isxlock->acc));

    return 0;
}

int isxlock_X_lock(struct myISXlockStruct* isxlock){
    int try_cnt = 0;
    while (1){
        pthread_mutex_lock(&(isxlock->acc));
        if (isxlock->X_cnt == 1 || isxlock->S_cnt > 0 || isxlock->IX_cnt > 0 || isxlock->IS_cnt > 0){
            if (isxlock->X_pre >= ISX_x_tol) isxlock->X_pre = ISX_x_tol;
            else isxlock->X_pre += 1;
            pthread_mutex_unlock(&(isxlock->acc));
            try_cnt += 1;
            if (try_cnt >= TRYCNT_UPPER) return -1;
            waitNanoSec(60);
            continue;
        }
        isxlock->X_pre = 0;
        isxlock->X_cnt = 1;
        pthread_mutex_unlock(&(isxlock->acc));
        break;
    }
    return 0;
}

int isxlock_X_unlock(struct myISXlockStruct* isxlock){
    pthread_mutex_lock(&(isxlock->acc));
    if (isxlock->X_cnt != 1){
        fprintf(stderr, "[ERR]: try to unlock a non-locked X lock\n");
        return -1;
    }
    isxlock->X_cnt = 0;
    pthread_mutex_unlock(&(isxlock->acc));
    return 0;
}

struct myISXlockStruct* create_dtblk_lock(const char* shmname, int locknum){
    struct myISXlockStruct* ret;
    int lock_ipcfd = shm_open(shmname, O_RDWR | O_CREAT, 0666);
    if (lock_ipcfd <= 0) {
        perror("shmopen error:");
    }
    int ftct = ftruncate(lock_ipcfd, sizeof(struct myISXlockStruct) * locknum * 2);
    if (ftct < 0){
        perror("ftruncate:");
        exit(1);
    }
    ret = mmap(NULL, sizeof(struct myISXlockStruct) * locknum * 2, 
        PROT_WRITE, MAP_SHARED, lock_ipcfd, 0);
    if (ret == MAP_FAILED){
        perror("mmap in create_dtblk_lock:");
        exit(1);
    }
    memset(ret, 0, sizeof(struct myISXlockStruct) * locknum * 2);

    // fprintf(stderr, "RUN INFO: create_DtBlk, BEFORE mutexattr_init\n");

    pthread_mutexattr_t mutex_attr;
    pthread_mutexattr_init(&mutex_attr);
    pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED);
    int i;
    for (i = 0; i < locknum * 2; ++i){
        struct myISXlockStruct* isxlock = ret + i;
        pthread_mutex_t* lockacc = &(isxlock->acc);
        pthread_mutex_init(lockacc, &mutex_attr);
    }
    close(lock_ipcfd);

    // fprintf(stderr, "RUN INFO: create_DtBlk, BEFORE X_lock\n");

    isxlock_X_lock(ret + 0);
    return ret;
}

int destroy_dtblk_lock(struct myISXlockStruct* lock, const char* shmname, int locknum){    
    isxlock_X_unlock(lock + 0);
    
    int i;
    for (i = 0; i < locknum * 2; ++i){
        struct myISXlockStruct* isxlock = lock + i;
        pthread_mutex_t* lockacc = &(isxlock->acc);
        pthread_mutex_destroy(lockacc);
    }

    int mup = munmap(lock, sizeof(struct myISXlockStruct) * locknum * 2);
    if (mup < 0){
        perror("munmap in destroy_dtblk_lock:");
        exit(1);
    }

    return 0;
}

// From top to bottom
int dtblk_rd_lock(struct myISXlockStruct* lock, int id){
    int quetop = 0;
    int* par_que = (int*)malloc(64*sizeof(int));
    memset(par_que, 0, 64*sizeof(int));
    int par = id;
    // As far as I can see, this locking strategy is not optimal in space
    while(par > 1){
        par /= 2;
        par_que[quetop] = par;
        quetop += 1;
    };
    int lockopret = 0;
    int quepos = quetop;
    while(quepos > 0){
        quepos -= 1;
        lockopret = isxlock_IS_lock(lock + par_que[quepos]);
        if (lockopret < 0) {
            quepos += 1;
            while (quepos < quetop){
                isxlock_IS_unlock(lock + par_que[quepos]);
                quepos += 1;
            }
            return lockopret;
        }
    }
    lockopret = isxlock_S_lock(lock + id);
    if (lockopret < 0) return lockopret;
    free(par_que);
    return 0;
}

// From bottom to top
int dtblk_rd_unlock(struct myISXlockStruct* lock, int id){
    int quetop = 0;
    int* par_que = (int*)malloc(64*sizeof(int));
    memset(par_que, 0, 64*sizeof(int));
    int par = id;
    // As far as I can see, this locking strategy is not optimal in space
    while(par > 1){
        par /= 2;
        par_que[quetop] = par;
        quetop += 1;
    };
    int sunl = isxlock_S_unlock(lock + id);
    if (sunl == 0){
        int j;
        for (j = 0; j < quetop; ++j)
            isxlock_IS_unlock(lock + par_que[j]);
    }
    free(par_que);
    return 0;
}

// also, from top to bottom
int dtblk_wr_lock(struct myISXlockStruct* lock, int id){
    int quetop = 0;
    int* par_que = (int*)malloc(64*sizeof(int));
    memset(par_que, 0, 64*sizeof(int));
    int par = id;
    while(par > 1){
        par /= 2;
        par_que[quetop] = par;
        quetop += 1;
    };
    int lockopret = 0;
    int quepos = quetop;
    while(quepos > 0){
        quepos -= 1;
        lockopret = isxlock_IX_lock(lock + par_que[quepos]);
        if (lockopret < 0) {
            quepos += 1;
            while (quepos < quetop){
                isxlock_IX_unlock(lock + par_que[quepos]);
                quepos += 1;
            }
            return lockopret;
        }
    }
    lockopret = isxlock_X_lock(lock + id);
    if (lockopret < 0) return lockopret;
    free(par_que);
    return 0;
}

// also, from bottom to top
int dtblk_wr_unlock(struct myISXlockStruct* lock, int id){
    int quetop = 0;
    int* par_que = (int*)malloc(64*sizeof(int));
    memset(par_que, 0, 64*sizeof(int));
    int par = id;
    while(par > 1){
        par /= 2;
        par_que[quetop] = par;
        quetop += 1;
    };
    isxlock_X_unlock(lock + id);
    int j;
    for (j = 0; j < quetop; ++j)
        isxlock_IX_unlock(lock + par_que[j]);
    free(par_que);
    return 0;
}

// lock, from right to left
int rdlock_blklock_list(struct myISXlockStruct* lock, int *lockids){
    int j;
    int id;
    int lockopret = 0;
    for(j = 0; j < 128; ++j){
        id = lockids[j];
        if (id == 0) break;
        lockopret = dtblk_rd_lock(lock, id);
        if (lockopret < 0) {
            int j1;
            for (j1 = j-1; j1 >= 0; j1--){
                id = lockids[j1];
                dtblk_rd_unlock(lock, id);
            }
            return lockopret;
        }
    }
    return 0;
}

// unlock, from left to right
int rdUnlock_blklock_list(struct myISXlockStruct* lock, int *lockids){
    int j;
    int id;
    for(j = 127; j >= 0; --j){
        id = lockids[j];
        if (id == 0) continue;
        dtblk_rd_unlock(lock, id);
    }
    return 0;
}

// lock, from right to left
int wrlock_blklock_list(struct myISXlockStruct* lock, int *lockids){
    int j;
    int id;
    int lockopret = 0;
    for(j = 0; j < 128; ++j){
        id = lockids[j];
        if (id == 0) break;
        lockopret = dtblk_wr_lock(lock, id);
        if (lockopret < 0) {
            int j1;
            for (j1 = j-1; j1 >= 0; j1--){
                id = lockids[j1];
                dtblk_wr_unlock(lock, id);
            }
            return lockopret;
        }
    }
    return 0;
}

// unlock, from left to right
int wrUnlock_blklock_list(struct myISXlockStruct* lock, int *lockids){
    int j;
    int id;
    for(j = 127; j >= 0; --j){
        id = lockids[j];
        if (id == 0) continue;
        dtblk_wr_unlock(lock, id);
    }
    return 0;
} 

// This will display a list of lock ids
void show_lockid_list(int* locks){
    int j = 0;
    fprintf(stdout, "\n");
    for (j = 0; j < 128; ++j){
        if (locks[j] == 0) continue;
        fprintf(stdout, "%d, ", locks[j]);
        waitMicroSec(12);
    }
    fprintf(stdout, "\n\n");
}

#endif 
