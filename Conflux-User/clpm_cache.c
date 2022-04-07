#ifndef __CLPM_CACHE
#define __CLPM_CACHE


#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <libpmem.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "dcache_rdmaconx.c"


char * LPCACHE_POOLNAME;
const uint64_t REMOTE_PMEM_LEN = (uint64_t)(1 << 30) * 64;
const uint64_t LOCAL_PCACHE_LEN = (uint64_t)(1 << 30) * 17;
const uint64_t LOCAL_PCACHE_BEG = (uint64_t)(1 << 30);
const uint64_t PCACHE_CHUNK_LEN = (1 << 20);
const uint64_t MAXN_CHUNK = ((uint64_t)(1 << 30) * 16) / (1 << 20);

// in-system constant value copied from Conflux-Center Parts 
const int COPY_MAX_INODE = (1 << 20);
const int COPY_MAX_NOFILE = 1000000;
const int COPY_QXDFS_FDBASE = 12345678;

const uint64_t PCACHE_METABEG = (1 << 20) * 999;
const uint64_t PCACHE_METALEN = (1 << 20) * 4;
const uint64_t CHUNKS_ADDRPTR = (1 << 20) * 1000;
const uint64_t CHUNKS_INDXPTR = (1 << 20) * 1001;
const int LENRPC_RESLIST = 122;
const int PROCLEASE_MAX = (1 << 15);
const int PROCLEASE_MIN = (1 << 7);

const char* LCACHE_SHMLOCK_NAME = "QZLSHM_LCACHELOCK";
const char* LCACHE_SHMMETA_NAME = "QZLSHM_LCACHEDATA";


struct one_lease{
    uint64_t start_off;
    uint64_t end_off;
    void* addr_list; // convert this to uint64_t arr is safe
};


struct lease_entry_periid{
    int lease_cnt;
    int host_machineID;
    uint64_t address_occupied;
    struct one_lease* lease_list;
};


struct lcache_logger{
    int *global_time; // init with 0
    int *num_chunk; // num of filled chunks
    int* time_arr; // size is MAXN_CHUNK
    int* prior_arr; // MIN-heap, i'th element is idx of the i'th least time in time_arr
    int* prior_byidx; // i'th element is the rank (for least) of time_arr[i] 
    uint64_t* addr_arr; // mapping local chunks to center's addr, size MAXN_CHUNK*8
    int* indx_arr; // inverse of addr_arr, size is (REMOTE_PMEM_LEN / PCACHE_CHUNK_LEN)*4
    char *pcache; // all meta and data of local cache
    
    // char *vcache; // only data for local cache

    char* lock_shm;
    char* meta_shm;
    pthread_mutex_t* lock;

    // For integration, we create aiagent-qdata-shm in lcache initialization
    int* aipart_stats;

    // For availability, add a user-proc-num, related to (lock_shm + 64)
    int user_pn;

    // Construct lease arr here
    int* fd2iid_log;
    struct lease_entry_periid* lease_groups_read;
    struct lease_entry_periid* lease_groups_write;
    // Note this is buffer fot return values of search_lease
    int* buffor_search_lease;
    int* search_table_leaseR;
    int* search_table_leaseW;
};


void mymem_flush(const void *p, size_t mem_size){
    static const size_t cache_line = 64;
    const char *cp = (const char *)p;
    size_t i = 0;

    if (p == NULL || mem_size <= 0)
        return;

    for (i = 0; i < mem_size; i += cache_line) {
        asm volatile("clflush (%0)\n\t"
            : 
            : "r"(&cp[i])
            : "memory");
    }
    asm volatile("sfence\n\t"
        :
        :
        : "memory");
    return;
}


const int lease_hashtsize = (1 << 20);
uint32_t hash_value_iidoff(int iid, uint64_t offset){
    uint64_t v = (uint64_t)iid;
    offset = (offset << 24) >> 24;
    v = (v << 42) ^ offset;
    v = v ^ (v << 19);
    v = v ^ (v >> 41);
    v = v ^ (v << 21);
    v = (v >> 12) << 12;
    uint32_t ret = (uint32_t)((v << 32) >> 44);
    return ret;
}


// The PCSTAT shared memory now maintains across-proc status: 
// MAXCL_THNUM*0 -- MAXCL_THNUM*1 are running(0,1) status
// MAXCL_THNUM*1 -- MAXCL_THNUM*2 are io-counts
// MAXCL_THNUM*2 -- MAXCL_THNUM*3 are cur pmem-read iosize
// MAXCL_THNUM*3 -- MAXCL_THNUM*4 are cur pmem-write iosize
// MAXCL_THNUM*4 -- MAXCL_THNUM*5 are cur rdma-read iosize
// MAXCL_THNUM*5 -- MAXCL_THNUM*6 are cur rdma-write iosize

int* statinit_allproc(int isfirst){
    int pcstat_shmfd;
    if (isfirst)
        pcstat_shmfd = shm_open("QZLSHM_PCSTAT", O_RDWR | O_CREAT, 0666);
    else 
        pcstat_shmfd = shm_open("QZLSHM_PCSTAT", O_RDWR, 0666);
    // fprintf(stderr, "RUN_INFO: pcstat_shmfd is %d.\n", pcstat_shmfd);
    if (isfirst) 
        ftruncate(pcstat_shmfd, sizeof(int) * MAXCL_THNUM * 6);
    int* shared_stats = (int*)mmap(NULL, sizeof(int) * MAXCL_THNUM * 6, PROT_WRITE, MAP_SHARED, pcstat_shmfd, 0);
    if (isfirst) 
        memset(shared_stats, 0, sizeof(int) * MAXCL_THNUM * 6);
    close(pcstat_shmfd);
    return shared_stats;
}



// Note that we DO NOT need to sync vcache(in DRAM) with pcache(on PMEM)
// But addr_arr and indx_arr shoule be changed both in v and nv space at a time


struct lcache_logger* create_pcache(int init_flag){
    sizeof(struct lease_entry_periid);
    sizeof(struct one_lease);
    int lock_fd = shm_open(LCACHE_SHMLOCK_NAME, O_RDWR | O_CREAT, 0666);
    if (lock_fd <= 0) {
        fprintf(stderr, "[ERR]: lcache lock file creation failed.\n");
        return NULL;
    }
    ftruncate(lock_fd, 128 + sizeof(pthread_mutex_t));

    int meta_fd = shm_open(LCACHE_SHMMETA_NAME, O_RDWR | O_CREAT, 0666);
    if (meta_fd <= 0) {
        fprintf(stderr, "[ERR]: lcache meta file creation failed.\n");
        return NULL;
    }
    ftruncate(meta_fd, PCACHE_METALEN);

    int poolfd = open(LPCACHE_POOLNAME, O_RDWR);
    if (poolfd <= 0) {
        fprintf(stderr, "[ERR]: no lpcache pool file.\n");
        return NULL;
    }

    struct lcache_logger* ret = (struct lcache_logger*)malloc(sizeof(struct lcache_logger));
    ret->lock_shm = (char*)mmap(NULL, 128 + sizeof(pthread_mutex_t), PROT_WRITE, MAP_SHARED, lock_fd, 0);
    ret->meta_shm = (char*)mmap(NULL, PCACHE_METALEN, PROT_WRITE, MAP_SHARED, meta_fd, 0);
    ret->pcache = (char*)mmap(NULL, LOCAL_PCACHE_LEN, PROT_WRITE, MAP_SHARED, poolfd, 0);

    // A NOT-perfect lock-shm init process

    int pid = syscall(__NR_gettid);
    int fflag = 777;
    pthread_mutexattr_t mutex_attr;
    pthread_mutexattr_init(&mutex_attr);
    pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED);
    while (1){
        if (memcmp(&fflag, ret->lock_shm, sizeof(int)) == 0){
            fprintf(stderr, "DEBUG_INFO: lpcache lock-shm has been set by others.\n");
            init_flag = 0;
            ret->lock = (pthread_mutex_t*)((ret->lock_shm) + 128);
            break;
        }
        memcpy(ret->lock_shm, &pid, sizeof(int));
        mymem_flush(ret->lock_shm, sizeof(int));
        ret->lock = (pthread_mutex_t*)((ret->lock_shm) + 128);
        memset(ret->lock, 0, sizeof(pthread_mutex_t));
        pthread_mutex_init(ret->lock, &mutex_attr);
        if (memcmp(&pid, ret->lock_shm, sizeof(int)) == 0){
            memcpy(ret->lock_shm, &fflag, sizeof(int));
            mymem_flush(ret->lock_shm, sizeof(int));
            fprintf(stderr, "DEBUG_INFO: lpcache lock-shm has been set by %d.\n", pid);
            break;
        }
        waitMicroSec(pid%31);
    }

    // pointer set in mata_shm
    ret->global_time = (int*)(ret->meta_shm+8);
    ret->num_chunk = (int*)(ret->meta_shm+16);
    ret->time_arr = (int*)(ret->meta_shm+(1<<12));
    ret->prior_arr = (int*)(ret->meta_shm+(1<<19));
    ret->addr_arr = (uint64_t*)(ret->meta_shm+(1<<20));
    ret->indx_arr = (int*)(ret->meta_shm+(1<<21));

    // init all lcache meta data
    pthread_mutex_lock(ret->lock);

    ret->user_pn = -1;

    if (init_flag == 1){
        // First set user_pn (lock_shm+64) as 0
        memset(ret->lock_shm + 64, 0, sizeof(int));
        mymem_flush(ret->lock_shm + 64, sizeof(int));
        ret->user_pn = 0;
        
        fprintf(stderr, "DEBUG_INFO: Zero set pool file.\n");
        pmem_memset(ret->pcache + PCACHE_METABEG, 0, PCACHE_METALEN, PMEM_F_MEM_NONTEMPORAL);
        *(ret->num_chunk) = 0;
        memset(ret->addr_arr, 0, sizeof(uint64_t) * MAXN_CHUNK);
        memset(ret->indx_arr, 0, sizeof(int) * (REMOTE_PMEM_LEN / PCACHE_CHUNK_LEN));

        ret->aipart_stats = statinit_allproc(1);
    }
    else{
        // Get user_pn (lock_shm+64)
        int cur_pn = *(int*)(ret->lock_shm + 64);
        *(int*)(ret->lock_shm + 64) = cur_pn + 1;
        mymem_flush(ret->lock_shm + 64, sizeof(int));
        ret->user_pn = cur_pn + 1;
        
        fprintf(stderr, "DEBUG_INFO: Reload pool file.\n");
        memcpy(ret->addr_arr, ret->pcache + CHUNKS_ADDRPTR, sizeof(uint64_t) * MAXN_CHUNK);
        memcpy(ret->indx_arr, ret->pcache + CHUNKS_INDXPTR, sizeof(int) * (REMOTE_PMEM_LEN / PCACHE_CHUNK_LEN));
        int i;
        int n1 = 0;
        for (i = 0; i < MAXN_CHUNK; ++i){
            if (ret->addr_arr[i] != 0) n1 += 1;
        }
        int n2 = 0;
        for (i = 0; i < (REMOTE_PMEM_LEN / PCACHE_CHUNK_LEN); ++i){
            if(ret->indx_arr[i] != 0) n2 += 1;
        }
        if (n1 != n2) fprintf(stderr, "[ERR]: inconsistent addr and indx for LCache.\n");
        *(ret->num_chunk) = n1;

        ret->aipart_stats = statinit_allproc(0);
    }

    *(ret->global_time) = 0;
    int i = 0;
    // at init, the times and prior can be roughly same as before
    memset(ret->time_arr, 0, sizeof(int) * MAXN_CHUNK);
    memset(ret->prior_arr, 0, sizeof(int) * MAXN_CHUNK);
    ret->prior_byidx = (int*)malloc(sizeof(int)*MAXN_CHUNK);
    memset(ret->prior_byidx, 0, sizeof(int) * MAXN_CHUNK);
    for (i = 0; i < *(ret->num_chunk); ++i){
        *(ret->global_time) += 1;
        ret->time_arr[i] = *(ret->global_time);
        ret->prior_arr[i] = i;
        ret->prior_byidx[i] = i;
    }
    pthread_mutex_unlock(ret->lock);

    // ret->vcache = (char*)malloc(LOCAL_PCACHE_LEN - LOCAL_PCACHE_BEG);
    // memset(ret->vcache, 0, LOCAL_PCACHE_LEN - LOCAL_PCACHE_BEG);
    close(poolfd);
    if (ret->user_pn < 0) {
        fprintf(stderr, "[ERR]: Set lcache user_pn Failed!\n");
        return NULL;
    }

    // To construct some useful lease structs
    ret->fd2iid_log = (int*)malloc(sizeof(int) * COPY_MAX_NOFILE);
    memset(ret->fd2iid_log, 0, sizeof(int) * COPY_MAX_NOFILE);

    ret->lease_groups_read = (struct lease_entry_periid*)malloc(sizeof(struct lease_entry_periid)*COPY_MAX_INODE);
    memset(ret->lease_groups_read, 0, sizeof(struct lease_entry_periid)*COPY_MAX_INODE);
    ret->lease_groups_write = (struct lease_entry_periid*)malloc(sizeof(struct lease_entry_periid)*COPY_MAX_INODE);
    memset(ret->lease_groups_write, 0, sizeof(struct lease_entry_periid)*COPY_MAX_INODE);

    ret->buffor_search_lease = (int*)malloc((PROCLEASE_MAX + 2) * sizeof(int));
    memset(ret->buffor_search_lease, 0, (PROCLEASE_MAX + 2) * sizeof(int));

    ret->search_table_leaseR = (int*)malloc(lease_hashtsize * sizeof(int));
    memset(ret->search_table_leaseR, 255, lease_hashtsize * sizeof(int));
    if (ret->search_table_leaseR[0] != -1){
        perror("\n\n[ERR] NO WAY, memset -1 err!\n\n\n");
        exit(1);
    }
    ret->search_table_leaseW = (int*)malloc(lease_hashtsize * sizeof(int));
    memset(ret->search_table_leaseW, 255, lease_hashtsize * sizeof(int));

    return ret;
}


int free_local_cache(struct lcache_logger* lcache){
    free(lcache->buffor_search_lease);
    free(lcache->lease_groups_read);
    free(lcache->lease_groups_write);
    fprintf(stdout, "DEBUG_INFO: the lcache used %d chunks in total.\n", *(lcache->num_chunk));
    free(lcache->fd2iid_log);
    shm_unlink(LCACHE_SHMMETA_NAME);
    shm_unlink(LCACHE_SHMLOCK_NAME);
    free(lcache->prior_byidx);
    munmap(lcache->pcache, LOCAL_PCACHE_LEN);
    return 0;
}


// rw_bit: 0 as read and 1 as write
int hash_search_lease(struct lcache_logger* lcache, int rw_bit, int iid, uint64_t offset){
    struct lease_entry_periid* cur_lease_entry = lcache->lease_groups_read;
    if (rw_bit) cur_lease_entry = lcache->lease_groups_write;
    int* stored_slotpos = lcache->search_table_leaseR;
    if (rw_bit) stored_slotpos = lcache->search_table_leaseW;
    cur_lease_entry = cur_lease_entry + iid;
    if (cur_lease_entry->lease_list == NULL) return -7;
    uint32_t prob_ptr = hash_value_iidoff(iid, offset);
    int prob_pos = stored_slotpos[prob_ptr];
    if (prob_pos < 0 || cur_lease_entry->lease_cnt <= prob_pos) return -1;
    struct one_lease* spec_lease = cur_lease_entry->lease_list + prob_pos;
    if (spec_lease->start_off == offset) return prob_pos;
    else return -1;
}


// rw_bit: 0 as read and 1 as write
int hash_insert_lease(struct lcache_logger* lcache, int rw_bit, int iid, uint64_t offset, int actual_pos){
    int* stored_slotpos = lcache->search_table_leaseR;
    if (rw_bit) stored_slotpos = lcache->search_table_leaseW;
    uint32_t prob_ptr = hash_value_iidoff(iid, offset);
    stored_slotpos[prob_ptr] = actual_pos;
    return 0;
}


// Be careful that rw_flag
// read-query_R:1,
// read-query_W:2,
// write-query_R:3, 
// write-query_W:4.
int search_lease(struct lcache_logger* lcache, int rw_flag, int iid, uint64_t start, uint64_t end){
    int tobe_evict;
    struct lease_entry_periid* cur_lease_entry = NULL;
    if (rw_flag == 1 || rw_flag == 3) 
        cur_lease_entry = lcache->lease_groups_read + iid;
    if (rw_flag == 2 || rw_flag == 4) 
        cur_lease_entry = lcache->lease_groups_write + iid;
    if (cur_lease_entry->lease_list == NULL){
        cur_lease_entry->lease_list = (struct one_lease*)malloc(sizeof(struct one_lease)*PROCLEASE_MIN);
        memset(cur_lease_entry->lease_list, 0, sizeof(struct one_lease)*PROCLEASE_MIN);
        cur_lease_entry->lease_cnt = 0;
        cur_lease_entry->address_occupied = 0;
    }
    int i;
    int cnt_leases = cur_lease_entry->lease_cnt;
    struct one_lease* all_leases = cur_lease_entry->lease_list;
    tobe_evict = 0;

    uint64_t dirty_write_B = 0;
    uint64_t dirty_write_MAX = (uint64_t)(1<<20) * 10240;
    uint64_t dirty_write_MIN = (uint64_t)(1<<20) * 64;

    if (rw_flag == 1){
        int fast_prob = hash_search_lease(lcache, 0, iid, start);
        if (fast_prob >= 0) {
            lcache->buffor_search_lease[0] = 0;
            lcache->buffor_search_lease[1] = fast_prob;
            return 0;
        }
    }

    if (rw_flag == 3){
        int fast_prob = hash_search_lease(lcache, 0, iid, start);
        if (fast_prob >= 0) {
            lcache->buffor_search_lease[0] = -1;
            lcache->buffor_search_lease[1] = cur_lease_entry->lease_cnt - 1;
            lcache->buffor_search_lease[2] = fast_prob;
            return 0;
        }
    }

    if (rw_flag == 4 || rw_flag == 2){
        dirty_write_B = cur_lease_entry->address_occupied;
        if (dirty_write_B > dirty_write_MAX){
            tobe_evict = 0;
            while (dirty_write_B > dirty_write_MIN){
                lcache->buffor_search_lease[2 + tobe_evict] = tobe_evict;
                dirty_write_B = dirty_write_B - (all_leases[tobe_evict].end_off - all_leases[tobe_evict].start_off);
                tobe_evict += 1;
            }
            lcache->buffor_search_lease[0] = 0 - tobe_evict;
            lcache->buffor_search_lease[1] = cur_lease_entry->lease_cnt - tobe_evict;
            return 0;
        }
        int fast_prob = hash_search_lease(lcache, 1, iid, start);
        if (fast_prob >= 0) {
            lcache->buffor_search_lease[0] = 0;
            lcache->buffor_search_lease[1] = fast_prob;
            return 0;
        }
    }

    for (i = 0; i < cnt_leases; ++i){
        if (rw_flag == 4){
            dirty_write_B = dirty_write_B + (all_leases[i].end_off - all_leases[i].start_off);
        }
        // note here "==" is a bad condition, we can make it wider and add new addr calculation function
        if (all_leases[i].start_off == start && all_leases[i].end_off == end){
            if (rw_flag == 1 || rw_flag == 4){
                lcache->buffor_search_lease[0] = 0;
                lcache->buffor_search_lease[1] = i;
                return 0;
            }
            else{
                lcache->buffor_search_lease[2 + tobe_evict] = i;
                tobe_evict += 1;
                break;
            }
        }
        if (all_leases[i].start_off >= end || all_leases[i].end_off <= start) 
            continue;
        lcache->buffor_search_lease[2 + tobe_evict] = i;
        tobe_evict += 1;
    }

    if ((tobe_evict == 0 && cur_lease_entry->lease_cnt < PROCLEASE_MAX) && dirty_write_B <= dirty_write_MAX){
        lcache->buffor_search_lease[0] = 1;
        lcache->buffor_search_lease[1] = cur_lease_entry->lease_cnt;
        return 0;
    }
    if (tobe_evict > 0) {
        lcache->buffor_search_lease[0] = 0 - tobe_evict;
        lcache->buffor_search_lease[1] = cur_lease_entry->lease_cnt - tobe_evict;
        return 0;
    }
    if (cur_lease_entry->lease_cnt >= PROCLEASE_MAX) {
        tobe_evict = PROCLEASE_MAX / 2;
        for (i = 0; i < tobe_evict; ++i) lcache->buffor_search_lease[2 + i] = i;
        lcache->buffor_search_lease[0] = 0 - tobe_evict;
        lcache->buffor_search_lease[1] = cur_lease_entry->lease_cnt - tobe_evict;
        return 0;
    }
    if (dirty_write_B > dirty_write_MAX){
        tobe_evict = 0;
        while (dirty_write_B > dirty_write_MIN){
            lcache->buffor_search_lease[2 + tobe_evict] = tobe_evict;
            dirty_write_B = dirty_write_B - (all_leases[tobe_evict].end_off - all_leases[tobe_evict].start_off);
            tobe_evict += 1;
        }
        lcache->buffor_search_lease[0] = 0 - tobe_evict;
        lcache->buffor_search_lease[1] = cur_lease_entry->lease_cnt - tobe_evict;
    }
    return 0;
}


// Be careful that rw_flag (R:0, W:1) should be correct for lease_groups
int add_one_lease(struct lcache_logger* lcache, int rw_flag, int iid, struct one_lease* the_lease){
    struct lease_entry_periid* cur_lease_entry = NULL;
    if (rw_flag == 0) 
        cur_lease_entry = lcache->lease_groups_read + iid;
    if (rw_flag == 1) 
        cur_lease_entry = lcache->lease_groups_write + iid;
    if (cur_lease_entry->lease_list == NULL){
        cur_lease_entry->lease_list = (struct one_lease*)malloc(sizeof(struct one_lease)*PROCLEASE_MIN);
        memset(cur_lease_entry->lease_list, 0, sizeof(struct one_lease)*PROCLEASE_MIN);
        cur_lease_entry->lease_cnt = 0;
        cur_lease_entry->address_occupied = 0;
    }
    if (cur_lease_entry->lease_cnt != lcache->buffor_search_lease[1])
        fprintf(stderr, "[WARN]: lease_cnt not correctly settled.\n");
    cur_lease_entry->address_occupied += (the_lease->end_off - the_lease->start_off);

    if (cur_lease_entry->lease_cnt == PROCLEASE_MIN){
        struct one_lease* new_leases = (struct one_lease*)malloc(sizeof(struct one_lease)*PROCLEASE_MAX);
        memset(new_leases, 0, sizeof(struct one_lease)*PROCLEASE_MIN);
        memcpy(new_leases, cur_lease_entry->lease_list, cur_lease_entry->lease_cnt * sizeof(struct one_lease));
        free(cur_lease_entry->lease_list);
        cur_lease_entry->lease_list = new_leases;
    }

    memcpy(cur_lease_entry->lease_list + cur_lease_entry->lease_cnt, the_lease, sizeof(struct one_lease));

    hash_insert_lease(lcache, rw_flag, iid, the_lease->start_off, cur_lease_entry->lease_cnt);

    cur_lease_entry->lease_cnt += 1;
    return 0;
}


int adjust_cache_prior(struct lcache_logger* lcache, int pos){
    // similar but need to adjust the min-heap, i.e. prior_arr
    *(lcache->global_time) += 1;
    lcache->time_arr[pos] = *(lcache->global_time);
    if (1){ // maintain the min-heap when increasing root's time-stamp
        int tmp, l, r, m;
        int ptr = lcache->prior_byidx[pos];
        while (ptr < MAXN_CHUNK){
            m = ptr;
            l = ptr * 2 + 1;
            if (l >= MAXN_CHUNK) break;
            if (lcache->time_arr[lcache->prior_arr[l]] < lcache->time_arr[lcache->prior_arr[m]]) m = l;
            r = ptr * 2 + 2;
            if (r >= MAXN_CHUNK) break;
            if (lcache->time_arr[lcache->prior_arr[r]] < lcache->time_arr[lcache->prior_arr[m]]) m = r;
            if (m != ptr){
                tmp = lcache->prior_arr[ptr];
                lcache->prior_arr[ptr] = lcache->prior_arr[m];
                lcache->prior_arr[m] = tmp;
                lcache->prior_byidx[lcache->prior_arr[m]] = m;
                lcache->prior_byidx[lcache->prior_arr[ptr]] = ptr;
            }
            else 
                break;
            ptr = m;
        }
        // min-heap maintained
    }
    return 0;
}


int64_t search_lcache(struct lcache_logger* lcache, uint64_t real_addr){
    uint64_t chunk_begaddr = real_addr - (real_addr % PCACHE_CHUNK_LEN);
    int chunk_id = (int)(chunk_begaddr / PCACHE_CHUNK_LEN);
    int chunk_pos = lcache->indx_arr[chunk_id] - 1;
    if (chunk_pos < 0) return -1;
    // pthread_mutex_lock(lcache->lock);
    // adjust_cache_prior(lcache, chunk_pos);
    // pthread_mutex_unlock(lcache->lock);
    uint64_t incache_addr = PCACHE_CHUNK_LEN * chunk_pos + (real_addr % PCACHE_CHUNK_LEN);
    return (int64_t)incache_addr;
}



// Note that we DO NOT need to sync vcache(in DRAM) with pcache(on PMEM)
// But addr_arr and indx_arr shoule be changed both in v and nv space at a time

struct arg_insert_lcache{
    struct lcache_logger* lcache;
    uint64_t chunk_beg;
    struct ibdev_resc* resc;
    struct ib_conx_info** conx_info_local;
    int main_mcn;
};

void* insert_lcache(void* targ){

    struct arg_insert_lcache* arg = (struct arg_insert_lcache*)targ;
    struct lcache_logger* lcache = arg->lcache;
    uint64_t chunk_beg = arg->chunk_beg;
    struct ibdev_resc* resc = arg->resc;
    struct ib_conx_info** conx_info_local = arg->conx_info_local;
    int main_mcn = arg->main_mcn;

    int ins_pos;

    pthread_mutex_lock(lcache->lock);
    if (*(lcache->num_chunk) < MAXN_CHUNK){
        ins_pos = *(lcache->num_chunk);
        *(lcache->num_chunk) += 1;
        post_read(resc, conx_info_local, main_mcn, -1, PCACHE_CHUNK_LEN, LOCAL_PCACHE_BEG+PCACHE_CHUNK_LEN*ins_pos, chunk_beg, 0, 0);
        ibpoll_completion(resc->procState, resc->cq_rw[main_mcn-1], CQE_RT_TIMEOUT, 1, 1);
        // pmem_memcpy(lcache->pcache + LOCAL_PCACHE_BEG + PCACHE_CHUNK_LEN * ins_pos, 
        //     lcache->vcache + PCACHE_CHUNK_LEN * ins_pos, PCACHE_CHUNK_LEN, PMEM_F_MEM_NONTEMPORAL);
        *(lcache->global_time) += 1;
        lcache->time_arr[ins_pos] = *(lcache->global_time);
        lcache->prior_arr[ins_pos] = ins_pos; // no need to adjust the min-heap here
        lcache->prior_byidx[ins_pos] = ins_pos;
        lcache->addr_arr[ins_pos] = chunk_beg;
        int indx_id = (int)(chunk_beg / PCACHE_CHUNK_LEN);
        int indx = ins_pos + 1;
        lcache->indx_arr[indx_id] = indx;
        pthread_mutex_unlock(lcache->lock);
        pmem_memcpy(lcache->pcache + CHUNKS_ADDRPTR + 8*ins_pos, &(chunk_beg), 8, PMEM_F_MEM_TEMPORAL);
        pmem_memcpy(lcache->pcache + CHUNKS_INDXPTR + 4*indx_id, &(indx), 4, PMEM_F_MEM_TEMPORAL);
        asm volatile("sfence\n\t"::: "memory");
    }
    else{
        ins_pos = lcache->prior_arr[0];
        int old_indx_id = (int)(lcache->addr_arr[ins_pos] / PCACHE_CHUNK_LEN);
        lcache->indx_arr[old_indx_id] = 0;
        lcache->addr_arr[ins_pos] = chunk_beg;
        int indx_id = (int)(chunk_beg / PCACHE_CHUNK_LEN);
        int indx = ins_pos + 1;
        lcache->indx_arr[indx_id] = indx;
        pmem_memcpy(lcache->pcache + CHUNKS_ADDRPTR + 8*ins_pos, &(chunk_beg), 8, PMEM_F_MEM_TEMPORAL);
        pmem_memcpy(lcache->pcache + CHUNKS_INDXPTR + 4*indx_id, &(indx), 4, PMEM_F_MEM_TEMPORAL);
        asm volatile("sfence\n\t"::: "memory");
        adjust_cache_prior(lcache, ins_pos);
        pthread_mutex_unlock(lcache->lock);
        post_read(resc, conx_info_local, main_mcn, -1, PCACHE_CHUNK_LEN, LOCAL_PCACHE_BEG+PCACHE_CHUNK_LEN*ins_pos, chunk_beg, 0, 0);
        ibpoll_completion(resc->procState, resc->cq_rw[main_mcn-1], CQE_RT_TIMEOUT, 1, 1);
        // pmem_memcpy(lcache->pcache + LOCAL_PCACHE_BEG + PCACHE_CHUNK_LEN * ins_pos, 
        //     lcache->vcache + PCACHE_CHUNK_LEN * ins_pos, PCACHE_CHUNK_LEN, PMEM_F_MEM_NONTEMPORAL);
    }

    return NULL;
}



struct arg_update_redata{
    struct lcache_logger* lcache;
    struct ibdev_resc* resc;
    struct ib_conx_info** conx_info_local;
    int server_id;
    uint64_t beg;
    uint64_t len;
    uint64_t app_addr;
    uint32_t app_lkey;
};

void* update_redata(void* targ){
    struct arg_update_redata* arg = (struct arg_update_redata*)targ;

    struct lcache_logger* lcache = arg->lcache;
    struct ibdev_resc* resc = arg->resc;
    struct ib_conx_info** conx_info_local = arg->conx_info_local;
    int server_id = arg->server_id;
    uint64_t beg = arg->beg;
    uint64_t len = arg->len;
    uint64_t app_addr = arg->app_addr;
    uint32_t app_lkey = arg->app_lkey;

    uint64_t end_addr = beg - (beg % PCACHE_CHUNK_LEN) + PCACHE_CHUNK_LEN;
    uint64_t beg_addr = beg;
    while (1){
        if (end_addr >= beg + len){
            end_addr = beg + len;
            if (app_addr)
                post_write(resc, conx_info_local, server_id, -1, end_addr - beg_addr, 0, beg_addr, app_addr, app_lkey);
            else{
                int64_t loff = search_lcache(lcache, beg_addr);
                post_write(resc, conx_info_local, server_id, -1, end_addr - beg_addr, LOCAL_PCACHE_BEG+loff, beg_addr, 0, 0);
            }
            ibpoll_completion(resc->procState, resc->cq_rw[server_id-1], CQE_RT_TIMEOUT, 1, 1);
            break;
        }
        if (app_addr)
            post_write(resc, conx_info_local, server_id, -1, end_addr - beg_addr, 0, beg_addr, app_addr, app_lkey);
        else{
            int64_t loff = search_lcache(lcache, beg_addr);
            post_write(resc, conx_info_local, server_id, -1, end_addr - beg_addr, LOCAL_PCACHE_BEG+loff, beg_addr, 0, 0);
        }
        ibpoll_completion(resc->procState, resc->cq_rw[server_id-1], CQE_RT_TIMEOUT, 1, 1);
        if (app_addr) app_addr += (end_addr - beg_addr);
        beg_addr = end_addr;
        end_addr += PCACHE_CHUNK_LEN;
    }
    return NULL;
}



struct rpc_query{
    int op_ordr;
    int op_name; // 1 as pread, 2 as pwrite, 3 as open, 4 as close, ...
    char* query_cont;
};


struct rpc_respo{
    int op_ordr;
    int op_name;
    int servr_mcnid;
    int posib_iid;
    char* respo_cont;
};


struct arg_thexe_memcpy{
    char* dest;
    char* src;
    uint64_t nbytes;
    uint8_t nv_flag; // 0 for dram, 1 for pmem; describe dest
};


void* thexe_memcpy(void* targ){
    struct arg_thexe_memcpy* arg = (struct arg_thexe_memcpy*)targ;
    char* d = arg->dest;
    char* s = arg->src;
    size_t n = (size_t)(arg->nbytes);
    uint8_t nv = arg->nv_flag;
    // if (!nv) memcpy(d, s, n);
    // else pmem_memcpy(d, s, n, PMEM_F_MEM_NONTEMPORAL); 
    return NULL;
}


struct openfile_ret{
    int ord_fd;
    int file_mode;
    uint64_t stat_size;
};


struct rpc_respo* issue_query(struct ibdev_resc* resc, int mcnid, int qlength){
    int know_opname = *((int*)((resc->rpc_buf[mcnid-1]) + 4));
    // fprintf(stderr, "opname ::0 %d\n", know_opname);

    if (ALLOW_DM_RPC && know_opname > 2){
        post_receive(resc, mcnid, -1, DM_FLAG_LENT + PRC_DM_SIZE);
    }
    else {
        post_receive(resc, mcnid, -1, RPC_MR_SIZE);
    }

    if (ALLOW_DM_RPC && qlength <= PRC_DM_SIZE){
        ibv_memcpy_to_dm(resc->dm_buf, PRC_DM_SIZE*2*(mcnid-1), resc->rpc_buf[mcnid-1], PRC_DM_SIZE);
        post_send(resc, mcnid, -1, DM_FLAG_LENT + qlength);
    }
    else {
        post_send(resc, mcnid, -1, qlength);
    }

    // fprintf(stderr, "opname ::1 %d\n", know_opname);

    int qpn;
    qpn = ibpoll_completion(resc->procState, resc->cq_send[mcnid-1], CQE_RT_TIMEOUT, 1, 1);
    if (qpn < 0){
        fprintf(stderr, "[ERR]: ib completion failed.\n");
        return NULL;
    }
    // fprintf(stderr, "opname ::1.5 %d\n", know_opname);
    // if (qpn != resc->qp_list[mcnid-1]->qp_num) 
    //     fprintf(stderr, "[WARN]: unmatched send comp.\n");
    qpn = ibpoll_completion(resc->procState, resc->cq_recv[mcnid-1], CQE_RT_TIMEOUT, 1, 1);
    if (qpn < 0){
        fprintf(stderr, "[ERR]: ib completion failed.\n");
        return NULL;
    }
    // if (qpn != resc->qp_list[mcnid-1]->qp_num) 
    //     fprintf(stderr, "[WARN]: unmatched recv comp.\n");
    // fprintf(stdout, "RUN_INFO: get response from %d\n", mcnid);
    // fprintf(stderr, "opname ::2 %d\n", know_opname);

    char *res_addr = (resc->rpc_buf[mcnid-1]) + RPC_MR_SIZE;
    if (ALLOW_DM_RPC && know_opname > 2){
        ibv_memcpy_from_dm(res_addr, resc->dm_buf, PRC_DM_SIZE*2*(mcnid-1) + PRC_DM_SIZE, PRC_DM_SIZE);
    }

    // fprintf(stderr, "opname ::3 %d\n", know_opname);
    struct rpc_respo* respo = (struct rpc_respo*)malloc(sizeof(struct rpc_respo));
    memset(respo, 0, sizeof(struct rpc_respo));
    respo->op_ordr = *((int*)(res_addr+0));
    respo->op_name = *((int*)(res_addr+4));
    respo->servr_mcnid = mcnid;

    if (respo->op_name == 1){
        // this is read query
        respo->posib_iid = *(int*)(res_addr + 8);
        respo->respo_cont = (char*)malloc(RPC_MR_SIZE);
        memcpy(respo->respo_cont, res_addr + 16, sizeof(uint64_t)*LENRPC_RESLIST);
        // check if lease should be removed caused by reconstruct
        if (*(int*)(res_addr + 12) == -1){
            *(uint64_t*)(respo->respo_cont + sizeof(uint64_t)*LENRPC_RESLIST) = 0xffffffffffff;
        }
    }
    else if (respo->op_name == 2){
        // this is write query
        respo->posib_iid = *(int*)(res_addr + 8);
        respo->respo_cont = (char*)malloc(RPC_MR_SIZE);
        memcpy(respo->respo_cont, res_addr + 16, sizeof(uint64_t)*LENRPC_RESLIST);
        // check if lease should be removed caused by reconstruct
        if (*(int*)(res_addr + 12) == -1){
            *(uint64_t*)(respo->respo_cont + sizeof(uint64_t)*LENRPC_RESLIST) = 0xffffffffffff;
        }
    }
    else if (respo->op_name == 3){
        // this is open query
        respo->respo_cont = (char*)malloc(64);
        memcpy(respo->respo_cont, res_addr + 8, sizeof(struct openfile_ret));
    }
    else if (respo->op_name == 4){
        // this is close query
        respo->respo_cont = (char*)malloc(64);
        memcpy(respo->respo_cont, res_addr + 8, sizeof(int));
    }
    else if (respo->op_name == 5){
        // this is unlock query
        respo->respo_cont = (char*)malloc(64);
        memcpy(respo->respo_cont, res_addr + 8, sizeof(int));
    }
    else if (respo->op_name == 6){
        // this async operation has been moved out
        fprintf(stderr, "[WARN]: using wrong function for lease query.\n");
    }
    else if (respo->op_name == 101){
        // this is mkdir query
        respo->respo_cont = (char*)malloc(64);
        memcpy(respo->respo_cont, res_addr + 8, sizeof(int));
    }
    else if (respo->op_name == 102){
        // this is getlen query
        respo->respo_cont = (char*)malloc(64);
        memcpy(respo->respo_cont, res_addr + 8, sizeof(uint64_t));
    }
    else if (respo->op_name == 103){
        // this is getlen query
        respo->respo_cont = (char*)malloc(64);
        memcpy(respo->respo_cont, res_addr + 8, sizeof(int));
    }
    else if (respo->op_name == 104){
        // this is stat query
        respo->respo_cont = (char*)malloc(64);
        memcpy(respo->respo_cont, res_addr + 8, sizeof(struct openfile_ret));
    }
    else{
        fprintf(stderr, "[ERR]: Invalid op_name %d at QDFS user.\n", respo->op_name);
        return NULL;
    }

    return respo;
}


int generate_query_open(struct ibdev_resc* resc, int mcnid, struct rpc_query* query, char* fname, int o_flag){
    char* rpc_buf = resc->rpc_buf[mcnid - 1];
    memset(rpc_buf, 0, RPC_MR_SIZE);
    query->query_cont = fname;

    // process in-param and generate query
    *((int*)(rpc_buf+0)) = query->op_ordr;
    *((int*)(rpc_buf+4)) = query->op_name;
    int fname_len = strlen(query->query_cont);
    if (fname_len >= 982) 
        fprintf(stderr, "[ERR]: filename is too long for rpc buffer.\n");
    *((int*)(rpc_buf+8)) = fname_len;
    *((int*)(rpc_buf+12)) = o_flag;
    memcpy(rpc_buf + 16, query->query_cont, fname_len);

    return (fname_len + 24);
}


int generate_query_stat(struct ibdev_resc* resc, int mcnid, struct rpc_query* query, char* fname, int o_flag){
    char* rpc_buf = resc->rpc_buf[mcnid - 1];
    memset(rpc_buf, 0, RPC_MR_SIZE);
    query->query_cont = fname;

    // process in-param and generate query
    *((int*)(rpc_buf+0)) = query->op_ordr;
    *((int*)(rpc_buf+4)) = query->op_name;
    int fname_len = strlen(query->query_cont);
    if (fname_len >= 982) 
        fprintf(stderr, "[ERR]: filename is too long for rpc buffer.\n");
    *((int*)(rpc_buf+8)) = fname_len;
    *((int*)(rpc_buf+12)) = o_flag;
    memcpy(rpc_buf + 16, query->query_cont, fname_len);

    return (fname_len + 24);
}


int generate_query_close(struct ibdev_resc* resc, int mcnid, struct rpc_query* query, int fd){
    char* rpc_buf = resc->rpc_buf[mcnid - 1];
    memset(rpc_buf, 0, RPC_MR_SIZE);

    // process in-param and generate query
    *((int*)(rpc_buf+0)) = query->op_ordr;
    *((int*)(rpc_buf+4)) = query->op_name;
    *((int*)(rpc_buf+8)) = fd;
    
    return 16;
}


int generate_query_pread(struct ibdev_resc* resc, int mcnid, struct rpc_query* query, 
                    int fd, uint64_t offset, uint64_t length){
    char* rpc_buf = resc->rpc_buf[mcnid - 1];
    memset(rpc_buf, 0, RPC_MR_SIZE);

    // process in-param and generate query
    *((int*)(rpc_buf+0)) = query->op_ordr;
    *((int*)(rpc_buf+4)) = query->op_name;
    *((int*)(rpc_buf+8)) = fd;
    *((uint64_t*)(rpc_buf+16)) = offset;
    *((uint64_t*)(rpc_buf+24)) = length;
    
    return 48;
}


int generate_query_pwrite(struct ibdev_resc* resc, int mcnid, struct rpc_query* query, 
                    int fd, uint64_t offset, uint64_t length){
    char* rpc_buf = resc->rpc_buf[mcnid - 1];
    memset(rpc_buf, 0, RPC_MR_SIZE);

    // process in-param and generate query
    *((int*)(rpc_buf+0)) = query->op_ordr;
    *((int*)(rpc_buf+4)) = query->op_name;
    *((int*)(rpc_buf+8)) = fd;
    *((uint64_t*)(rpc_buf+16)) = offset;
    *((uint64_t*)(rpc_buf+24)) = length;
    
    return 48;
}


// for rw_flag, 1 as read (unlock), 2 as write (unlock)
int generate_query_unlock(struct ibdev_resc* resc, int mcnid, struct rpc_query* query, 
                    int fd, uint8_t rw_flag, uint64_t offset, uint64_t length){
    char* rpc_buf = resc->rpc_buf[mcnid - 1];
    memset(rpc_buf, 0, RPC_MR_SIZE);

    // process in-param and generate query
    *((int*)(rpc_buf+0)) = query->op_ordr;
    *((int*)(rpc_buf+4)) = query->op_name;
    *((int*)(rpc_buf+8)) = fd;
    *((uint8_t*)(rpc_buf+12)) = rw_flag;
    *((uint64_t*)(rpc_buf+16)) = offset;
    *((uint64_t*)(rpc_buf+24)) = length;

    *((int*)(rpc_buf+32)) = -1;
    
    return 48;
}


// for asking centers' blocking leases, pretend dead wait
int generate_query_blocked(struct ibdev_resc* resc, int mcnid, struct rpc_query* query){
    char* rpc_buf = resc->rpc_buf[mcnid - 1];
    memset(rpc_buf, 0, RPC_MR_SIZE);

    // process in-param and generate query
    *((int*)(rpc_buf+0)) = query->op_ordr;
    *((int*)(rpc_buf+4)) = query->op_name;
    
    return 16;
}


int issue_query_blocked_1(struct ibdev_resc* resc, int mcnid, int qlength){
    post_receive(resc, mcnid, -1, RPC_MR_SIZE);
    post_send(resc, mcnid, -1, qlength);
    return 0;
}


struct rpc_respo* issue_query_blocked_2(struct ibdev_resc* resc, int mcnid){
    int qpn;

    qpn = ibpoll_completion(resc->procState, resc->cq_send[mcnid-1], CQE_RT_TIMEOUT, 1, 1);
    if (qpn < 0){
        fprintf(stderr, "[ERR]: ib completion failed.\n");
        return NULL;
    }

    qpn = ibpoll_completion(resc->procState, resc->cq_recv[mcnid-1], CQE_RT_TIMEOUT, 1, 1);
    if (qpn < 0){
        fprintf(stderr, "[ERR]: ib completion failed.\n");
        return NULL;
    }

    char *res_addr = (resc->rpc_buf[mcnid-1]) + RPC_MR_SIZE;
    struct rpc_respo* respo = (struct rpc_respo*)malloc(sizeof(struct rpc_respo));
    memset(respo, 0, sizeof(struct rpc_respo));
    respo->op_ordr = *((int*)(res_addr+0));
    respo->op_name = *((int*)(res_addr+4));
    respo->servr_mcnid = mcnid;

    if (respo->op_name == 6){
        // 6 is for query blocking
        respo->respo_cont = (char*)malloc(RPC_MR_SIZE);
        memcpy(respo->respo_cont, res_addr + 8, RPC_MR_SIZE - 8);
    }
    else {
        fprintf(stderr, "[ERR]: Wrong op-name at query_blocked_2.\n");
        return NULL;
    }
    return respo;
}


int generate_query_mkdir(struct ibdev_resc* resc, int mcnid, struct rpc_query* query, char* dirname){
    char* rpc_buf = resc->rpc_buf[mcnid - 1];
    memset(rpc_buf, 0, RPC_MR_SIZE);
    query->query_cont = dirname;

    // process in-param and generate query
    *((int*)(rpc_buf+0)) = query->op_ordr;
    *((int*)(rpc_buf+4)) = query->op_name;
    int dname_len = strlen(query->query_cont);
    if (dname_len >= 982) 
        fprintf(stderr, "[ERR]: dirname is too long for rpc buffer.\n");
    *((int*)(rpc_buf+8)) = dname_len;
    memcpy(rpc_buf + 12, query->query_cont, dname_len);

    return (dname_len + 16);
}


int generate_query_getlen(struct ibdev_resc* resc, int mcnid, struct rpc_query* query, int fd){
    char* rpc_buf = resc->rpc_buf[mcnid - 1];
    memset(rpc_buf, 0, RPC_MR_SIZE);

    // process in-param and generate query
    *((int*)(rpc_buf+0)) = query->op_ordr;
    *((int*)(rpc_buf+4)) = query->op_name;
    *((int*)(rpc_buf+8)) = fd;

    return 16;
}


int generate_query_setlen(struct ibdev_resc* resc, int mcnid, struct rpc_query* query, int fd, int64_t length){
    char* rpc_buf = resc->rpc_buf[mcnid - 1];
    memset(rpc_buf, 0, RPC_MR_SIZE);

    // process in-param and generate query
    *((int*)(rpc_buf+0)) = query->op_ordr;
    *((int*)(rpc_buf+4)) = query->op_name;
    *((int*)(rpc_buf+8)) = fd;
    *((int64_t*)(rpc_buf+16)) = length;

    return 32;
}


// for simplicity, only pread(w) function implemented internally.
// an array of file pointer stored here as a support for read(w).
off_t file_pointer[1000000];
uint64_t fstat_size[1000000];
char* callnamebuf = NULL;
int fileis_dirty[1000000];


#endif
