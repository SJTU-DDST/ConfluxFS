#ifndef __QDFS_BACKGR
#define __QDFS_BACKGR

#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <x86intrin.h>
#include <libpmem.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include "qdfs_algori.c"


char* PMEM_POOL_NAME;
const char* SHM_HOMEDIR = "/dev/shm";
const uint64_t PMEM_POOL_LENT = (uint64_t)(1 << 30) * (uint64_t)64;
const uint32_t LOCK_GRANU = (1 << 12);
const uint32_t SEGM_LEN = (1 << 21);
const uint64_t SPBLK_BEG = (uint64_t)0;
const uint64_t SPBLK_SIZE = (uint64_t)(1 << 12);

const int MAX_INODE = (1 << 20);
const uint64_t INODE_MAP_BEG = (uint64_t)(1 << 20);
const uint64_t INODE_MAP_SIZE = (uint64_t)(1 << 20);
const int MAX_DENTRY = (1 << 21);
const uint64_t DENTRY_MAP_BEG = (uint64_t)(1 << 22);
const uint64_t DENTRY_MAP_SIZE = (uint64_t)(1 << 21);
const int MAX_DTBLK = (1 << 20) * 14;
const uint64_t DTBLK_MAP_BEG = (uint64_t)(1 << 24);
const uint64_t DTBLK_MAP_SIZE = (uint64_t)(1 << 20) * 14;

const int DENTRY_SIZE = 64;
const uint64_t DENTRY_TREE_BEG = (uint64_t)(1 << 29);
const uint64_t DENTRY_TREE_LENT = (uint64_t)(1 << 21) * (uint64_t)64;
const int INODE_SIZE = (1 << 12);
const uint64_t INODE_BLK_BEG = (uint64_t)(1 << 30);
const uint64_t INODE_BLK_LENT = (uint64_t)(1 << 20) * (uint64_t)(1 << 12);
const int DTBLK_SIZE = (1 << 12);
const uint64_t DTBLK_BEG = (uint64_t)(1 << 30) * 6;
const uint64_t DTBLK_LENT = (uint64_t)(1 << 20) * 14 * (uint64_t)(1 << 12);

const int Inode_Cache_GroupNum = (1 << 11);
const int Inode_Cache_GroupSize = (1 << 5);
const int Inode_Cache_Mask = (1 << 11) - 1;


const int MAXI_PROC_USER = 16;
const int MAXI_NOFILE = (1<<17);


int openPoolAndTestWrite(int fill){
    fprintf(stdout, "DEBUG_INFO: Pmem Pool Length: %llu\n", PMEM_POOL_LENT);
    int fd = open(PMEM_POOL_NAME, O_RDWR);
    fprintf(stdout, "DEBUG_INFO: Opened file fd is: %d\n", fd);
    char* pool = (char*)mmap(NULL, PMEM_POOL_LENT, PROT_WRITE, MAP_SHARED, fd, 0);
    uint64_t i;
    uint64_t i_max = PMEM_POOL_LENT / SEGM_LEN;
    uint64_t ptr;

    unsigned long start_time_msec;
	unsigned long cur_time_msec;
    struct timeval cur_time;
    gettimeofday(&cur_time, NULL);
	start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);

    for (i = 0; i < i_max; ++i){
        ptr = i * SEGM_LEN;
        pmem_memset(pool+ptr, fill, SEGM_LEN, PMEM_F_MEM_NONTEMPORAL);
        _mm_mfence();
    }

    gettimeofday(&cur_time, NULL);
	cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    int time_use = cur_time_msec - start_time_msec;

    fprintf(stdout, "RUN_LOG: time of writing whole pool is: %d.%03d sec\n", 
        time_use / 1000, time_use % 1000);
    ptr = (uint64_t)(1 << 20) * (uint64_t)60000;
    fprintf(stdout, "DEBUG_INFO: the written char is: %c\n", pool[ptr]);
    munmap(pool, PMEM_POOL_LENT);
    close(fd);
    return 0;
}

struct perproc_fsmeta{
    pthread_mutex_t lock;
    uint32_t pid;
    uint32_t nouse;
    uint64_t pmpool;
    uint64_t sb_addr;
    uint64_t inode_bm_addr;
    uint64_t dentry_bm_addr;
    uint64_t block_bm_addr;
    uint64_t sb_lock_addr;
    uint64_t indbm_lock_addr;
    uint64_t dirbm_lock_addr;
    uint64_t blkbm_lock_addr;
    uint64_t dir_cache;
    uint64_t dir_cache_flag;
    uint64_t inode_cache;
    uint64_t inode_cache_atime;
    uint64_t inode_cache_gtime;
    uint64_t inode_cache_lock;
    uint64_t dentry_lock;
    uint64_t inode_lock;
    uint64_t fileblk_lockarr;
    uint64_t allinflightlock;
};


char* getPmemPool(const char* pool_name, uint64_t pool_lent){
    int fd = open(pool_name, O_RDWR);
    if (fd < 0) {
        fprintf(stderr, "[ERR]: open pm File Failed.\n");
        return NULL;
    }
    fprintf(stdout, "DEBUG_INFO: Opened file fd is: %d\n", fd);

    // At service process, we need this pool to do initialization
    char* pool = (char*)mmap(NULL, pool_lent, PROT_WRITE, MAP_SHARED, fd, 0);
    if (!pool) {
        fprintf(stderr, "[ERR]: mmap pm File Failed.\n");
        return NULL;
    }

    int meta_list_fd = shm_open("QZLSHM_METALIST", O_RDWR | O_CREAT, 0666);
    ftruncate(meta_list_fd, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER);
    struct perproc_fsmeta* meta_list = mmap(NULL, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER, 
        PROT_WRITE, MAP_SHARED, meta_list_fd, 0);
    memset(meta_list, 0, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER);

    pthread_mutexattr_t mutex_attr;
    pthread_mutexattr_init(&mutex_attr);
    pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED);
    int i;
    for (i = 0; i < MAXI_PROC_USER; ++i){
        pthread_mutex_t* plock = &(meta_list[i].lock);
        pthread_mutex_init(plock, &mutex_attr);
    }

    struct perproc_fsmeta* main_meta = &(meta_list[0]);
    // int pos = atomic_fetch_add(&(main_meta->nouse), 1);
    int pos = main_meta->nouse++;
    if (pos != 0) fprintf(stderr, "[ERR]: server proc not marked as #0!\n");
    int pid = getpid();
    meta_list[pos].pid = pid;
    meta_list[pos].pmpool = (uint64_t)pool;
    
    munmap(meta_list, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER);
    close(meta_list_fd);
    close(fd);
    return pool;
}

int destroyPmemPool(char *pmpool, uint64_t pool_lent){
    fprintf(stdout, "DEBUG_INFO: about to close the pmem pool.\n");

    int meta_list_fd = shm_open("QZLSHM_METALIST", O_RDWR | O_CREAT, 0666);
    struct perproc_fsmeta* meta_list = mmap(NULL, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER, 
        PROT_WRITE, MAP_SHARED, meta_list_fd, 0);
    struct perproc_fsmeta* main_meta = &(meta_list[0]);
    while (main_meta->nouse > 1){
        fprintf(stderr, "[WARN]: There is some process using QDFS, waiting.\n");
        waitMiliSec(1500);
    }

    munmap(meta_list, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER);
    close(meta_list_fd);

    munmap(pmpool, pool_lent);

    return 0;
}



struct super_block{
    uint64_t base_addr;
    uint64_t fs_size;
    uint64_t root_addr;
    uint64_t magic;
    uint64_t max_file_lent;
    uint32_t dir_cnt;
    uint32_t ind_cnt; // count used inode
    uint32_t blk_cnt;
    uint32_t dir_bm_id;
    uint32_t ind_bm_id; // inode bitmap alloc-ptr
    uint32_t blk_bm_id;
};

// all addr in this struct are relative to pmpool
struct dentry_ele{
    uint32_t version;
    uint32_t dentry_id;
    uint64_t inode_ptr;
    uint64_t child_head;
    uint64_t parent_ptr;
    uint64_t next_sib;
    uint64_t prev_sib;
    uint64_t next_onINode;
    uint64_t prev_onINode;
};

struct inode_ele{
    uint32_t version;
    uint32_t type_flag; // 1 as dir, 2 as file
    uint32_t inode_id;
    uint32_t dentry_head_id;
    uint64_t file_len;
    uint64_t fstat_len;
    uint32_t index[SEG_PER_BLOCK * 2];
    uint32_t index_tail;
    uint32_t next_index;
};


struct super_block* load_super_block(char* pmpool){
    
    int sb_ipcfd = shm_open("QZLSHM_SB", O_RDWR | O_CREAT, 0666);
    ftruncate(sb_ipcfd, sizeof(struct super_block));
    struct super_block* sb;
    sb = mmap(NULL, sizeof(struct super_block), PROT_WRITE, MAP_SHARED, sb_ipcfd, 0);

    memcpy(sb, pmpool+SPBLK_BEG, sizeof(struct super_block));
    _mm_mfence();
    close(sb_ipcfd);

    int meta_list_fd = shm_open("QZLSHM_METALIST", O_RDWR | O_CREAT, 0666);
    struct perproc_fsmeta* meta_list = mmap(NULL, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER, 
        PROT_WRITE, MAP_SHARED, meta_list_fd, 0);
    struct perproc_fsmeta* main_meta = &(meta_list[0]);
    main_meta->sb_addr = (uint64_t)sb;

    munmap(meta_list, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER);
    close(meta_list_fd);

    return sb;
}

int stor_super_block(struct super_block* sb, char* pmpool){

    pmem_memcpy(pmpool+SPBLK_BEG, sb, sizeof(struct super_block), PMEM_F_MEM_NONTEMPORAL);
    _mm_mfence();

    return 0;
}

// No lock cause only init will trigger this func
int initial_super_block(char* pmpool){
    struct super_block* sb;
    sb = load_super_block(pmpool);
    sb->base_addr = SPBLK_BEG;
    sb->fs_size = PMEM_POOL_LENT;
    sb->root_addr = DENTRY_TREE_BEG;
    sb->magic = 19980803;
    sb->max_file_lent = (PMEM_POOL_LENT >> 1);
    sb->dir_cnt = 0;
    sb->ind_cnt = 0;
    sb->blk_cnt = 0;
    sb->dir_bm_id = 0;
    sb->ind_bm_id = 0;
    sb->blk_bm_id = 0;
    stor_super_block(sb, pmpool);
    
    return 0;
}

uint8_t* load_inode_bitmap(char *pmpool){
    
    int indbm_ipcfd = shm_open("QZLSHM_INDBM", O_RDWR | O_CREAT, 0666);
    ftruncate(indbm_ipcfd, sizeof(uint8_t) * INODE_MAP_SIZE);
    uint8_t* bm_arr;
    bm_arr = mmap(NULL, sizeof(uint8_t) * INODE_MAP_SIZE, PROT_WRITE, MAP_SHARED, indbm_ipcfd, 0);
    
    memcpy(bm_arr, pmpool+INODE_MAP_BEG, INODE_MAP_SIZE);
    _mm_mfence();
    close(indbm_ipcfd);

    int meta_list_fd = shm_open("QZLSHM_METALIST", O_RDWR | O_CREAT, 0666);
    struct perproc_fsmeta* meta_list = mmap(NULL, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER, 
        PROT_WRITE, MAP_SHARED, meta_list_fd, 0);
    struct perproc_fsmeta* main_meta = &(meta_list[0]);
    main_meta->inode_bm_addr = (uint64_t)bm_arr;

    munmap(meta_list, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER);
    close(meta_list_fd);

    return bm_arr;
}

uint8_t* load_dentry_bitmap(char *pmpool){
    
    int dirbm_ipcfd = shm_open("QZLSHM_DIRBM", O_RDWR | O_CREAT, 0666);
    ftruncate(dirbm_ipcfd, sizeof(uint8_t) * DENTRY_MAP_SIZE);
    uint8_t* bm_arr;
    bm_arr = mmap(NULL, sizeof(uint8_t) * DENTRY_MAP_SIZE, PROT_WRITE, MAP_SHARED, dirbm_ipcfd, 0);
    
    memcpy(bm_arr, pmpool+DENTRY_MAP_BEG, DENTRY_MAP_SIZE);
    _mm_mfence();
    close(dirbm_ipcfd);

    int meta_list_fd = shm_open("QZLSHM_METALIST", O_RDWR | O_CREAT, 0666);
    struct perproc_fsmeta* meta_list = mmap(NULL, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER, 
        PROT_WRITE, MAP_SHARED, meta_list_fd, 0);
    struct perproc_fsmeta* main_meta = &(meta_list[0]);
    main_meta->dentry_bm_addr = (uint64_t)bm_arr;

    munmap(meta_list, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER);
    close(meta_list_fd);

    return bm_arr;
}

uint8_t* load_dtblk_bitmap(char *pmpool){
    
    int blkbm_ipcfd = shm_open("QZLSHM_BLKBM", O_RDWR | O_CREAT, 0666);
    ftruncate(blkbm_ipcfd, sizeof(uint8_t) * DTBLK_MAP_SIZE);
    uint8_t* bm_arr;
    bm_arr = mmap(NULL, sizeof(uint8_t) * DTBLK_MAP_SIZE, PROT_WRITE, MAP_SHARED, blkbm_ipcfd, 0);
    
    memcpy(bm_arr, pmpool+DTBLK_MAP_BEG, DTBLK_MAP_SIZE);
    _mm_mfence();
    close(blkbm_ipcfd);

    int meta_list_fd = shm_open("QZLSHM_METALIST", O_RDWR | O_CREAT, 0666);
    struct perproc_fsmeta* meta_list = mmap(NULL, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER, 
        PROT_WRITE, MAP_SHARED, meta_list_fd, 0);
    struct perproc_fsmeta* main_meta = &(meta_list[0]);
    main_meta->block_bm_addr = (uint64_t)bm_arr;

    munmap(meta_list, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER);
    close(meta_list_fd);

    return bm_arr;
}

// Actually we do not need to store bitmap at runtime,
// scan the whole inode region will reconstruct it.
int stor_inode_bitmap(char *pmpool, uint8_t *bm_arr){
    pmem_memcpy(pmpool+INODE_MAP_BEG, bm_arr, INODE_MAP_SIZE, PMEM_F_MEM_NONTEMPORAL);
    _mm_mfence();
    return 0;
}

// Again, we do not need to store bitmap at runtime,
// scan the whole inode region will reconstruct it.
int stor_dentry_bitmap(char *pmpool, uint8_t *bm_arr){
    pmem_memcpy(pmpool+DENTRY_MAP_BEG, bm_arr, DENTRY_MAP_SIZE, PMEM_F_MEM_NONTEMPORAL);
    _mm_mfence();
    return 0;
}

// Also, we do not need to store bitmap at runtime,
// scan the whole inode region will reconstruct it.
int stor_dtblk_bitmap(char *pmpool, uint8_t *bm_arr){
    pmem_memcpy(pmpool+DTBLK_MAP_BEG, bm_arr, DTBLK_MAP_SIZE, PMEM_F_MEM_NONTEMPORAL);
    _mm_mfence();
    return 0;
}


int get_new_BMpos(struct super_block* sb, uint8_t *bm_arr, pthread_mutex_t* lock, char mode){
    int poll_cnt = 0;
    int cur;
    switch(mode){
        case 'i': {
            pthread_mutex_lock(lock);
            cur = sb->ind_bm_id;
            while (poll_cnt < INODE_MAP_SIZE){
                if (bm_arr[cur] == 0) break;
                cur += 1;
                if (cur >= INODE_MAP_SIZE) cur = 0;
                poll_cnt += 1; 
            }
            sb->ind_bm_id = cur;
            sb->ind_cnt += 1;
            break;
        }
        case 'd': {
            pthread_mutex_lock(lock);
            cur = sb->dir_bm_id;
            while (poll_cnt < DENTRY_MAP_SIZE){
                if (bm_arr[cur] == 0) break;
                cur += 1;
                if (cur >= DENTRY_MAP_SIZE) cur = 0;
                poll_cnt += 1; 
            }
            sb->dir_bm_id = cur;
            sb->dir_cnt += 1;
            break;
        }
        case 'b': {
            pthread_mutex_lock(lock);
            cur = sb->blk_bm_id;
            while (poll_cnt < DTBLK_MAP_SIZE){
                if (bm_arr[cur] == 0) break;
                cur += 1;
                if (cur >= DTBLK_MAP_SIZE) cur = 0;
                poll_cnt += 1; 
            }
            sb->blk_bm_id = cur;
            sb->blk_cnt += 1;
            break;
        }
        default: {
            fprintf(stderr, "[ERR]: bitmap mode should be: i or d or b.\n");
            return -1;
        }
    }
    if (bm_arr[cur] == 1){
        fprintf(stderr, "[ERR]: NO more free inode.\n");
        return -1;
    }
    bm_arr[cur] = 1;
    pthread_mutex_unlock(lock);
    return cur;
}

// only work at initialization in server process
int make_root(struct perproc_fsmeta* meta){

    char *pmpool = (char*)(meta->pmpool);
    struct super_block* sb = (struct super_block*)(meta->sb_addr);
    uint8_t* inode_bm = (uint8_t*)(meta->inode_bm_addr);
    uint8_t* dentry_bm = (uint8_t*)(meta->dentry_bm_addr);

    pthread_mutex_t* indbm_lock = (pthread_mutex_t*)(meta->indbm_lock_addr);
    pthread_mutex_t* dirbm_lock = (pthread_mutex_t*)(meta->dirbm_lock_addr);

    int new_inode_id = get_new_BMpos(sb, inode_bm, indbm_lock, 'i');
    if (new_inode_id != 0) {
        fprintf(stderr, "[ERR]: trying to write root inode at not-0 place, failed.\n");
        return 1;
    }
    struct inode_ele* new_inode;
    new_inode = (struct inode_ele*)malloc(sizeof(struct inode_ele));
    memset(new_inode, 0, sizeof(struct inode_ele));
    new_inode->version = 1;
    new_inode->type_flag = 1;
    new_inode->inode_id = 0;
    new_inode->file_len = 0;
    new_inode->fstat_len = 0;
    // although not fetch yet, I know the root's dir is #0
    new_inode->dentry_head_id = 0;
    new_inode->index_tail = 0;
    new_inode->next_index = 0;
    pmem_memcpy(pmpool+INODE_BLK_BEG, new_inode, sizeof(struct inode_ele), 
        PMEM_F_MEM_NONTEMPORAL);
    _mm_mfence();

    int new_dir_id = get_new_BMpos(sb, dentry_bm, dirbm_lock, 'd');
    if (new_dir_id != 0) {
        fprintf(stderr, "[ERR]: trying to put root dentry at not-0 place, failed.\n");
        return 1;
    }
    struct dentry_ele* root;
    root = (struct dentry_ele*)malloc(sizeof(struct dentry_ele));
    memset(root, 0, sizeof(struct dentry_ele));
    root->version = 1;
    root->dentry_id = 0;
    // same as above, in fact addr need to + pmpool
    root->inode_ptr = INODE_BLK_BEG;
    pmem_memcpy(pmpool+DENTRY_TREE_BEG, root, sizeof(struct dentry_ele), 
        PMEM_F_MEM_NONTEMPORAL);
    _mm_mfence();
    
    free(new_inode);
    free(root);
    return 0;
}


struct perproc_fsmeta* produce_meta_struct(uint8_t init_flag){
    if (init_flag == 1)
        openPoolAndTestWrite(0);
    
    char* pmpool = getPmemPool(PMEM_POOL_NAME, PMEM_POOL_LENT);

    fprintf(stdout, "DEBUG_INFO: beg of pmpool: %p\n", pmpool);
    fprintf(stdout, "DEBUG_INFO: end of pmpool: %p\n", &(pmpool[PMEM_POOL_LENT-1]));

    int meta_list_fd = shm_open("QZLSHM_METALIST", O_RDWR, 0666);
    struct perproc_fsmeta* meta_list = mmap(NULL, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER, 
        PROT_WRITE, MAP_SHARED, meta_list_fd, 0);
    struct perproc_fsmeta* main_meta = &(meta_list[0]);
    
    if (init_flag == 1)
        initial_super_block(pmpool);
    struct super_block* sb = load_super_block(pmpool);

    uint8_t * inode_bm = load_inode_bitmap(pmpool);
    uint8_t * dir_bm = load_dentry_bitmap(pmpool);
    uint8_t * blk_bm = load_dtblk_bitmap(pmpool);

    fprintf(stdout, "DEBUG_INFO: the inode bm[0] now is: %u\n", (uint32_t)(inode_bm[0]));

    // need process shared mutex_attr and rwlock_attr
    pthread_mutexattr_t mutex_attr;
    pthread_mutexattr_init(&mutex_attr);
    pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED);

    pthread_rwlockattr_t rwlock_attr;
    pthread_rwlockattr_init(&rwlock_attr);
    pthread_rwlockattr_setpshared(&rwlock_attr, PTHREAD_PROCESS_SHARED);

    // init four metadata (bm) locks

    pthread_mutex_t *super_block_mutex;
    int sblk_ipcfd = shm_open("QZLSHM_SBLK", O_RDWR | O_CREAT, 0666);
    ftruncate(sblk_ipcfd, sizeof(pthread_mutex_t));
    super_block_mutex = mmap(NULL, sizeof(pthread_mutex_t), PROT_WRITE, MAP_SHARED, sblk_ipcfd, 0);
    memset(super_block_mutex, 0, sizeof(pthread_mutex_t));
    pthread_mutex_init(super_block_mutex, &mutex_attr);

    main_meta->sb_lock_addr = (uint64_t)super_block_mutex;
    close(sblk_ipcfd);

    pthread_mutex_t *inode_bm_mutex;
    int indbmlk_ipcfd = shm_open("QZLSHM_INDBMLK", O_RDWR | O_CREAT, 0666);
    ftruncate(indbmlk_ipcfd, sizeof(pthread_mutex_t));
    inode_bm_mutex = mmap(NULL, sizeof(pthread_mutex_t), PROT_WRITE, MAP_SHARED, indbmlk_ipcfd, 0);
    memset(inode_bm_mutex, 0, sizeof(pthread_mutex_t));
    pthread_mutex_init(inode_bm_mutex, &mutex_attr);

    main_meta->indbm_lock_addr = (uint64_t)inode_bm_mutex;
    close(indbmlk_ipcfd);

    pthread_mutex_t *dentry_bm_mutex;
    int dirbmlk_ipcfd = shm_open("QZLSHM_DIRBMLK", O_RDWR | O_CREAT, 0666);
    ftruncate(dirbmlk_ipcfd, sizeof(pthread_mutex_t));
    dentry_bm_mutex = mmap(NULL, sizeof(pthread_mutex_t), PROT_WRITE, MAP_SHARED, dirbmlk_ipcfd, 0);
    memset(dentry_bm_mutex, 0, sizeof(pthread_mutex_t));
    pthread_mutex_init(dentry_bm_mutex, &mutex_attr);

    main_meta->dirbm_lock_addr = (uint64_t)dentry_bm_mutex;
    close(dirbmlk_ipcfd);

    pthread_mutex_t *dtblk_bm_mutex;
    int blkbmlk_ipcfd = shm_open("QZLSHM_BLKBMLK", O_RDWR | O_CREAT, 0666);
    ftruncate(blkbmlk_ipcfd, sizeof(pthread_mutex_t));
    dtblk_bm_mutex = mmap(NULL, sizeof(pthread_mutex_t), PROT_WRITE, MAP_SHARED, blkbmlk_ipcfd, 0);
    memset(dtblk_bm_mutex, 0, sizeof(pthread_mutex_t));
    pthread_mutex_init(dtblk_bm_mutex, &mutex_attr);

    main_meta->blkbm_lock_addr = (uint64_t)dtblk_bm_mutex;
    close(blkbmlk_ipcfd);


    if (init_flag == 1)
        make_root(main_meta); 
    
    // prepare dir cache
    int dir_cache_ipcfd = shm_open("QZLSHM_DIR_CACHE", O_RDWR | O_CREAT, 0666);
    ftruncate(dir_cache_ipcfd, sizeof(struct dentry_ele) * MAX_DENTRY);
    struct dentry_ele* dir_cache;
    dir_cache = mmap(NULL, sizeof(struct dentry_ele) * MAX_DENTRY, PROT_WRITE, MAP_SHARED, dir_cache_ipcfd, 0);
    memset(dir_cache, 0, sizeof(struct dentry_ele) * MAX_DENTRY);
    main_meta->dir_cache = (uint64_t)(dir_cache);
    close(dir_cache_ipcfd);

    int dir_cache_flag_ipcfd = shm_open("QZLSHM_DIR_CACHE_FLAG", O_RDWR | O_CREAT, 0666);
    ftruncate(dir_cache_flag_ipcfd, sizeof(uint8_t) * MAX_DENTRY);
    uint8_t *dir_cache_flag;
    dir_cache_flag = mmap(NULL, sizeof(uint8_t) * MAX_DENTRY, PROT_WRITE, MAP_SHARED, dir_cache_flag_ipcfd, 0);
    memset(dir_cache_flag, 0, sizeof(uint8_t) * MAX_DENTRY);
    main_meta->dir_cache_flag = (uint64_t)(dir_cache_flag);
    close(dir_cache_flag_ipcfd);

    // prepare inode cache
    int i;
    int inode_cache_ipcfd = shm_open("QZLSHM_INODE_CACHE", O_RDWR | O_CREAT, 0666);
    ftruncate(inode_cache_ipcfd, sizeof(struct inode_ele) * 
        (Inode_Cache_GroupNum * Inode_Cache_GroupSize));
    struct inode_ele* inode_cache;
    inode_cache = mmap(NULL, sizeof(struct inode_ele) * 
        (Inode_Cache_GroupNum * Inode_Cache_GroupSize), PROT_WRITE, MAP_SHARED, inode_cache_ipcfd, 0);
    memset(inode_cache, 0, sizeof(struct inode_ele) * 
        (Inode_Cache_GroupNum * Inode_Cache_GroupSize));
    main_meta->inode_cache = (uint64_t)(inode_cache);
    close(inode_cache_ipcfd);
    
    int inode_cache_atime_ipcfd = shm_open("QZLSHM_INODE_CACHE_ATIME", O_RDWR | O_CREAT, 0666);
    ftruncate(inode_cache_atime_ipcfd, sizeof(uint64_t) * 
        (Inode_Cache_GroupNum * Inode_Cache_GroupSize));
    uint64_t* inode_cache_atime;
    inode_cache_atime = mmap(NULL, sizeof(uint64_t) * 
        (Inode_Cache_GroupNum * Inode_Cache_GroupSize), PROT_WRITE, MAP_SHARED, inode_cache_atime_ipcfd, 0);
    memset(inode_cache_atime, 0, sizeof(uint64_t) * 
        (Inode_Cache_GroupNum * Inode_Cache_GroupSize));
    main_meta->inode_cache_atime = (uint64_t)(inode_cache_atime);
    close(inode_cache_atime_ipcfd);
    
    int inode_cache_lock_ipcfd = shm_open("QZLSHM_INODE_CACHE_LOCK", O_RDWR | O_CREAT, 0666);
    ftruncate(inode_cache_lock_ipcfd, sizeof(pthread_rwlock_t)*Inode_Cache_GroupNum);
    pthread_rwlock_t* inode_cache_lock;
    inode_cache_lock = mmap(NULL, sizeof(pthread_rwlock_t)*Inode_Cache_GroupNum, 
        PROT_WRITE, MAP_SHARED, inode_cache_lock_ipcfd, 0);
    memset(inode_cache_lock, 0, sizeof(pthread_rwlock_t)*Inode_Cache_GroupNum);
    for (i = 0; i < Inode_Cache_GroupNum; ++i)
        pthread_rwlock_init((inode_cache_lock + i), &rwlock_attr);
    main_meta->inode_cache_lock = (uint64_t)(inode_cache_lock);
    close(inode_cache_lock_ipcfd);
    
    int inode_cache_gtime_ipcfd = shm_open("QZLSHM_INODE_CACHE_GTIME", O_RDWR | O_CREAT, 0666);
    ftruncate(inode_cache_gtime_ipcfd, sizeof(uint64_t)*Inode_Cache_GroupNum);
    uint64_t* inode_cache_group_time;
    inode_cache_group_time = mmap(NULL, sizeof(uint64_t)*Inode_Cache_GroupNum, 
        PROT_WRITE, MAP_SHARED, inode_cache_gtime_ipcfd, 0);
    memset(inode_cache_group_time, 0, sizeof(uint64_t)*Inode_Cache_GroupNum);
    main_meta->inode_cache_gtime = (uint64_t)(inode_cache_group_time);
    close(inode_cache_gtime_ipcfd);


    // Init read_write lock array (on data region)

    int inode_lock_ipcfd = shm_open("QZLSHM_INODE_LOCK", O_RDWR | O_CREAT, 0666);
    ftruncate(inode_lock_ipcfd, sizeof(pthread_rwlock_t) * MAX_INODE);
    pthread_rwlock_t* inode_lock;
    inode_lock = mmap(NULL, sizeof(pthread_rwlock_t) * MAX_INODE, PROT_WRITE, MAP_SHARED, inode_lock_ipcfd, 0);
    memset(inode_lock, 0, sizeof(pthread_rwlock_t) * MAX_INODE);
    for (i = 0; i < MAX_INODE; ++i)
        pthread_rwlock_init((inode_lock + i), &rwlock_attr);
    main_meta->inode_lock = (uint64_t)(inode_lock);
    close(inode_lock_ipcfd);
    
    int dentry_lock_ipcfd = shm_open("QZLSHM_DENTRY_LOCK", O_RDWR | O_CREAT, 0666);
    ftruncate(dentry_lock_ipcfd, sizeof(pthread_rwlock_t) * MAX_DENTRY);
    pthread_rwlock_t* dentry_lock;
    dentry_lock = mmap(NULL, sizeof(pthread_rwlock_t) * MAX_DENTRY, PROT_WRITE, MAP_SHARED, dentry_lock_ipcfd, 0);
    memset(dentry_lock, 0, sizeof(pthread_rwlock_t) * MAX_DENTRY);
    for (i = 0; i < MAX_DENTRY; ++i)
        pthread_rwlock_init((dentry_lock + i), &rwlock_attr);
    main_meta->dentry_lock = (uint64_t)(dentry_lock);
    close(dentry_lock_ipcfd);
    
    // note that data block locks are tree structured, we store per-file &root,#blklock in array
    uint64_t* file_locks = (uint64_t*)malloc(sizeof(uint64_t) * 2 * MAX_INODE);
    memset(file_locks, 0, sizeof(uint64_t) * 2 * MAX_INODE);
    main_meta->fileblk_lockarr = (uint64_t)file_locks;

    // Per-Process opened fd array, needed by applications
    int* Appfd_Array;
    int appfd_array_ipcfd = shm_open("QZLSHM_APPFD_ARRAY", O_RDWR | O_CREAT, 0666);
    ftruncate(appfd_array_ipcfd, sizeof(int)*MAXI_PROC_USER*MAXI_NOFILE);
    Appfd_Array = mmap(NULL, sizeof(int)*MAXI_PROC_USER*MAXI_NOFILE, PROT_WRITE, MAP_SHARED, 
        appfd_array_ipcfd, 0);
    memset(Appfd_Array, 0, sizeof(int)*MAXI_PROC_USER*MAXI_NOFILE);
    munmap(Appfd_Array, sizeof(int)*MAXI_PROC_USER*MAXI_NOFILE);
    close(appfd_array_ipcfd);

    // To provide inflight locking query service
    int inflight_lock_ipcfd = shm_open("QZLSHM_INFLIGHT_LOCK", O_RDWR | O_CREAT, 0666);
    ftruncate(inflight_lock_ipcfd, sizeof(struct inflight_lockinfo) * MAXUSER_NUM * MAXCL_THNUM);
    struct inflight_lockinfo* inflight_locks;
    inflight_locks = mmap(NULL, sizeof(struct inflight_lockinfo) * MAXUSER_NUM * MAXCL_THNUM, PROT_WRITE, MAP_SHARED, inflight_lock_ipcfd, 0);
    memset(inflight_locks, 0, sizeof(struct inflight_lockinfo) * MAXUSER_NUM * MAXCL_THNUM);
    main_meta->allinflightlock = (uint64_t)(inflight_locks);
    close(inflight_lock_ipcfd);
    
    // munmap(meta_list, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER);
    close(meta_list_fd);
    
    return main_meta;
}


int free_meta_struct(){
    fprintf(stdout, "DEBUG_INFO: about to free meta info.\n");
    int meta_list_fd = shm_open("QZLSHM_METALIST", O_RDWR, 0666);
    struct perproc_fsmeta* meta_list = mmap(NULL, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER, 
        PROT_WRITE, MAP_SHARED, meta_list_fd, 0);
    struct perproc_fsmeta* main_meta = &(meta_list[0]);

    munmap((void*)(main_meta->allinflightlock), sizeof(struct inflight_lockinfo) * MAXUSER_NUM * MAXCL_THNUM);
    shm_unlink("QZLSHM_INFLIGHT_LOCK");

    shm_unlink("QZLSHM_APPFD_ARRAY");

    free((void*)(main_meta->fileblk_lockarr));

    munmap((void*)(main_meta->dentry_lock), sizeof(pthread_rwlock_t) * MAX_DENTRY);
    shm_unlink("QZLSHM_DENTRY_LOCK");

    munmap((void*)(main_meta->inode_lock), sizeof(pthread_rwlock_t) * MAX_INODE);
    shm_unlink("QZLSHM_INODE_LOCK");

    munmap((void*)(main_meta->inode_cache_gtime), sizeof(uint64_t)*Inode_Cache_GroupNum);
    shm_unlink("QZLSHM_INODE_CACHE_GTIME");

    munmap((void*)(main_meta->inode_cache_lock), sizeof(pthread_rwlock_t)*Inode_Cache_GroupNum);
    shm_unlink("QZLSHM_INODE_CACHE_LOCK");

    munmap((void*)(main_meta->inode_cache_atime), sizeof(uint64_t) * 
        (Inode_Cache_GroupNum*Inode_Cache_GroupSize));
    shm_unlink("QZLSHM_INODE_CACHE_ATIME");

    munmap((void*)(main_meta->inode_cache), sizeof(struct inode_ele) * 
        (Inode_Cache_GroupNum * Inode_Cache_GroupSize));
    shm_unlink("QZLSHM_INODE_CACHE");

    munmap((void*)(main_meta->dir_cache_flag), sizeof(uint8_t) * MAX_DENTRY);
    shm_unlink("QZLSHM_DIR_CACHE_FLAG");

    munmap((void*)(main_meta->dir_cache), sizeof(struct dentry_ele) * MAX_DENTRY);
    shm_unlink("QZLSHM_DIR_CACHE");

    munmap((void*)(main_meta->blkbm_lock_addr), sizeof(pthread_mutex_t));
    shm_unlink("QZLSHM_BLKBMLK");

    munmap((void*)(main_meta->dirbm_lock_addr), sizeof(pthread_mutex_t));
    shm_unlink("QZLSHM_DIRBMLK");

    munmap((void*)(main_meta->indbm_lock_addr), sizeof(pthread_mutex_t));
    shm_unlink("QZLSHM_INDBMLK");

    munmap((void*)(main_meta->sb_lock_addr), sizeof(pthread_mutex_t));
    shm_unlink("QZLSHM_SBLK");


    stor_dtblk_bitmap((char*)(main_meta->pmpool), (uint8_t*)(main_meta->block_bm_addr));
    stor_dentry_bitmap((char*)(main_meta->pmpool), (uint8_t*)(main_meta->dentry_bm_addr));
    stor_inode_bitmap((char*)(main_meta->pmpool), (uint8_t*)(main_meta->inode_bm_addr));

    munmap((void*)(main_meta->block_bm_addr), sizeof(uint8_t)*DTBLK_MAP_SIZE);
    munmap((void*)(main_meta->dentry_bm_addr), sizeof(uint8_t)*DENTRY_MAP_SIZE);
    munmap((void*)(main_meta->inode_bm_addr), sizeof(uint8_t)*INODE_MAP_SIZE);

    shm_unlink("QZLSHM_BLKBM");
    shm_unlink("QZLSHM_DIRBM");
    shm_unlink("QZLSHM_INDBM");

    stor_super_block((struct super_block*)(main_meta->sb_addr), (char*)(main_meta->pmpool));
    munmap((void*)(main_meta->sb_addr), sizeof(struct super_block));
    shm_unlink("QZLSHM_SB");

    destroyPmemPool((char*)(main_meta->pmpool), PMEM_POOL_LENT);

    munmap(meta_list, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER);
    close(meta_list_fd);
    shm_unlink("QZLSHM_METALIST");

    // dur to availability reason file lock shm unlink here
    char* command = (char*)malloc(80);
    memset(command, 0, 80);
    sprintf(command, "sudo rm -f %s/QZLSHM_ISXLOCK*", SHM_HOMEDIR);
    system(command);
    free(command); 

    return 0;
}


void* wait_sys_halt(void* arg){
    char quit_char = *(char*)arg;
    char* inchar = (char*)malloc(80);
    memset(inchar, 0, 80);
    while (inchar[0] != quit_char || inchar[1] != '\0') {
        // fprintf(stdout, "If FS system is about to halt, input q char: ");
        waitMiliSec(10);
        // fscanf(stdin, "%s", inchar);
    }
    fprintf(stdout, "\nHalt Command recieved.\n");
    return NULL;
}


int get_pinned_cpus(pthread_t pid){
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	int s = pthread_getaffinity_np(pid, sizeof(cpu_set_t), &cpuset);
	printf("Pinned CPUs for thread %d:\n", pid);
	int j;
	for (j = 0; j < CPU_SETSIZE; j++)
		if (CPU_ISSET(j, &cpuset))
			printf("    CPU %d;", j);
	printf("\n");
	return 0;
}

int set_pinned_cpus(pthread_t pid, int cpuBEG, int cpuEND){
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	int j;
	for (j = cpuBEG; j < cpuEND; j++)
        CPU_SET(j, &cpuset);
	int s = pthread_setaffinity_np(pid, sizeof(cpu_set_t), &cpuset);
	if (s != 0) fprintf(stderr, "\nPin thread %d to cpus %d~%d failed!!\n\n");
	return 0;
}


int main(int argc, char* argv[]){

    if (0){
        // config by parameter
        PMEM_POOL_NAME = argv[optind++];
        fprintf(stdout, "RUN INFO: the pmem pool is at %s\n", PMEM_POOL_NAME);
    }
    else{
        FILE *config_file = fopen(conflux_config_file, "r");
        PMEM_POOL_NAME = malloc(80);
        memset(PMEM_POOL_NAME, 0, 80);
        fscanf(config_file, "%[^\n] ", PMEM_POOL_NAME);
        fprintf(stdout, "RUN INFO: the pmem pool is at %s\n", PMEM_POOL_NAME);
        fclose(config_file);
    }

    // pthread_t self_pid = pthread_self();
    // set_pinned_cpus(self_pid, 16, 17);

    struct perproc_fsmeta* meta1 = produce_meta_struct(1);

    pthread_t wsh_tid;
    char QUIT_COMD = 'q';
    pthread_create(&wsh_tid, NULL, (void*)&wait_sys_halt, (void*)(&QUIT_COMD));
    pthread_join(wsh_tid, NULL);

    free_meta_struct();

    return 0;
}


#endif 
