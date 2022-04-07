#ifndef __FS_CLIENT_QD_USER
#define __FS_CLIENT_QD_USER

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
#include <sys/wait.h>
#include <x86intrin.h>
#include <libpmem.h>
#include <fcntl.h>
#include <pthread.h>
#include "qdfs_algori.c"


// const char* PMEM_POOL_NAME = "/dev/dax0.1";
char* PMEM_POOL_NAME;
const uint64_t PMEM_POOL_LENT = (uint64_t)(1 << 30) * (uint64_t)64;
const uint32_t LOCK_GRANU = (1 << 12);
const uint32_t SEGM_LEN = (1 << 23);
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
const int MAX_OPENED_APPFD = (1 << 17);

const int CPY_LENRPC_RESLIST = 122;



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




int load_dentry_struct(struct perproc_fsmeta* meta, int dentry_id){
    // check the dir_cache_flag first
    uint8_t* dir_cache_flag = (uint8_t*)(meta->dir_cache_flag);
    if (dir_cache_flag[dentry_id] == 1){
        // fprintf(stdout, "DEBUG_INFO: dentry cache hit on dir id: %d (OvO)\n", dentry_id);
        return 0;
    }
    char* pmpool = (char*)(meta->pmpool);
    struct dentry_ele* dir_cache = (struct dentry_ele*)(meta->dir_cache);
    struct dentry_ele* dir = dir_cache + dentry_id;
    uint64_t ptr = DENTRY_TREE_BEG + (uint64_t)sizeof(struct dentry_ele) * (uint64_t)dentry_id;
    
    memcpy(dir, pmpool+ptr, sizeof(struct dentry_ele));
    _mm_mfence();
    // dir->version += 1; // No change on version when load
    dir->dentry_id = dentry_id;

    dir_cache_flag[dentry_id] = 1;

    // fprintf(stdout, "dentry #%d loaded.\n", dentry_id);
    return 0;
}


int stor_dentry_struct(struct perproc_fsmeta* meta, int dentry_id){
    uint8_t* dir_cache_flag = (uint8_t*)(meta->dir_cache_flag);
    if (dir_cache_flag[dentry_id] == 0) {
        fprintf(stderr, "[ERR]: the dentry stor is on invalid position %d.\n", dentry_id);
        return -1;
    }
    char* pmpool = (char*)(meta->pmpool);
    struct dentry_ele* dir_cache = (struct dentry_ele*)(meta->dir_cache);

    struct dentry_ele* dir = dir_cache + dentry_id;
    uint64_t ptr = DENTRY_TREE_BEG + (uint64_t)dentry_id * (uint64_t)DENTRY_SIZE;

    pmem_memcpy(pmpool+ptr, dir, sizeof(struct dentry_ele),
        PMEM_F_MEM_NONTEMPORAL);
    _mm_mfence();

    // fprintf(stdout, "dentry #%d stored.\n", dentry_id);
    return 0;
}


// Keep in mind that this function will return an element in inode_cache
struct inode_ele* load_inode_struct(struct perproc_fsmeta* meta, int inode_id){
    //try in case cache hit
    int g = inode_id & Inode_Cache_Mask;
    uint32_t cache_g = g * Inode_Cache_GroupSize;

    pthread_rwlock_t* inode_cache_lock = (pthread_rwlock_t*)(meta->inode_cache_lock);
    uint64_t* inode_cache_atime = (uint64_t*)(meta->inode_cache_atime);
    struct inode_ele* inode_cache = (struct inode_ele*)(meta->inode_cache);
    uint64_t* inode_cache_group_time = (uint64_t*)(meta->inode_cache_gtime);

    struct inode_ele* ind;
    int i;
    pthread_rwlock_rdlock(inode_cache_lock + g);
    for (i = 0; i < Inode_Cache_GroupSize; ++i){
        if (inode_cache_atime[cache_g+i] == 0) continue;
        if (inode_cache[cache_g+i].inode_id == inode_id){
            inode_cache_group_time[g] += 1;
            inode_cache_atime[cache_g+i] = inode_cache_group_time[g];
            pthread_rwlock_unlock(inode_cache_lock + g);

            ind = inode_cache + (cache_g+i);
            return ind;
        }
    }
    pthread_rwlock_unlock(inode_cache_lock + g);

    //find replace position
    int pos = -1;
    uint64_t min = UINT64_MAX;
    pthread_rwlock_wrlock(inode_cache_lock + g);
    for (i = 0; i < Inode_Cache_GroupSize; ++i){
        if (inode_cache_atime[cache_g+i] == 0) {
            pos = i;
            break;
        }
        if (inode_cache_atime[cache_g+i] < min){
            min = inode_cache_atime[cache_g+i];
            pos = i;
        }
    }
    inode_cache_group_time[g] += 1;
    inode_cache_atime[cache_g+pos] = inode_cache_group_time[g];

    char* pmpool = (char*)(meta->pmpool);
    ind = (inode_cache + (cache_g+pos));

    uint64_t ptr = INODE_BLK_BEG + (uint64_t)sizeof(struct inode_ele) * (uint64_t)inode_id;
    
    memcpy(ind, pmpool+ptr, sizeof(struct inode_ele));
    _mm_mfence();
    
    ind->inode_id = inode_id;

    pthread_rwlock_unlock(inode_cache_lock + g);
    // fprintf(stdout, "inode #%d loaded.\n", inode_id);

    return ind;
}


int change_inode_struct(struct perproc_fsmeta* meta, int inode_id, int beg, int len, char* buf){
    if (beg + len >= INODE_SIZE)
        return -1;
    //require a cache hit
    int g = inode_id & Inode_Cache_Mask;
    uint32_t cache_g = g * Inode_Cache_GroupSize;
    int i;
    int pos = -1;

    pthread_rwlock_t* inode_cache_lock = (pthread_rwlock_t*)(meta->inode_cache_lock);
    uint64_t* inode_cache_atime = (uint64_t*)(meta->inode_cache_atime);
    struct inode_ele* inode_cache = (struct inode_ele*)(meta->inode_cache);
    uint64_t* inode_cache_group_time = (uint64_t*)(meta->inode_cache_gtime);

    pthread_rwlock_rdlock(inode_cache_lock + g);
    for (i = 0; i < Inode_Cache_GroupSize; ++i){
        if (inode_cache_atime[cache_g+i] == 0) continue;
        if (inode_cache[cache_g+i].inode_id == inode_id){
            pos = i;
            pthread_rwlock_unlock(inode_cache_lock + g);
            break;
        }
    }
    if (pos < 0){
        fprintf(stderr, "[ERR]: Inode cache miss but inode-store called.\n");
        pthread_rwlock_unlock(inode_cache_lock + g);
        return -1;
    }
    pthread_rwlock_wrlock(inode_cache_lock + g);
    inode_cache_group_time[g] += 1;
    inode_cache_atime[cache_g+pos] = inode_cache_group_time[g];

    struct inode_ele* ind = (inode_cache + (cache_g+pos));

    memcpy((char*)ind + beg, buf, len);
    _mm_mfence();

    pthread_rwlock_unlock(inode_cache_lock + g);

    // fprintf(stdout, "inode #%d changed.\n", inode_id);
    return 0;
}


int stor_inode_struct(struct perproc_fsmeta* meta, int inode_id){
    //require a cache hit
    int g = inode_id & Inode_Cache_Mask;
    uint32_t cache_g = g * Inode_Cache_GroupSize;
    int i;
    int pos = -1;

    pthread_rwlock_t* inode_cache_lock = (pthread_rwlock_t*)(meta->inode_cache_lock);
    uint64_t* inode_cache_atime = (uint64_t*)(meta->inode_cache_atime);
    struct inode_ele* inode_cache = (struct inode_ele*)(meta->inode_cache);
    uint64_t* inode_cache_group_time = (uint64_t*)(meta->inode_cache_gtime);

    pthread_rwlock_rdlock(inode_cache_lock + g);
    for (i = 0; i < Inode_Cache_GroupSize; ++i){
        if (inode_cache_atime[cache_g+i] == 0) continue;
        if (inode_cache[cache_g+i].inode_id == inode_id){
            pos = i;
            pthread_rwlock_unlock(inode_cache_lock + g);
            break;
        }
    }
    if (pos < 0){
        // Here we should reload the inode in cache, but for simplicity, not implemented
        fprintf(stderr, "[ERR]: Inode cache miss but inode-store called.\n");
        pthread_rwlock_unlock(inode_cache_lock + g);
        return -1;
    }
    pthread_rwlock_wrlock(inode_cache_lock + g);
    inode_cache_group_time[g] += 1;
    inode_cache_atime[cache_g+pos] = inode_cache_group_time[g];

    char* pmpool = (char*)(meta->pmpool);
    uint64_t ptr = INODE_BLK_BEG + (uint64_t)inode_id * INODE_SIZE;
    struct inode_ele* ind = (inode_cache + (cache_g+pos));

    pmem_memcpy(pmpool+ptr, ind, sizeof(struct inode_ele),
        PMEM_F_MEM_NONTEMPORAL);
    _mm_mfence();

    pthread_rwlock_unlock(inode_cache_lock + g);
    // fprintf(stdout, "inode #%d stored.\n", inode_id);
    return 0;
}


// Note this func use allocated dram buff when given NULL as buff
void* load_slice_buff(struct perproc_fsmeta* meta, uint64_t addr, uint64_t lent, void* buff){
    char* pmpool = (char*)(meta->pmpool);
    if (!buff) buff = (char*)malloc(lent);
    // no need to lock anything since done before
    memcpy(buff, pmpool+addr, lent);
    _mm_mfence();

    return (void*)buff;
}


int stor_slice_buff(struct perproc_fsmeta* meta, void* buff, uint64_t addr, uint64_t lent){
    char* pmpool = (char*)(meta->pmpool);

    // Need to figure out which part addr belongs to
    if (addr >= INODE_BLK_BEG && addr < INODE_BLK_BEG + INODE_BLK_LENT){
        int inode_id = (addr - INODE_BLK_BEG) / INODE_SIZE;
        int beg = (addr - INODE_BLK_BEG) % INODE_SIZE;
        change_inode_struct(meta, inode_id, beg, lent, buff);
        return 0;
    }

    pmem_memcpy(pmpool+addr, buff, lent, PMEM_F_MEM_NONTEMPORAL);
    _mm_mfence();

    // no need to unlock data area since done before
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
        pthread_mutex_unlock(lock);
        fprintf(stderr, "[ERR]: NO more free inode.\n");
        return -1;
    }
    bm_arr[cur] = 1;
    pthread_mutex_unlock(lock);
    return cur;
}

// Keep in mind that a index block(kind of data block) is:
// 8 byte prev ptr, (SEG_PER_BLOCK * 2)*4 byte index, 8 byte next ptr

int inode_append_lent(struct perproc_fsmeta* meta, struct inode_ele* ind, uint64_t x){

    struct super_block* sb = (struct super_block*)(meta->sb_addr);
    uint8_t* blk_bm = (uint8_t*)(meta->block_bm_addr);
    char* pmpool = (char*)(meta->pmpool);

    if (ind->fstat_len + x <= ind->file_len) {
        ind->fstat_len += x;
        return 0;
    }

    uint64_t new_lent = ind->fstat_len + x;
    if (new_lent > sb->max_file_lent){
        fprintf(stderr, "[ERR]: file length overflow.\n");
        return -1;
    }

    pthread_rwlock_t* inode_lock = (pthread_rwlock_t*)(meta->inode_lock);
    pthread_rwlock_wrlock((inode_lock + ind->inode_id));

    uint32_t blknum_old = ind->file_len / DTBLK_SIZE;
    if (ind->file_len % DTBLK_SIZE) blknum_old += 1;
    uint32_t blknum_new = new_lent / DTBLK_SIZE;
    if (new_lent % DTBLK_SIZE) blknum_new += 1;

    if (ind->type_flag == 1){
        blknum_new -= (blknum_new % 256);
        blknum_new += 256;
    }
    uint32_t ablk = blknum_new - blknum_old;

    uint64_t tempres = 0;
    uint32_t beg_index = 0;
    uint32_t cur_lent = blknum_old;
    uint32_t pre_lent = 0;
    int index_t = ind->index_tail;

    uint64_t st_ptr = INODE_BLK_BEG + INODE_SIZE * (uint64_t)(ind->inode_id) + 32;
    if (index_t >= SEG_PER_BLOCK) {
        st_ptr = (uint64_t)(*(uint32_t*)(pmpool + st_ptr + (SEG_PER_BLOCK * 8) + 4));
        st_ptr = INODE_BLK_BEG + INODE_SIZE * st_ptr;
        index_t -= SEG_PER_BLOCK;
        while (index_t >= SEG_PER_BLOCK){
            st_ptr = *(uint64_t*)(pmpool + st_ptr + (SEG_PER_BLOCK * 8) + 8);
            index_t -= SEG_PER_BLOCK;
        }
        st_ptr += 8;
    }
    st_ptr += (index_t * 8);

    uint64_t* int64buf = (uint64_t*)malloc(8);
    uint64_t* int64buf2 = (uint64_t*)malloc(8);

    pthread_mutex_t *dtblk_bm_mutex;
    dtblk_bm_mutex = (pthread_mutex_t*)(meta->blkbm_lock_addr);

    while (ablk > 0){
        pre_lent = cur_lent;
        pthread_mutex_lock(dtblk_bm_mutex);
        tempres = get_next_newseg(blk_bm, &(sb->blk_bm_id), ablk, &cur_lent);
        pthread_mutex_unlock(dtblk_bm_mutex);

        if (!tempres){
            fprintf(stderr, "[ERR]: File system full-filled.\n");
            return -1;       
        }
        beg_index = (uint32_t)(tempres & (uint64_t)0x00000000ffffffff);
        // fprintf(stderr, "RUN INFO: newseg beg index: %d\n", beg_index);
        ablk -= (cur_lent - pre_lent);
        // To write {beg_, lent_} in index region
        *int64buf = tempres;
        
        stor_slice_buff(meta, int64buf, st_ptr, 8);
        
        ind->index_tail += 1;
        if (ind->index_tail % SEG_PER_BLOCK == 0){
            *int64buf2 = st_ptr + 8;
            fprintf(stderr, "[WARN]: linked index block needed.\n");
            int new_id = get_new_BMpos(sb, blk_bm, dtblk_bm_mutex, 'b');
            if (new_id < 0){
                fprintf(stderr, "[ERR]: File system full-filled.\n");
                return -1;       
            }
            *int64buf = INODE_BLK_BEG + (uint64_t)INODE_SIZE * new_id;
            // write next addr
            stor_slice_buff(meta, int64buf, st_ptr+8, 8);
            st_ptr = *int64buf;
            // write prev addr
            stor_slice_buff(meta, int64buf2, st_ptr, 8);
        }
        st_ptr += 8;
    }

    if (cur_lent != blknum_new) 
        fprintf(stderr, "[ERR]: Meet insufficient blocks.\n");

    ind->version += 1;
    ind->file_len = (uint64_t)blknum_new * DTBLK_SIZE;
    ind->fstat_len += x;
    
    stor_inode_struct(meta, ind->inode_id);
    pthread_rwlock_unlock((inode_lock + ind->inode_id));
    free(int64buf);
    free(int64buf2);

    return 0;
}


int file_inode_write(struct perproc_fsmeta* meta, struct inode_ele* ind, uint64_t beg, 
                    uint64_t len, char* buf){

    uint64_t file_lent = ind->fstat_len;
    
    if (beg + len > file_lent){
        fprintf(stderr, "[ERR]: File write exceed total length.\n");
        return -1;
    }
    uint32_t beg_blk = beg / DTBLK_SIZE;
    uint32_t beg_off = beg % DTBLK_SIZE;

    pthread_rwlock_t* inode_lock = (pthread_rwlock_t*)(meta->inode_lock);

    pthread_rwlock_rdlock((inode_lock + (ind->inode_id)));

    uint64_t index_addr = 32 + (uint64_t)(ind->inode_id) * INODE_SIZE + INODE_BLK_BEG;
    // Here is inode WRITE
    uint32_t* index_arr = ind->index;
    int canfree = 0;
    uint32_t tail_index = ind->index_tail;

    if (tail_index >= SEG_PER_BLOCK){
        index_addr = 8 + (uint64_t)(ind->next_index) * INODE_SIZE + INODE_BLK_BEG;
    }
    uint32_t left = 0;
    uint32_t right = tail_index - 1;

    if (right >= SEG_PER_BLOCK) right = SEG_PER_BLOCK - 1;
    int beg_seg = arr_binary_find1011(index_arr, left, right, beg_blk);
    // Here is inode WRITE
    uint32_t prev_blkblk = 0;
    while (beg_seg < 0){
        tail_index -= SEG_PER_BLOCK;
        prev_blkblk = index_arr[SEG_PER_BLOCK*2-1];
        if (!canfree) canfree = 1;
        else free(index_arr);
        index_arr = (uint32_t*)load_slice_buff(meta, index_addr, SEG_PER_BLOCK*8+8, NULL);
        if (tail_index >= SEG_PER_BLOCK){
            index_addr = 8 + (uint64_t)(index_arr[SEG_PER_BLOCK*2+1]) * INODE_SIZE + INODE_BLK_BEG;
        }
        left = 0;
        right = tail_index - 1;
        if (right >= SEG_PER_BLOCK) right = SEG_PER_BLOCK - 1;
        beg_seg = arr_binary_find1011(index_arr, left, right, beg_blk);
    }
    uint32_t prev_blk = prev_blkblk;
    if (beg_seg > 0) prev_blk = prev_blkblk + index_arr[beg_seg * 2 - 1];
    uint32_t writing_blk = index_arr[beg_seg*2] + (beg_blk - prev_blk);
    uint64_t writing_ptr = DTBLK_BEG + (uint64_t)writing_blk*DTBLK_SIZE + beg_off;
    uint64_t seg_end = DTBLK_BEG + 
            (uint64_t)(index_arr[beg_seg*2+1] - prev_blk + index_arr[beg_seg*2]) * DTBLK_SIZE;
    // Here is inode WRITE
    uint64_t buf_ptr = 0;
    while (len > 0){
        uint64_t w_len = seg_end - writing_ptr;
        if (w_len > len) w_len = len;
        stor_slice_buff(meta, buf+buf_ptr, writing_ptr, w_len);
        len -= w_len;
        buf_ptr += w_len;
        if (len == 0) break;
        beg_seg += 1;
        if (beg_seg >= SEG_PER_BLOCK){
            index_addr = 8 + (uint64_t)(index_arr[SEG_PER_BLOCK*2+1]) * INODE_SIZE + INODE_BLK_BEG;
            prev_blkblk = index_arr[SEG_PER_BLOCK*2-1];
            if (!canfree) canfree = 1;
            else free(index_arr);
            index_arr = (uint32_t*)load_slice_buff(meta, index_addr, SEG_PER_BLOCK*8+8, NULL);
            beg_seg = 0;
        }
        if (beg_seg > 0) prev_blk = prev_blkblk + index_arr[beg_seg * 2 - 1];
        writing_ptr = DTBLK_BEG + (uint64_t)(index_arr[beg_seg*2]) * DTBLK_SIZE;
        seg_end = DTBLK_BEG + (uint64_t)(index_arr[beg_seg*2+1] - 
                prev_blk + index_arr[beg_seg*2]) * DTBLK_SIZE;
    }
    _mm_mfence();
    
    pthread_rwlock_unlock((inode_lock + (ind->inode_id)));

    if (!canfree) canfree = 1;
    else free(index_arr);

    return 0;
}


// Note this func use allocated dram buf if dest is NULL
char* file_inode_read(struct perproc_fsmeta* meta, struct inode_ele* ind, uint64_t beg, 
                    uint64_t len, char* dest){

    uint64_t file_lent = ind->fstat_len;
    
    if (beg + len > file_lent){
        fprintf(stderr, "[ERR]: File read exceed total length.\n");
        return NULL;
    }

    char *buf;
    if (dest == NULL){
        if (ind->fstat_len == 0 || len == 0){
            buf = (char*)malloc(1);
            buf[0] = '\0';
            return buf;
        }
        buf = (char*)malloc(len + 1);
        buf[len] = '\0';
    }
    else {
        if (ind->fstat_len == 0 || len == 0){
            dest[0] = '\0';
            return dest;
        }
        buf = dest;
    }

    uint32_t beg_blk = beg / DTBLK_SIZE;
    uint32_t beg_off = beg % DTBLK_SIZE;

    pthread_rwlock_t* inode_lock = (pthread_rwlock_t*)(meta->inode_lock);

    pthread_rwlock_rdlock((inode_lock + (ind->inode_id)));

    uint64_t index_addr = 32 + (uint64_t)(ind->inode_id) * INODE_SIZE + INODE_BLK_BEG;
    // Here is inode READ
    uint32_t* index_arr = ind->index;
    int canfree = 0;

    uint32_t tail_index = ind->index_tail;

    if (tail_index >= SEG_PER_BLOCK){
        index_addr = 8 + (uint64_t)(ind->next_index) * INODE_SIZE + INODE_BLK_BEG;
    }
    uint32_t left = 0;
    uint32_t right = tail_index - 1;

    if (right >= SEG_PER_BLOCK) right = SEG_PER_BLOCK - 1;
    int beg_seg = arr_binary_find1011(index_arr, left, right, beg_blk);
    // Here is inode READ
    uint32_t prev_blkblk = 0;
    while (beg_seg < 0){
        tail_index -= SEG_PER_BLOCK;
        prev_blkblk = index_arr[SEG_PER_BLOCK*2-1];
        if (!canfree) canfree = 1;
        else free(index_arr);
        index_arr = (uint32_t*)load_slice_buff(meta, index_addr, SEG_PER_BLOCK*8+8, NULL);
        if (tail_index >= SEG_PER_BLOCK){
            index_addr = 8 + (uint64_t)(index_arr[SEG_PER_BLOCK*2+1]) * INODE_SIZE + INODE_BLK_BEG;
        }
        left = 0;
        right = tail_index - 1;
        if (right >= SEG_PER_BLOCK) right = SEG_PER_BLOCK - 1;
        beg_seg = arr_binary_find1011(index_arr, left, right, beg_blk);
    }
    uint32_t prev_blk = prev_blkblk;
    if (beg_seg > 0) prev_blk = prev_blkblk + index_arr[beg_seg * 2 - 1];
    uint32_t writing_blk = index_arr[beg_seg*2] + (beg_blk - prev_blk);
    uint64_t reading_ptr = DTBLK_BEG + (uint64_t)writing_blk*DTBLK_SIZE + beg_off;
    uint64_t seg_end = DTBLK_BEG + 
            (uint64_t)(index_arr[beg_seg*2+1] - prev_blk + index_arr[beg_seg*2]) * DTBLK_SIZE;
    // Here is inode READ
    uint64_t buf_ptr = 0;
    while (1){
        uint64_t r_len = seg_end - reading_ptr;
        if (r_len > len) r_len = len;
        load_slice_buff(meta, reading_ptr, r_len, buf+buf_ptr);
        len -= r_len;
        buf_ptr += r_len;
        if (len == 0) break;
        beg_seg += 1;
        if (beg_seg >= SEG_PER_BLOCK){
            index_addr = 8 + (uint64_t)(index_arr[SEG_PER_BLOCK*2+1]) * INODE_SIZE + INODE_BLK_BEG;
            prev_blkblk = index_arr[SEG_PER_BLOCK*2-1];
            if (!canfree) canfree = 1;
            else free(index_arr);
            index_arr = (uint32_t*)load_slice_buff(meta, index_addr, SEG_PER_BLOCK*8+8, NULL);
            beg_seg = 0;
        }
        if (beg_seg > 0) prev_blk = prev_blkblk + index_arr[beg_seg * 2 - 1];
        reading_ptr = DTBLK_BEG + (uint64_t)(index_arr[beg_seg*2]) * DTBLK_SIZE;
        seg_end = DTBLK_BEG + (uint64_t)(index_arr[beg_seg*2+1] - 
                prev_blk + index_arr[beg_seg*2]) * DTBLK_SIZE;
    }
    _mm_mfence();
    pthread_rwlock_unlock((inode_lock + (ind->inode_id)));

    if (!canfree) canfree = 1;
    else free(index_arr);

    return buf;
}


int create_file_ininode(struct perproc_fsmeta* meta, struct inode_ele* ind, const char* filename, 
                    int isDir_flag){
    
    uint32_t ind_type = ind->type_flag;

    char* pmpool = (char*)(meta->pmpool);

    if (ind_type != 1){
        fprintf(stderr, "[ERR]: Trying to create file in non-dir file, failed.\n");
        return -1;
    }

    struct super_block* sb = (struct super_block*)(meta->sb_addr);
    uint8_t* inode_bm = (uint8_t*)(meta->inode_bm_addr);
    uint8_t* dir_bm = (uint8_t*)(meta->dentry_bm_addr);
    uint8_t* blk_bm = (uint8_t*)(meta->block_bm_addr);

    uint64_t ind_len = ind->fstat_len;
    uint32_t get_pos = 0;
    char* content = file_inode_read(meta, ind, 0, ind_len, NULL);
    
    // Deal with the content and add a term
    char* new_cont = dirbuf_add_term(content, filename, &ind_len, &get_pos);
    
    if (!new_cont){
        fprintf(stderr, "[WARN]: Found file(dir) with same name, abort.\n");
        free(content);
        return -1;
    }
    int64_t add_len = ind_len - ind->fstat_len;

    uint64_t diff_pos = 0;
    while (diff_pos < ind->fstat_len){
        if (content[diff_pos] != new_cont[diff_pos]) break;
        diff_pos += 1;
    }
    uint64_t tail_zeronum = 0;
    while (tail_zeronum < ind_len){
        if (new_cont[ind_len-1-tail_zeronum] != 0) break;
        tail_zeronum += 1;
    }
    if (tail_zeronum > 8) tail_zeronum -= 8;
    else tail_zeronum = 0;
    // fprintf(stderr, "RUN_INFO: at file create, ind_len is %lu, diff_len is %lu\n", ind_len, ind_len-diff_pos-tail_zeronum);
    
    if (add_len > 0) inode_append_lent(meta, ind, (uint64_t)add_len);

    file_inode_write(meta, ind, diff_pos, ind_len-diff_pos-tail_zeronum, new_cont + diff_pos);

    free(content);
    free(new_cont);

    pthread_mutex_t* indbm_lock = (pthread_mutex_t*)(meta->indbm_lock_addr);
    pthread_mutex_t* dirbm_lock = (pthread_mutex_t*)(meta->dirbm_lock_addr);

    // Deal with dentry tree
    int inode_newid = get_new_BMpos(sb, inode_bm, indbm_lock, 'i');
    struct inode_ele* newind;
    
    pthread_rwlock_t* inode_lock = (pthread_rwlock_t*)(meta->inode_lock);
    
    pthread_rwlock_wrlock(inode_lock + inode_newid);
    newind = load_inode_struct(meta, inode_newid);
    memset(newind, 0, sizeof(struct inode_ele));
    newind->version = 1;
    if (isDir_flag) newind->type_flag = 1;
    else newind->type_flag = 2;
    newind->inode_id = inode_newid;

    int dentry_newid = get_new_BMpos(sb, dir_bm, dirbm_lock, 'd');

    pthread_rwlock_t* dentry_lock = (pthread_rwlock_t*)(meta->dentry_lock);
    pthread_rwlock_wrlock(dentry_lock + dentry_newid);
    load_dentry_struct(meta, dentry_newid);

    struct dentry_ele* dir_cache = (struct dentry_ele*)(meta->dir_cache);

    struct dentry_ele* newdir = dir_cache + dentry_newid;
    memset(newdir, 0, sizeof(struct dentry_ele));
    newdir->version = 1;
    newdir->dentry_id = dentry_newid;
    // Notice that newind and newdir will be persisted after 
    // filling their contents, see tail on the bottom of this function.

    newind->dentry_head_id = (uint32_t)dentry_newid;
    newdir->inode_ptr = INODE_BLK_BEG + (uint64_t)inode_newid*INODE_SIZE;
    pthread_rwlock_unlock(inode_lock + inode_newid);

    int dir_id = (int)(ind->dentry_head_id);
    uint64_t ptr;

    // Upmove this lock op, for multi-creation consistency
    // pthread_rwlock_wrlock((dentry_lock + ind->dentry_head_id));

    load_dentry_struct(meta, dir_id);

    if (get_pos == 1) {
        (dir_cache+dir_id)->child_head = DENTRY_TREE_BEG + 
                            (uint64_t)(newdir->dentry_id)*DENTRY_SIZE;
        stor_dentry_struct(meta, dir_id);
    }
    else {
        int i;
        ptr = (dir_cache + dir_id)->child_head;
        dir_id = (ptr - DENTRY_TREE_BEG) / DENTRY_SIZE;
        load_dentry_struct(meta, dir_id);
        for (i = 1; i < get_pos-1; ++i){
            ptr = (dir_cache + dir_id)->next_sib;
            dir_id = (ptr - DENTRY_TREE_BEG) / DENTRY_SIZE;
            load_dentry_struct(meta, dir_id);
        }
        (dir_cache + dir_id)->next_sib = DENTRY_TREE_BEG + 
                            (uint64_t)(newdir->dentry_id)*DENTRY_SIZE;
        newdir->prev_sib = DENTRY_TREE_BEG + 
                            (uint64_t)((dir_cache + dir_id)->dentry_id)*DENTRY_SIZE;
        newdir->parent_ptr = ind->dentry_head_id*(uint64_t)DENTRY_SIZE + DENTRY_TREE_BEG;
        stor_dentry_struct(meta, dir_id);
    }
    pthread_rwlock_unlock(dentry_lock + dentry_newid);

    stor_inode_struct(meta, inode_newid);
    stor_dentry_struct(meta, dentry_newid);

    // Upmove this lock op, for multi-creation consistency
    // pthread_rwlock_unlock((dentry_lock + ind->dentry_head_id));

    return 0;
}


struct hashmap_s* filenamehash;
int* hashvalues;


int32_t get_inodeid_ofpath(struct perproc_fsmeta* meta, const char* pathname, int thr_id){
    int path_len = strlen(pathname);
    if (path_len <= 0) {
        fprintf(stderr, "[ERR]: file path cannot be empty!\n");
        return -1;
    }
    if (pathname[0] != '/') {
        fprintf(stderr, "[ERR]: file path should be start with '/'!\n");
        return -1;
    }
    int ret = -1;

    void* ahash_onflie = NULL;
    if (pathname[path_len-1] == '/') {
        ahash_onflie = hashmap_get(&(filenamehash[thr_id]), pathname, path_len);
        // fprintf(stderr, "RUN_INFO: hash %d query %s, result %p.\n", thr_id, pathname, ahash_onflie);
    }
    if (ahash_onflie != NULL){
        int32_t retval = *((int32_t*)ahash_onflie);
        return retval;
    }

    char* split_dirname = (char*)malloc(path_len);
    int split_dirlen = path_len - 1;
    while (pathname[split_dirlen-1] != '/') split_dirlen -= 1;
    memcpy(split_dirname, pathname, split_dirlen);
    split_dirname[split_dirlen] = 0;

    int p1 = 0;
    int p2 = 0;
    char* cur_name = (char*)malloc(path_len+1);
    memset(cur_name, 0, path_len+1);
    struct inode_ele* ind;
    int type = 1;

    ahash_onflie = hashmap_get(&(filenamehash[thr_id]), split_dirname, split_dirlen);
    // fprintf(stderr, "RUN_INFO: hash %d query %s, result %p.\n", thr_id, split_dirname, ahash_onflie);
    if (ahash_onflie != NULL){
        ret = *((int32_t*)ahash_onflie);
        p1 = split_dirlen;
        p2 = p1;
    }

    pthread_rwlock_t* inode_lock = (pthread_rwlock_t*)(meta->inode_lock);
    struct dentry_ele* dir_cache = (struct dentry_ele*)(meta->dir_cache);
    
    while (p1 < path_len){
        while (pathname[p2] != '/'){
            p2 += 1;
            if (p2 >= path_len) break;
        }
        if (p2 >= path_len) p2 -= 1;
        int cur_len = p2 - p1 + 1;
        memcpy(cur_name, pathname+p1, cur_len);
        cur_name[cur_len] = '\0';

        if (ret < 0) ret = 0;
        else{
            pthread_rwlock_rdlock(inode_lock + ret);
            ind = load_inode_struct(meta, ret);
            if (ind->type_flag != type) {
                fprintf(stderr, "[ERR]: in-consistent type flag on inode %u. which is %d but %d required.\n", 
                        ind->inode_id, ind->type_flag, type);
                pthread_rwlock_unlock(inode_lock + ret);
                return -1;
            }
            char* tmpbuf = file_inode_read(meta, ind, 0, ind->fstat_len, NULL);
            if (cur_name[cur_len-1] == '/') {
                cur_name[cur_len-1] = '\0';
                type = 1;
            }
            else type = 2;
            int32_t pos = find_fname_inbuf(tmpbuf, cur_name, ind->fstat_len);
            free(tmpbuf);
            if (pos < 0){
                // fprintf(stderr, "[ERR]: No such file (%s) in inode %u.\n", cur_name, ind->inode_id);
                pthread_rwlock_unlock(inode_lock + ret);
                return -7;
            }

            // pos means which child is the "cur_name" of the ind
            uint64_t tra;
            int dir_id = (int)(ind->dentry_head_id);
            load_dentry_struct(meta, dir_id);
            
            tra = (dir_cache+dir_id)->child_head;
            dir_id = (tra - DENTRY_TREE_BEG) / DENTRY_SIZE;
            load_dentry_struct(meta, dir_id);

            int j;
            for (j = 0; j < pos-1; ++j){
                tra = (dir_cache+dir_id)->next_sib;
                dir_id = (tra - DENTRY_TREE_BEG) / DENTRY_SIZE;
                load_dentry_struct(meta, dir_id);
            }
            tra = (dir_cache+dir_id)->inode_ptr;
            
            pthread_rwlock_unlock(inode_lock + ret);
            
            ret = (tra - INODE_BLK_BEG) / INODE_SIZE;

        }
        p1 = p2 + 1;
        p2 = p1;
    }

    int filehashcons = hashmap_put(&(filenamehash[thr_id]), split_dirname, split_dirlen, &(hashvalues[ind->inode_id]));
    if (filehashcons != 0) {fprintf(stderr, "[ERR]: filenamehash insert FAILED.\n");}

    ind = load_inode_struct(meta, ret);
    if (ind->type_flag != type) {
        // fprintf(stderr, "RUN_LOG: wrong-assumption on type of inode %u.\n", ind->inode_id);
        return -3;
    }

    free(cur_name);

    if (pathname[path_len-1] == '/'){
        char* stored_pathname = (char*)malloc(path_len+1);
        memcpy(stored_pathname, pathname, path_len);
        stored_pathname[path_len] = 0;
        int filehashcons = hashmap_put(&(filenamehash[thr_id]), stored_pathname, path_len, &(hashvalues[ret]));
        if (filehashcons != 0) {fprintf(stderr, "[ERR]: filenamehash insert FAILED.\n");}
        // else fprintf(stderr, "DEBUG_INFO: filenamehash #%d insert %s, value %d.\n", thr_id, pathname, hashvalues[ret]);
    }

    return ret;
}



const int QXDFS_FDBASE = 12345678;
const int MAXI_PROC_USER = 16;
const int MAXI_NOFILE = 1000000;

struct perproc_fsmeta* meta_list = NULL;
struct perproc_fsmeta* self_meta = NULL;
uint32_t* total_fdarr = NULL;
uint32_t* self_fdarr = NULL;

void* get_struct_meta(struct perproc_fsmeta* meta){
    if (pthread_mutex_trylock(&(meta->lock)) == 0){
        pthread_mutex_unlock(&(meta->lock));
    }else{
        fprintf(stderr, "[ERR]: the meta entry locked before init\n");
        return NULL;
    }
    meta->nouse = 0;

    int pool_fd = open(PMEM_POOL_NAME, O_RDWR);
    char* pool = (char*)mmap(NULL, PMEM_POOL_LENT, PROT_WRITE, MAP_SHARED, pool_fd, 0);
    meta->pmpool = (uint64_t)pool;
    close(pool_fd);

    int sb_ipcfd = shm_open("QZLSHM_SB", O_RDWR, 0666);
    struct super_block* sb = mmap(NULL, sizeof(struct super_block), PROT_WRITE, MAP_SHARED, sb_ipcfd, 0);
    meta->sb_addr = (uint64_t)sb;
    close(sb_ipcfd);

    int indbm_ipcfd = shm_open("QZLSHM_INDBM", O_RDWR, 0666);
    uint8_t* indbm_arr = mmap(NULL, sizeof(uint8_t) * INODE_MAP_SIZE, PROT_WRITE, MAP_SHARED, indbm_ipcfd, 0);
    meta->inode_bm_addr = (uint64_t)indbm_arr;
    close(indbm_ipcfd);

    int dirbm_ipcfd = shm_open("QZLSHM_DIRBM", O_RDWR, 0666);
    uint8_t* dirbm_arr = mmap(NULL, sizeof(uint8_t) * DENTRY_MAP_SIZE, PROT_WRITE, MAP_SHARED, dirbm_ipcfd, 0);
    meta->dentry_bm_addr = (uint64_t)dirbm_arr;
    close(dirbm_ipcfd);

    int blkbm_ipcfd = shm_open("QZLSHM_BLKBM", O_RDWR, 0666);
    uint8_t* blkbm_arr = mmap(NULL, sizeof(uint8_t) * DTBLK_MAP_SIZE, PROT_WRITE, MAP_SHARED, blkbm_ipcfd, 0);
    meta->block_bm_addr = (uint64_t)blkbm_arr;
    close(blkbm_ipcfd);

    pthread_mutex_t *super_block_mutex;
    int sblk_ipcfd = shm_open("QZLSHM_SBLK", O_RDWR, 0666);
    super_block_mutex = mmap(NULL, sizeof(pthread_mutex_t), PROT_WRITE, MAP_SHARED, sblk_ipcfd, 0);
    meta->sb_lock_addr = (uint64_t)super_block_mutex;
    close(sblk_ipcfd);

    pthread_mutex_t *inode_bm_mutex;
    int indbmlk_ipcfd = shm_open("QZLSHM_INDBMLK", O_RDWR, 0666);
    inode_bm_mutex = mmap(NULL, sizeof(pthread_mutex_t), PROT_WRITE, MAP_SHARED, indbmlk_ipcfd, 0);
    meta->indbm_lock_addr = (uint64_t)inode_bm_mutex;
    close(indbmlk_ipcfd);

    pthread_mutex_t *dentry_bm_mutex;
    int dirbmlk_ipcfd = shm_open("QZLSHM_DIRBMLK", O_RDWR, 0666);
    dentry_bm_mutex = mmap(NULL, sizeof(pthread_mutex_t), PROT_WRITE, MAP_SHARED, dirbmlk_ipcfd, 0);
    meta->dirbm_lock_addr = (uint64_t)dentry_bm_mutex;
    close(dirbmlk_ipcfd);

    pthread_mutex_t *dtblk_bm_mutex;
    int blkbmlk_ipcfd = shm_open("QZLSHM_BLKBMLK", O_RDWR, 0666);
    dtblk_bm_mutex = mmap(NULL, sizeof(pthread_mutex_t), PROT_WRITE, MAP_SHARED, blkbmlk_ipcfd, 0);
    meta->blkbm_lock_addr = (uint64_t)dtblk_bm_mutex;
    close(blkbmlk_ipcfd);

    int dir_cache_ipcfd = shm_open("QZLSHM_DIR_CACHE", O_RDWR, 0666);
    struct dentry_ele* dir_cache;
    dir_cache = mmap(NULL, sizeof(struct dentry_ele) * MAX_DENTRY, PROT_WRITE, MAP_SHARED, dir_cache_ipcfd, 0);
    meta->dir_cache = (uint64_t)dir_cache;
    close(dir_cache_ipcfd);

    int dir_cache_flag_ipcfd = shm_open("QZLSHM_DIR_CACHE_FLAG", O_RDWR, 0666);
    uint8_t *dir_cache_flag;
    dir_cache_flag = mmap(NULL, sizeof(uint8_t) * MAX_DENTRY, PROT_WRITE, MAP_SHARED, dir_cache_flag_ipcfd, 0);
    meta->dir_cache_flag = (uint64_t)dir_cache_flag;
    close(dir_cache_flag_ipcfd);

    int inode_cache_ipcfd = shm_open("QZLSHM_INODE_CACHE", O_RDWR, 0666);
    struct inode_ele* inode_cache;
    inode_cache = mmap(NULL, sizeof(struct inode_ele) * 
        (Inode_Cache_GroupNum * Inode_Cache_GroupSize), PROT_WRITE, MAP_SHARED, inode_cache_ipcfd, 0);
    meta->inode_cache = (uint64_t)inode_cache;
    close(inode_cache_ipcfd);

    int inode_cache_atime_ipcfd = shm_open("QZLSHM_INODE_CACHE_ATIME", O_RDWR, 0666);
    uint64_t* inode_cache_atime;
    inode_cache_atime = mmap(NULL, sizeof(uint64_t) * 
        (Inode_Cache_GroupNum * Inode_Cache_GroupSize), PROT_WRITE, MAP_SHARED, inode_cache_atime_ipcfd, 0);
    meta->inode_cache_atime = (uint64_t)inode_cache_atime;
    close(inode_cache_atime_ipcfd);

    int inode_cache_gtime_ipcfd = shm_open("QZLSHM_INODE_CACHE_GTIME", O_RDWR, 0666);
    uint64_t* inode_cache_group_time;
    inode_cache_group_time = mmap(NULL, sizeof(uint64_t)*Inode_Cache_GroupNum, 
        PROT_WRITE, MAP_SHARED, inode_cache_gtime_ipcfd, 0);
    meta->inode_cache_gtime = (uint64_t)inode_cache_group_time;
    close(inode_cache_gtime_ipcfd);

    int inode_cache_lock_ipcfd = shm_open("QZLSHM_INODE_CACHE_LOCK", O_RDWR, 0666);
    pthread_rwlock_t* inode_cache_lock;
    inode_cache_lock = mmap(NULL, sizeof(pthread_rwlock_t)*Inode_Cache_GroupNum, 
        PROT_WRITE, MAP_SHARED, inode_cache_lock_ipcfd, 0);
    meta->inode_cache_lock = (uint64_t)inode_cache_lock;
    close(inode_cache_lock_ipcfd);

    int dentry_lock_ipcfd = shm_open("QZLSHM_DENTRY_LOCK", O_RDWR, 0666);
    pthread_rwlock_t* dentry_lock;
    dentry_lock = mmap(NULL, sizeof(pthread_rwlock_t) * MAX_DENTRY, PROT_WRITE, MAP_SHARED, dentry_lock_ipcfd, 0);
    meta->dentry_lock = (uint64_t)dentry_lock;
    close(dentry_lock_ipcfd);

    int inode_lock_ipcfd = shm_open("QZLSHM_INODE_LOCK", O_RDWR, 0666);
    pthread_rwlock_t* inode_lock;
    inode_lock = mmap(NULL, sizeof(pthread_rwlock_t) * MAX_INODE, PROT_WRITE, MAP_SHARED, inode_lock_ipcfd, 0);
    meta->inode_lock = (uint64_t)inode_lock;
    close(inode_lock_ipcfd);

    uint64_t* file_locks = (uint64_t*)malloc(sizeof(uint64_t) * 2 * MAX_INODE);
    memset(file_locks, 0, sizeof(uint64_t) * 2 * MAX_INODE);
    meta->fileblk_lockarr = (uint64_t)file_locks;

    int inflight_lock_ipcfd = shm_open("QZLSHM_INFLIGHT_LOCK", O_RDWR, 0666);
    struct inflight_lockinfo* inflight_locks;
    inflight_locks = mmap(NULL, sizeof(struct inflight_lockinfo) * USER_NUM * MAXCL_THNUM, PROT_WRITE, MAP_SHARED, inflight_lock_ipcfd, 0);
    meta->allinflightlock = (uint64_t)inflight_locks;
    close(inflight_lock_ipcfd);

    return NULL;
}


void* clear_struct_meta(struct perproc_fsmeta* meta){
    meta->pid = 0;
    pthread_mutex_unlock(&(meta->lock));
    meta->nouse = 0;
    munmap((void*)(meta->pmpool), PMEM_POOL_LENT);
    munmap((void*)(meta->sb_addr), sizeof(struct super_block));
    munmap((void*)(meta->inode_bm_addr), sizeof(uint8_t) * INODE_MAP_SIZE);
    munmap((void*)(meta->dentry_bm_addr), sizeof(uint8_t) * DENTRY_MAP_SIZE);
    munmap((void*)(meta->block_bm_addr), sizeof(uint8_t) * DTBLK_MAP_SIZE);
    munmap((void*)(meta->sb_lock_addr), sizeof(pthread_mutex_t));
    munmap((void*)(meta->indbm_lock_addr), sizeof(pthread_mutex_t));
    munmap((void*)(meta->dirbm_lock_addr), sizeof(pthread_mutex_t));
    munmap((void*)(meta->blkbm_lock_addr), sizeof(pthread_mutex_t));
    munmap((void*)(meta->dir_cache), sizeof(struct dentry_ele) * MAX_DENTRY);
    munmap((void*)(meta->dir_cache_flag), sizeof(uint8_t) * MAX_DENTRY);
    munmap((void*)(meta->inode_cache), sizeof(struct inode_ele) * 
        (Inode_Cache_GroupNum * Inode_Cache_GroupSize));
    munmap((void*)(meta->inode_cache_atime), sizeof(uint64_t) * 
        (Inode_Cache_GroupNum * Inode_Cache_GroupSize));
    munmap((void*)(meta->inode_cache_gtime), sizeof(uint64_t)*Inode_Cache_GroupNum);
    munmap((void*)(meta->inode_cache_lock), sizeof(pthread_rwlock_t)*Inode_Cache_GroupNum);
    munmap((void*)(meta->dentry_lock), sizeof(pthread_rwlock_t) * MAX_DENTRY);
    munmap((void*)(meta->inode_lock), sizeof(pthread_rwlock_t) * MAX_INODE);
    munmap((void*)(meta->allinflightlock), sizeof(struct inflight_lockinfo) * USER_NUM * MAXCL_THNUM);
    free((void*)(meta->fileblk_lockarr));

    return NULL;
}


void* inproc_init_fsmeta(){
    if (!meta_list){
        int meta_list_fd = shm_open("QZLSHM_METALIST", O_RDWR, 0644);
        meta_list = mmap(NULL, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER, 
            PROT_WRITE, MAP_SHARED, meta_list_fd, 0);
        struct perproc_fsmeta* main_meta = &(meta_list[0]);

        int a = pthread_mutex_lock(&(main_meta->lock));
        if (a != 0) {
            fprintf(stderr, "***Jesus***, the mutex lock returns %d\n", a);
        }

        main_meta->nouse += 1;
        if (main_meta->nouse >= MAXI_PROC_USER) {
            fprintf(stderr, "[ERR]: max proc user reached!\n");
            return (void*)main_meta;
        }

        int i = 0;
        while (meta_list[i].pid != 0) ++i;
        int self_pid = getpid();
        meta_list[i].pid = self_pid;

        self_meta = meta_list + i;
        get_struct_meta(self_meta);
        pthread_mutex_unlock(&(main_meta->lock));

        int appfd_array_ipcfd = shm_open("QZLSHM_APPFD_ARRAY", O_RDWR, 0666);
        total_fdarr = mmap(NULL, sizeof(int)*MAXI_PROC_USER*MAXI_NOFILE, PROT_WRITE, MAP_SHARED, 
            appfd_array_ipcfd, 0);
        self_fdarr = total_fdarr + (i*MAXI_NOFILE);
        fprintf(stderr, "OK, PID %d got metalist %d\n", self_pid, i);

        pthread_mutexattr_t mutex_attr;
        pthread_mutexattr_init(&mutex_attr);
        pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED);
        memset(&(self_meta->lock), 0, sizeof(pthread_mutex_t));
        pthread_mutex_init(&(self_meta->lock), &mutex_attr);
    }
    return NULL;
}


void* inproc_free_fsmeta(){
    memset(self_fdarr, 0, sizeof(int)*MAXI_NOFILE);
    self_fdarr = NULL;
    munmap(total_fdarr, sizeof(int)*MAXI_PROC_USER*MAXI_NOFILE);
    total_fdarr = NULL;

    clear_struct_meta(self_meta);
    self_meta = NULL;

    struct perproc_fsmeta* main_meta = &(meta_list[0]);
    pthread_mutex_lock(&(main_meta->lock));

    if (main_meta->nouse <= 1) {
        fprintf(stderr, "[ERR]: no proc user but droping!\n");
        return (void*)main_meta;
    }
    main_meta->nouse -= 1;
    pthread_mutex_unlock(&(main_meta->lock));

    munmap(meta_list, sizeof(struct perproc_fsmeta)*MAXI_PROC_USER);
    meta_list = NULL;
    fprintf(stderr, "RUN_INFO: per-proc fs meta removed.\n");
    return NULL;
}


// Note that / dir is exsiting all the time
int xxx_create_file(char* pathname, int thr_id){
    inproc_init_fsmeta();
    struct perproc_fsmeta* meta = self_meta;

    int testR = get_inodeid_ofpath(meta, pathname, thr_id);
    if (testR >= 0) {
        struct inode_ele* ind = load_inode_struct(meta, testR);
        ind->fstat_len = 0;
        stor_inode_struct(meta, testR);
        return 0;
    }

    int namelen = strlen(pathname);
    int p;
    for (p = namelen-1; p >= 0; --p)
        if (pathname[p] == '/') break;
    char *dirname = (char*)malloc(p+2);
    memcpy(dirname, pathname, p+1);
    dirname[p+1] = '\0';
    char *fname = (char*)malloc(namelen - p);
    memcpy(fname, pathname+p+1, namelen - p - 1);
    fname[namelen - p - 1] = '\0';

    int r = get_inodeid_ofpath(meta, dirname, thr_id);
    // fprintf(stdout, "RUN_LOG: %s's result inode id: %d\n", dirname, r);
    struct inode_ele* ind = load_inode_struct(meta, r);

    // Lock the parent dir here
    pthread_rwlock_t* dentry_lock = (pthread_rwlock_t*)(meta->dentry_lock);
    pthread_rwlock_wrlock((dentry_lock + ind->dentry_head_id));
    create_file_ininode(meta, ind, fname, 0);
    pthread_rwlock_unlock((dentry_lock + ind->dentry_head_id));

    free(dirname);

    return 0;
}

// Note this function serve as dir creation
int xxx_create_dir(char* pathname, int thr_id){
    inproc_init_fsmeta();
    struct perproc_fsmeta* meta = self_meta;

    int namelen = strlen(pathname);

    char* round_pathn = (char*)malloc(namelen + 2);
    memcpy(round_pathn, pathname, namelen);
    round_pathn[namelen] = '/';
    round_pathn[namelen+1] = '\0';
    int testR = get_inodeid_ofpath(meta, round_pathn, thr_id);
    free(round_pathn);
    if (testR >= 0) return 0;

    int p;
    for (p = namelen-1; p >= 0; --p)
        if (pathname[p] == '/') break;
    char *parname = (char*)malloc(p+2);
    memcpy(parname, pathname, p+1);
    parname[p+1] = '\0';
    char *dname = (char*)malloc(namelen - p);
    memcpy(dname, pathname+p+1, namelen - p - 1);
    dname[namelen - p - 1] = '\0';

    int r = get_inodeid_ofpath(meta, parname, thr_id);
    // fprintf(stdout, "RUN_LOG: %s's result inode id: %d\n", parname, r);

    struct inode_ele* ind = load_inode_struct(meta, r);

    // Lock the parent dir here
    pthread_rwlock_t* dentry_lock = (pthread_rwlock_t*)(meta->dentry_lock);
    pthread_rwlock_wrlock((dentry_lock + ind->dentry_head_id));
    create_file_ininode(meta, ind, dname, 1);
    pthread_rwlock_unlock((dentry_lock + ind->dentry_head_id));

    free(parname);
    free(dname);

    return 0;
}

// This could also be used in write function (when needed)
int xxx_append_file(const char* pathname, uint64_t x, int inodeid_if, int thr_id){
    inproc_init_fsmeta();
    struct perproc_fsmeta* meta = self_meta;

    int r;
    if (inodeid_if >= 0) r = inodeid_if;
    else {
        r = get_inodeid_ofpath(meta, pathname, thr_id);
        // fprintf(stdout, "RUN_LOG: append, %s's result inode id: %d\n", pathname, r);
    }

    struct inode_ele* ind = load_inode_struct(meta, r);
    pthread_rwlock_t* inode_lock = (pthread_rwlock_t*)(meta->inode_lock);

    if (ind->type_flag == 1){
        fprintf(stderr, "[ERR]: no user admission to append dir-file.\n");
        return 1;
    }
    else if (ind->type_flag == 2){

        char* lock_shmname = (char*)malloc(40);
        memset(lock_shmname, 0, 40);
        sprintf(lock_shmname, "QZLSHM_ISXLOCK_%u", r);
        struct myISXlockStruct* lock;

        uint32_t blknum_old = ind->fstat_len / DTBLK_SIZE;
        if (ind->fstat_len % DTBLK_SIZE) blknum_old += 1;
        uint32_t blknum_new = (ind->fstat_len + x) / DTBLK_SIZE;
        if ((ind->fstat_len + x) % DTBLK_SIZE) blknum_new += 1;

        uint64_t enclose_old = 1;
        while (enclose_old < blknum_old) enclose_old *= 2;
        uint64_t enclose_new = 1;
        while (enclose_new < blknum_new) enclose_new *= 2;
        
        // pthread_mutex_lock(&(self_meta->lock));
        // fprintf(stderr, "RUN INFO: xxx_Append, BEFORE unlock 1\n");
        int a;
        if (thr_id < MAXCL_THNUM) {
            a = pthread_rwlock_unlock((inode_lock + r));
            if (a != 0) {
                perror("rwlock un:");
                exit(1);
            }
        }
        // fprintf(stderr, "RUN INFO: xxx_Append, BEFORE lock 1\n");
        a = pthread_rwlock_wrlock((inode_lock + r)); 
        if (a != 0) {
            perror("rwlock wr:");
            exit(1);
        }
        // pthread_mutex_unlock(&(self_meta->lock));

        // fprintf(stderr, "RUN INFO: xxx_Append, BEFORE destroy\n");
        uint64_t locknum = ((uint64_t*)(self_meta->fileblk_lockarr))[r * 2 + 1];
        if (locknum > 0){
            uint64_t lockptr = ((uint64_t*)(self_meta->fileblk_lockarr))[r * 2];
            lock = (struct myISXlockStruct*)lockptr;
            if (lockptr <= 0){
                fprintf(stderr, "[ERR]: Need to destroy blklock BUT not mmaped.\n");
                exit(1);
            }
            destroy_dtblk_lock(lock, lock_shmname, locknum);
        }
        // fprintf(stderr, "RUN INFO: xxx_Append, BEFORE create\n");
        if (blknum_new > 0){
            lock = create_dtblk_lock(lock_shmname, enclose_new);
            ((uint64_t*)(self_meta->fileblk_lockarr))[r * 2] = (uint64_t)lock;
            ((uint64_t*)(self_meta->fileblk_lockarr))[r * 2 + 1] = enclose_new;
        }
        else {
            fprintf(stderr, "[ERR]: File append called with x=0.\n");
            exit(1);
        }

        // inode_lock operations merged with below
    }
    else {
        perror("invalid ind->type_flag");
        exit(0);
    }
    
    // pthread_mutex_lock(&(self_meta->lock));
    // fprintf(stderr, "RUN INFO: xxx_Append, BEFORE unlock 2\n");
    pthread_rwlock_unlock((inode_lock + r));
    inode_append_lent(meta, ind, x);
    // fprintf(stderr, "RUN INFO: xxx_Append, BEFORE lock 2\n");
    if (thr_id < MAXCL_THNUM) {
        pthread_rwlock_rdlock((inode_lock + r));
    }
    // pthread_mutex_unlock(&(self_meta->lock));

    return 0;
}


struct openfile_ret{
    int ord_fd;
    int file_mode;
    uint64_t stat_size;
};


struct openfile_ret* qxdfs_open_file(const char* fname, int o_flag, int thr_id){
    
    inproc_init_fsmeta();
    
    // Deal with different o_flag: 0(file), 1(creat), 2(stat|REMOVED)
    if (o_flag != 0 && o_flag != 1) {
        fprintf(stderr, "[ERR]: open while stating file %s.\n", fname);
        return NULL;
    }
    int r = get_inodeid_ofpath(self_meta, fname, thr_id);
    if (r == -7){
        if (o_flag) {
            xxx_create_file((char*)fname, thr_id);
            r = get_inodeid_ofpath(self_meta, fname, thr_id);
        }
        else{
            fprintf(stderr, "[ERR]: opening a non-exist file %s, get %d.\n", fname, r);
            return NULL;
        }
    }
    else if (r < 0){
        fprintf(stderr, "[ERR]: opening a non-exist file %s.\n", fname);
        return NULL;
    }

    struct inode_ele* ind = load_inode_struct(self_meta, r);
    uint32_t blknum = ind->fstat_len / DTBLK_SIZE;
    if (ind->fstat_len % DTBLK_SIZE) blknum += 1;
    uint64_t locknum = 1;
    while (locknum < blknum) locknum *= 2;

    pthread_rwlock_t* inode_lock = (pthread_rwlock_t*)(self_meta->inode_lock);
    int a = pthread_rwlock_rdlock((inode_lock + r)); 
    if (a != 0) {
        perror("rwlock rd:");
        exit(1);
    }
    
    uint64_t lockptr = ((uint64_t*)(self_meta->fileblk_lockarr))[r * 2];
    if (ind->fstat_len > 0 && lockptr == 0) {
        // attach the file blk lock
        char* lock_shmname = (char*)malloc(40);
        memset(lock_shmname, 0, 40);
        sprintf(lock_shmname, "QZLSHM_ISXLOCK_%d", r);
        int flock_fd = shm_open(lock_shmname, O_RDWR, 0666);
        if (flock_fd <= 0){
            perror("shmopen in open:");
            exit(1);
        }
        struct myISXlockStruct* lock = mmap(NULL, sizeof(struct myISXlockStruct) * locknum * 2, 
                PROT_WRITE, MAP_SHARED, flock_fd, 0);
        if (lock == MAP_FAILED){
            perror("mmap in open:");
            exit(1);
        }
        ((uint64_t*)(self_meta->fileblk_lockarr))[r * 2] = (uint64_t)lock;
        ((uint64_t*)(self_meta->fileblk_lockarr))[r * 2 + 1] = locknum;
        free(lock_shmname);
        close(flock_fd);
    }

    int open_metalock_ret = pthread_mutex_lock(&(self_meta->lock));
    if (open_metalock_ret != 0){
        perror("open_metalock_ret on mutex:");
        exit(0);
    }

    if (self_meta->nouse >= MAXI_NOFILE) {
        fprintf(stderr, "[ERR]: max user-ofile reached!\n");
        pthread_mutex_unlock(&(self_meta->lock));
        return NULL;
    }
    self_meta->nouse += 1;

    if (o_flag) ind->fstat_len = 0;

    int j = 0;
    while(self_fdarr[j] != 0) ++j;
    self_fdarr[j] = r;
    pthread_mutex_unlock(&(self_meta->lock));

    struct openfile_ret* ret = (struct openfile_ret*)malloc(sizeof(struct openfile_ret));
    ret->ord_fd = (QXDFS_FDBASE + j);
    ret->file_mode = 0;
    ret->stat_size = ind->fstat_len;
    return ret;
}


struct openfile_ret* qxdfs_stat_file(char* fname, int o_flag, int thr_id){
    // fprintf(stderr, "RUN_INFO: stat called %s arrive.\n", fname);
    inproc_init_fsmeta();
    int namelen = strlen(fname);
    if (fname[namelen-1] == '/') {
        fname[namelen-1] = '\0';
        namelen -= 1;
    }

    char* round_pathn = (char*)malloc(namelen + 2);
    memcpy(round_pathn, fname, namelen);
    round_pathn[namelen] = '/';
    round_pathn[namelen+1] = '\0';
    int testR = get_inodeid_ofpath(self_meta, round_pathn, thr_id);
    free(round_pathn);

    struct openfile_ret* ret = (struct openfile_ret*)malloc(sizeof(struct openfile_ret));
    // fprintf(stderr, "RUN_INFO: stat called on a name: %s, result %d.\n", fname, testR);

    if (testR == -3){
        testR = get_inodeid_ofpath(self_meta, fname, thr_id);
        // fprintf(stderr, "RUN_INFO: stat called on a file %s, result %d.\n", fname, testR);
        ret->ord_fd = testR;
        ret->file_mode = 0;
        struct inode_ele* ind = load_inode_struct(self_meta, testR);
        ret->stat_size = ind->fstat_len;
        return ret;
    }
    if (testR >= 0) {
        // fprintf(stderr, "RUN_INFO: stat called on a dir %s, result %d.\n", fname, testR);
        ret->ord_fd = testR;
        ret->file_mode = 1;
        struct inode_ele* ind = load_inode_struct(self_meta, testR);
        ret->stat_size = ind->fstat_len;
        return ret;
    }
    ret->ord_fd = testR;
    ret->file_mode = -1;
    ret->stat_size = 0;
    return ret;
}


int qxdfs_close_file(int fd, int thr_id){
    int realfd = fd - QXDFS_FDBASE;
    // fprintf(stderr, "DEBUG_INFO: closing fd #%d!\n", fd); 

    // detach the file blk lock
    int r = self_fdarr[realfd];

    uint64_t locknum = ((uint64_t*)(self_meta->fileblk_lockarr))[r * 2 + 1];
    uint64_t lock = ((uint64_t*)(self_meta->fileblk_lockarr))[r * 2];
    
    if (lock == 0 && locknum != 0){
        fprintf(stderr, "[ERR]: Boiled fileblk_lockarr lock addr at thr %d iid %d close\n", thr_id, r);
        exit(1);
    }
    
    int close_metalock_ret = pthread_mutex_lock(&(self_meta->lock));
    if (close_metalock_ret != 0){
        perror("close_metalock_ret on mutex:");
        exit(0);
    }

    if (self_meta->nouse == 0){
        fprintf(stderr, "[WARN]: 0 user-ofile but closing..\n");
        pthread_mutex_unlock(&(self_meta->lock));
        return -1;
    }
    self_meta->nouse -= 1;

    self_fdarr[realfd] = 0;
    pthread_mutex_unlock(&(self_meta->lock));

    pthread_rwlock_t* inode_lock = (pthread_rwlock_t*)(self_meta->inode_lock);
    int a = pthread_rwlock_unlock((inode_lock + r)); 
    if (a != 0) {
        perror("rwlock un:");
        exit(1);
    }

    return 0;
}


ssize_t qxdfs_pwrite_file(int fd, const void* buf, size_t len, off_t beg, int thr_id){
    int realfd = fd - QXDFS_FDBASE;
    if (!meta_list) perror("QXDFS write however no meta-in-proc.\n");

    int iid = self_fdarr[realfd];
    struct inode_ele* ind = load_inode_struct(self_meta, iid);
    if (ind->fstat_len < beg+len){
        fprintf(stderr, "RUN_LOG: Writing on file inode#%d overlength, append.\n", iid);
        xxx_append_file(NULL, (beg+len) - ind->fstat_len, ind->inode_id, thr_id);
    }

    // data lock
    uint64_t lockptr = ((uint64_t*)(self_meta->fileblk_lockarr))[ind->inode_id * 2];
    uint64_t locknum = ((uint64_t*)(self_meta->fileblk_lockarr))[ind->inode_id * 2 + 1];
    struct myISXlockStruct* lock = (struct myISXlockStruct*)lockptr;
    int *lockids = get_blklock_list(beg, beg+len-1, (int)locknum);

    // keep asking for lock, because this func is for local thread write
    int lockopret = -1;
    while (lockopret < 0) lockopret = wrlock_blklock_list(lock, lockids);

    file_inode_write(self_meta, ind, beg, len, (char*)buf);
    wrUnlock_blklock_list(lock, lockids);
    free(lockids);

    return (ssize_t)len;
}

ssize_t qxdfs_pread_file(int fd, void* buf, size_t len, off_t beg, int thr_id){
    int realfd = fd - QXDFS_FDBASE;
    if (!meta_list) perror("QXDFS read however no meta-in-proc.\n");

    int iid = self_fdarr[realfd];
    struct inode_ele* ind = load_inode_struct(self_meta, iid);
    if (ind->fstat_len < beg+len){
        fprintf(stderr, "[ERR]: Reading on file inode#%d overlength.\n", iid);
        return 0;
    }

    uint64_t lockptr = ((uint64_t*)(self_meta->fileblk_lockarr))[ind->inode_id * 2 + thr_id * MAX_INODE * 2];
    uint64_t locknum = ((uint64_t*)(self_meta->fileblk_lockarr))[ind->inode_id * 2 + thr_id * MAX_INODE * 2 + 1];
    struct myISXlockStruct* lock = (struct myISXlockStruct*)lockptr;
    int *lockids = get_blklock_list(beg, beg+len-1, (int)locknum);

    // keep asking for lock, because this func is for local thread read
    int lockopret = -1;
    while (lockopret < 0) lockopret = rdlock_blklock_list(lock, lockids);

    file_inode_read(self_meta, ind, beg, len, (char*)buf);
    rdUnlock_blklock_list(lock, lockids);
    free(lockids);

    return (ssize_t)len;
}


uint64_t* getaddrlist_fromqfs(int fd, uint64_t offset, uint64_t length, int thr_id){
    // the returned list is filled with (beg_addr, len_byte) pairs 
    uint64_t* ret = (uint64_t*)malloc(sizeof(uint64_t)*(CPY_LENRPC_RESLIST + 1));
    memset(ret, 0, sizeof(uint64_t)*CPY_LENRPC_RESLIST);
    int ret_ptr = 0;

    // extract from qfs read/write function
    int realfd = fd - QXDFS_FDBASE;
    if (!meta_list) perror("QXDFS queried however no meta-in-proc.\n");

    int iid = self_fdarr[realfd];
    struct inode_ele* ind = load_inode_struct(self_meta, iid);
    if (ind->fstat_len < offset+length){
        // fprintf(stderr, "RUN_INFO: Quering on file inode#%d overlength, append\n", iid);
        xxx_append_file(NULL, (offset+length) - ind->fstat_len, ind->inode_id, thr_id);
        ret[CPY_LENRPC_RESLIST] = 0xffffffffffff;
        // fprintf(stderr, "RUN_INFO: Quering on file inode#%d append success\n", iid);
    }

    
    struct perproc_fsmeta* meta = self_meta;
    uint64_t beg = offset;
    uint64_t len = length;
    uint64_t file_lent = ind->fstat_len;

    uint32_t beg_blk = beg / DTBLK_SIZE;
    uint32_t beg_off = beg % DTBLK_SIZE;

    uint64_t index_addr = 32 + (uint64_t)(ind->inode_id) * INODE_SIZE + INODE_BLK_BEG;
    uint32_t* index_arr = ind->index;
    int canfree = 0;

    uint32_t tail_index = ind->index_tail;

    if (tail_index >= SEG_PER_BLOCK){
        index_addr = 8 + (uint64_t)(ind->next_index) * INODE_SIZE + INODE_BLK_BEG;
    }
    uint32_t left = 0;
    uint32_t right = tail_index - 1;

    if (right >= SEG_PER_BLOCK) right = SEG_PER_BLOCK - 1;
    int beg_seg = arr_binary_find1011(index_arr, left, right, beg_blk);
    
    uint32_t prev_blkblk = 0;
    while (beg_seg < 0){
        tail_index -= SEG_PER_BLOCK;
        prev_blkblk = index_arr[SEG_PER_BLOCK*2-1];
        if (!canfree) canfree = 1;
        else free(index_arr);
        index_arr = (uint32_t*)load_slice_buff(meta, index_addr, SEG_PER_BLOCK*8+8, NULL);
        if (tail_index >= SEG_PER_BLOCK){
            index_addr = 8 + (uint64_t)(index_arr[SEG_PER_BLOCK*2+1]) * INODE_SIZE + INODE_BLK_BEG;
        }
        left = 0;
        right = tail_index - 1;
        if (right >= SEG_PER_BLOCK) right = SEG_PER_BLOCK - 1;
        beg_seg = arr_binary_find1011(index_arr, left, right, beg_blk);
    }
    uint32_t prev_blk = prev_blkblk;
    if (beg_seg > 0) prev_blk = prev_blkblk + index_arr[beg_seg * 2 - 1];
    uint32_t writing_blk = index_arr[beg_seg*2] + (beg_blk - prev_blk);
    uint64_t reading_ptr = DTBLK_BEG + (uint64_t)writing_blk*DTBLK_SIZE + beg_off;
    uint64_t seg_end = DTBLK_BEG + 
            (uint64_t)(index_arr[beg_seg*2+1] - prev_blk + index_arr[beg_seg*2]) * DTBLK_SIZE;
    
    while (1){
        uint64_t r_len = seg_end - reading_ptr;
        if (r_len > len) r_len = len;
        
        if (ret_ptr * 2 + 1 >= CPY_LENRPC_RESLIST){
            fprintf(stderr, "[ERR]: RPC buffer is too small to load this addr list.\n");
            return NULL;
        }
        ret[ret_ptr * 2] = reading_ptr;
        ret[ret_ptr * 2 + 1] = r_len;
        ret_ptr += 1;

        len -= r_len;
        if (len == 0) break;
        beg_seg += 1;
        if (beg_seg >= SEG_PER_BLOCK){
            index_addr = 8 + (uint64_t)(index_arr[SEG_PER_BLOCK*2+1]) * INODE_SIZE + INODE_BLK_BEG;
            prev_blkblk = index_arr[SEG_PER_BLOCK*2-1];
            if (!canfree) canfree = 1;
            else free(index_arr);
            index_arr = (uint32_t*)load_slice_buff(meta, index_addr, SEG_PER_BLOCK*8+8, NULL);
            beg_seg = 0;
        }
        if (beg_seg > 0) prev_blk = prev_blkblk + index_arr[beg_seg * 2 - 1];
        reading_ptr = DTBLK_BEG + (uint64_t)(index_arr[beg_seg*2]) * DTBLK_SIZE;
        seg_end = DTBLK_BEG + (uint64_t)(index_arr[beg_seg*2+1] - 
                prev_blk + index_arr[beg_seg*2]) * DTBLK_SIZE;
    }

    if (!canfree) canfree = 1;
    else free(index_arr);

    return ret;
}


// for rw_flag, 1 as read, 2 as write; for on_off, 1 as lock, 2 as unlock
int userquery_lockop(int thr_num, int fd, int probiid, uint64_t offset, uint64_t length, uint8_t rw_flag, uint8_t on_off){
    int iid;
    if (!meta_list) perror("QXDFS lock-operation while no meta-in-proc.\n");

    int proc_order = (self_meta - meta_list) - 1;
    if (proc_order < 0) perror("QXDFS metalist broken.");

    if (probiid < 0) {
        int realfd = fd - QXDFS_FDBASE;
        iid = self_fdarr[realfd];
    }
    else 
        iid = probiid;
    
    uint64_t locknum = ((uint64_t*)(self_meta->fileblk_lockarr))[iid * 2 + 1];
    uint64_t lockptr = ((uint64_t*)(self_meta->fileblk_lockarr))[iid * 2];
    struct myISXlockStruct* lock = (struct myISXlockStruct*)lockptr;
    if (lockptr == 0) {
        fprintf(stderr, "\n\n\n ???? THIS is quite strange when no mmaped lock but query. ???? \n\n\n");
        // attach the file blk lock
        char* lock_shmname = (char*)malloc(40);
        memset(lock_shmname, 0, 40);
        sprintf(lock_shmname, "QZLSHM_ISXLOCK_%d", iid);
        int flock_fd = shm_open(lock_shmname, O_RDWR, 0666);
        if (flock_fd <= 0){
            perror("shmopen in lockop:");
            exit(1);
        }
        lock = mmap(NULL, sizeof(struct myISXlockStruct) * locknum * 2, 
                PROT_WRITE, MAP_SHARED, flock_fd, 0);
        if (lock == MAP_FAILED){
            perror("mmap in lockop:");
            exit(1);
        }
        ((uint64_t*)(self_meta->fileblk_lockarr))[iid * 2] = (uint64_t)lock;
        free(lock_shmname);
        close(flock_fd);
    }
    int *lockids = get_blklock_list(offset, offset+length-1, (int)locknum);

    struct inflight_lockinfo* inflight_locks = (struct inflight_lockinfo*)(self_meta->allinflightlock);
    struct inflight_lockinfo* main_inflight = inflight_locks + (MAXCL_THNUM * proc_order + thr_num);

    main_inflight->lostat = 0;
    if (on_off == 1){
        main_inflight->infs_iid = iid;
        main_inflight->rw_flag = rw_flag;
        main_inflight->beg = offset;
        main_inflight->length = length;
    }

    int lockopret = -1;
    if (rw_flag == 1 && on_off == 1)
        lockopret = rdlock_blklock_list(lock, lockids);
    else if (rw_flag == 1 && on_off == 2)
        lockopret = rdUnlock_blklock_list(lock, lockids);
    else if (rw_flag == 2 && on_off == 1)
        lockopret = wrlock_blklock_list(lock, lockids);
    else if (rw_flag == 2 && on_off == 2)
        lockopret = wrUnlock_blklock_list(lock, lockids);
    else{
        fprintf(stderr, "[ERR]: Invalid lock operation type.\n");
        return -1;
    }

    free(lockids);
    if (lockopret < 0){
        main_inflight->lostat = 2;
        return lockopret;
    }
    main_inflight->lostat = 1;
    return iid;
}


int userquery_inflightlock(int thr_num, char* copy_buf){
    if (!meta_list) perror("QXDFS query-operation while no meta-in-proc.\n");

    int proc_order = (self_meta - meta_list) - 1;
    if (proc_order < 0) perror("QXDFS metalist broken.");

    struct inflight_lockinfo* inflight_locks;
    inflight_locks = (struct inflight_lockinfo*)(self_meta->allinflightlock);
    struct inflight_lockinfo* main_inflight = inflight_locks + (MAXCL_THNUM * proc_order + thr_num);
    struct inflight_lockinfo* cur;

    char* target_buf = copy_buf;
    int i; int inflight_size = MAXCL_THNUM * USER_NUM;
    for (i = 0; i < inflight_size; ++i){
        cur = inflight_locks + i;
        if (cur == main_inflight) continue;
        if (cur->infs_iid == 0 || cur->lostat != 2) continue;
        memcpy(target_buf, cur, sizeof(struct inflight_lockinfo));
        target_buf += sizeof(struct inflight_lockinfo);
        if (target_buf - copy_buf > RPC_MR_SIZE - 16 - sizeof(struct inflight_lockinfo)) 
            break;
    }
    memset(target_buf, 0, 8);
    
    return 0;
}


#endif 
