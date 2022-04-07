#ifndef __DCACHE_MANAGER
#define __DCACHE_MANAGER


#include <stdint.h>
#include "dcache_rdmaconx.c"
#include "clpm_cache.c"
#include "thpool.h"
#include "conflux_AIagent.h"
#include "hashmapstr.h"


static const int config_byfile = 1;
struct lcache_logger* lcache = NULL;
struct ibdev_resc* resc = NULL;
struct ib_conx_info** conx_info_local = NULL;
struct conflux_aiachr* aiagent = NULL;
int GLOB_OPCNT = 0;
int Center_McnId = 7;
const int GEN_MR_FLAG = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
threadpool glob_thpool = NULL;


static int fortest_mcnID = -1;
static int fortest_ofile = -1;


const uint8_t Aggr_Flag = 1;
const uint64_t Threshold_ToRemote = 20 * (uint64_t)(1<<10);
float IoRatio_ToRemote = 0.55; 

const uint64_t n_iosize = 1024*2 * (uint64_t)(1 << 10); 
const uint64_t PROC_SEG = 320 * (uint64_t)(1 << 20);
const int PROC_NUM = 4;
const int TestTime_Sec = 44;


pid_t Current_ConfluxTid = 0;
pid_t Current_ConfluxTGid = 0;


int syncwait_allproc(int pn, int* stats){
    // int pcstat_shmfd = shm_open("QZLSHM_PCSTAT", O_RDWR, 0666);
    // int* stats = (int*)mmap(NULL, sizeof(int) * MAXCL_THNUM * 2, PROT_WRITE, MAP_SHARED, pcstat_shmfd, 0);

    // ensure all centers linked
    do{
        int sNUM = 0; int eNUM = 0;
        int i;
        for (i = 0; i < MCN_NUM; ++i){
            if (resc->procState[i] == 'S') sNUM += 1;
            if (resc->procState[i] == 'E') eNUM += 1;
        }
        if (sNUM == CENTER_NUM && eNUM == 0) break;
    }while(1);

    stats[pn] = 1;
    stats[MAXCL_THNUM + pn] = 0;

    // close(pcstat_shmfd);
    waitMiliSec(50);
    waitNanoSec(800);
    return 0;
}


void* freeall_conflux_handle(void* arg){
    fprintf(stderr, "A conflux handle free triggered at TID %d\n", Current_ConfluxTid);

    int halt_stat = user_waiting_halt(resc, conx_info_local);
    if (halt_stat == 1) return NULL;

    free_conflux_aiachr(aiagent);

    destroy_ibdev_resc(resc, 1);
    free_local_cache(lcache);
    thpool_destroy(glob_thpool);

    glob_thpool = NULL;
    Current_ConfluxTid = 0;
    Current_ConfluxTGid = 0;
    return NULL;
}


int getall_conflux_handles(){

    // if (glob_thpool != NULL) return 0;

    pid_t tid = syscall(__NR_gettid);
    if (tid == Current_ConfluxTid) return 0;
    Current_ConfluxTid = tid;

    pid_t tgid = getpid();
    Current_ConfluxTGid = tgid;

    fprintf(stderr, "Conflux Handles at TGID %d, TID %d.\n", tgid, tid);

    glob_thpool = thpool_init(8);

    if (config_byfile){
        FILE *config_file = fopen(conflux_config_file, "r");
        LPCACHE_POOLNAME = malloc(80);
        memset(LPCACHE_POOLNAME, 0, 80);
        fscanf(config_file, "%[^\n] ", LPCACHE_POOLNAME);
        fprintf(stdout, "RUN INFO: the pmem pool is at %s\n", LPCACHE_POOLNAME);
        MLX5_DEV_NAME = malloc(80);
        memset(MLX5_DEV_NAME, 0, 80);
        fscanf(config_file, "%[^\n] ", MLX5_DEV_NAME);
        fprintf(stdout, "RUN INFO: the rdma dev is %s\n", MLX5_DEV_NAME);
        char *mcnID_str = malloc(80);
        memset(mcnID_str, 0, 80);
        fscanf(config_file, "%[^\n] ", mcnID_str);
        fprintf(stdout, "RUN INFO: the IDstr is %s\n", mcnID_str);
        fclose(config_file);

        cluster_info_parser();

        fortest_mcnID = query_user_mcninfo(mcnID_str);
        if (fortest_mcnID < 0) exit(1);
    }

    struct caltime_nano* nano_timer = init_caltime_nano();

    lcache = create_pcache(1);

    resc = create_ibdev_resc(MLX5_DEV_NAME, lcache->pcache, 1, lcache->user_pn);
    // fprintf(stderr, "RUN_INFO: resc created at %p.\n", resc);
    conx_info_local = ibconx_info_init(resc, 1);
    user_tcp_polling(fortest_mcnID, resc, conx_info_local);
    
    aiagent = init_conflux_aiachr(lcache->user_pn, MAXCL_THNUM, lcache->aipart_stats, nano_timer);

    // in syncwait, one proc can wait for all servers' link, and all other procs
    syncwait_allproc(aiagent->pn, aiagent->stats);

    fortest_ofile = 0;
    start_caltime_nano(aiagent->nano_timer);

    return 0;
}


int execute_clientRW(struct ibdev_resc* resc, struct ib_conx_info** conx_info_local, 
                    struct rpc_respo* respo, 
                    struct lcache_logger* lcache,
                    threadpool tp,
                    char* app_rwbuf){
    int i;
    uint64_t beg, len;
    int server_id = respo->servr_mcnid;
    if (respo->op_name == 1){
        // means it is read operation
        uint64_t* segm_list = (uint64_t*)(respo->respo_cont);
        for (i = 0; i < LENRPC_RESLIST; i += 2){
            beg = segm_list[i];
            if (beg == 0) break;
            len = segm_list[i + 1];
            uint64_t beg_addr = beg - (beg % PCACHE_CHUNK_LEN);
            while (beg_addr < beg + len){
                // fprintf(stdout, "DEBUG_INFO: search offset %llu incache.\n", beg_addr);
                int64_t laddr = search_lcache(lcache, beg_addr);
                struct arg_insert_lcache *targ = NULL;
                if (laddr < 0) {
                    targ = (struct arg_insert_lcache*)malloc(sizeof(struct arg_insert_lcache));
                    targ->lcache = lcache;
                    targ->chunk_beg = beg_addr;
                    targ->resc = resc;
                    targ->conx_info_local = conx_info_local;
                    targ->main_mcn = server_id;

                    aiagent->stats[MAXCL_THNUM * 4 + aiagent->pn] = 12;
                    aiagent->stats[MAXCL_THNUM * 3 + aiagent->pn] = 12;
                    thpool_add_work(tp, (void*)&insert_lcache, (void*)targ);
                    // fprintf(stdout, "DEBUG_INFO: insert offset %llu incache.\n", beg_addr);
                }
                thpool_wait(tp);
                aiagent->stats[MAXCL_THNUM * 3 + aiagent->pn] = 0;
                aiagent->stats[MAXCL_THNUM * 4 + aiagent->pn] = 0;

                beg_addr += PCACHE_CHUNK_LEN;
                if (targ) free(targ);
            }

            // note here need to copy to real app-buffer 
            uint64_t end_addr = beg - (beg % PCACHE_CHUNK_LEN) + PCACHE_CHUNK_LEN;
            beg_addr = beg;
            char* app_addr = app_rwbuf;
            while (1){
                if (end_addr >= beg + len){
                    end_addr = beg + len;
                    int64_t loff = search_lcache(lcache, beg_addr);
                    int io_decision = APPLY_NVMMIO;
                    uint64_t io_size = end_addr - beg_addr;
                    int iolevel = upper_iolevel(io_size);
                    
                    // use random as a test, may be in real impl we should also use "some exploration" 
                    // if (Aggr_Flag) io_decision = random_decision(io_size, Threshold_ToRemote, IoRatio_ToRemote);
                    if (Aggr_Flag) io_decision = decideby_aiagent(aiagent, iolevel);
                    // io_decision = APPLY_RDMAIO;
                    
                    uint64_t t1 = stop_caltime_nano(aiagent->nano_timer);
                    
                    if (io_decision == APPLY_NVMMIO){
                        aiagent->stats[MAXCL_THNUM * 2 + aiagent->pn] = iolevel;
                        memcpy(app_addr, lcache->pcache + LOCAL_PCACHE_BEG + loff, (size_t)io_size);
                        aiagent->stats[MAXCL_THNUM * 2 + aiagent->pn] = 0;
                        aiagent->xdata_buf[*(aiagent->databuf_counter)] = iolevel - 1;
                    }
                    else if (io_decision == APPLY_RDMAIO){
                        aiagent->stats[MAXCL_THNUM * 4 + aiagent->pn] = iolevel;
                        // struct ibv_mr* app_mr = ibv_reg_mr(resc->pd, app_addr, io_size, GEN_MR_FLAG);
                        // app_lkey = app_mr->lkey;
                        // post_read(resc, conx_info_local, server_id, -1, (uint32_t)io_size, 0, beg_addr, (uint64_t)app_addr, app_lkey);
                        // ibpoll_completion(resc->procState, resc->cq_rw[server_id-1], CQE_RT_TIMEOUT, 1, 1);
                        // ibv_dereg_mr(app_mr);
                        post_read(resc, conx_info_local, server_id, -1, (uint32_t)io_size, 0, beg_addr, (uint64_t)(resc->chunk_cache), resc->chunkmr->lkey);
                        ibpoll_completion(resc->procState, resc->cq_rw[server_id-1], CQE_RT_TIMEOUT, 1, 1);
                        memcpy(app_addr, resc->chunk_cache, io_size);
                        aiagent->stats[MAXCL_THNUM * 4 + aiagent->pn] = 0;
                        aiagent->xdata_buf[*(aiagent->databuf_counter)] = DEVIO_CLASS + iolevel - 1;
                    }

                    uint64_t t2 = stop_caltime_nano(aiagent->nano_timer);
                    // record transform time data from nanosec to microsec
                    float scale_valy = valiolevel[iolevel - 1] / (float)io_size;
                    aiagent->ydata_buf[*(aiagent->databuf_counter)] = (float)(t2-t1) * scale_valy / 1000;
                    *(aiagent->databuf_counter) = (*(aiagent->databuf_counter) + 1) % PYSHM_ENTRY_NUM;

                    break;
                }
                int64_t loff = search_lcache(lcache, beg_addr);
                int io_decision = APPLY_NVMMIO;
                uint64_t io_size = end_addr - beg_addr;
                int iolevel = upper_iolevel(io_size);

                // use random as a test, may be in real impl we should also use "some exploration"
                // if (Aggr_Flag) io_decision = random_decision(io_size, Threshold_ToRemote, IoRatio_ToRemote);
                if (Aggr_Flag) io_decision = decideby_aiagent(aiagent, iolevel);
                // io_decision = APPLY_RDMAIO;
                
                uint64_t t1 = stop_caltime_nano(aiagent->nano_timer);
                
                if (io_decision == APPLY_NVMMIO){
                    aiagent->stats[MAXCL_THNUM * 2 + aiagent->pn] = iolevel;
                    memcpy(app_addr, lcache->pcache + LOCAL_PCACHE_BEG + loff, (size_t)io_size);
                    aiagent->stats[MAXCL_THNUM * 2 + aiagent->pn] = 0;
                    aiagent->xdata_buf[*(aiagent->databuf_counter)] = iolevel - 1;
                }
                else if (io_decision == APPLY_RDMAIO){
                    aiagent->stats[MAXCL_THNUM * 4 + aiagent->pn] = iolevel;
                    // struct ibv_mr* app_mr = ibv_reg_mr(resc->pd, app_addr, io_size, GEN_MR_FLAG);
                    // app_lkey = app_mr->lkey;
                    // post_read(resc, conx_info_local, server_id, -1, (uint32_t)io_size, 0, beg_addr, (uint64_t)app_addr, app_lkey);
                    // ibpoll_completion(resc->procState, resc->cq_rw[server_id-1], CQE_RT_TIMEOUT, 1, 1);
                    // ibv_dereg_mr(app_mr);
                    post_read(resc, conx_info_local, server_id, -1, (uint32_t)io_size, 0, beg_addr, (uint64_t)(resc->chunk_cache), resc->chunkmr->lkey);
                    ibpoll_completion(resc->procState, resc->cq_rw[server_id-1], CQE_RT_TIMEOUT, 1, 1);
                    memcpy(app_addr, resc->chunk_cache, io_size);
                    aiagent->stats[MAXCL_THNUM * 4 + aiagent->pn] = 0;
                    aiagent->xdata_buf[*(aiagent->databuf_counter)] = DEVIO_CLASS + iolevel - 1;
                }

                uint64_t t2 = stop_caltime_nano(aiagent->nano_timer);
                // record transform time data from nanosec to microsec
                float scale_valy = valiolevel[iolevel - 1] / (float)io_size;
                aiagent->ydata_buf[*(aiagent->databuf_counter)] = (float)(t2-t1) * scale_valy / 1000;
                *(aiagent->databuf_counter) = (*(aiagent->databuf_counter) + 1) % PYSHM_ENTRY_NUM;

                app_addr += (end_addr - beg_addr);
                beg_addr = end_addr;
                end_addr += PCACHE_CHUNK_LEN;
            }
        }
    }
    else if (respo->op_name == 2){
        // means it is write operation
        uint64_t* segm_list = (uint64_t*)(respo->respo_cont);
        for (i = 0; i < LENRPC_RESLIST; i += 2){
            beg = segm_list[i];
            if (beg == 0) break;
            len = segm_list[i + 1];
            uint64_t beg_addr = beg - (beg % PCACHE_CHUNK_LEN);
            while (beg_addr < beg + len){
                int64_t laddr = search_lcache(lcache, beg_addr);
                struct arg_insert_lcache *targ = NULL;
                if (laddr < 0) {
                    targ = (struct arg_insert_lcache*)malloc(sizeof(struct arg_insert_lcache));
                    targ->lcache = lcache;
                    targ->chunk_beg = beg_addr;
                    targ->resc = resc;
                    targ->conx_info_local = conx_info_local;
                    targ->main_mcn = server_id;

                    aiagent->stats[MAXCL_THNUM * 4 + aiagent->pn] = 12;
                    aiagent->stats[MAXCL_THNUM * 3 + aiagent->pn] = 12;
                    thpool_add_work(tp, (void*)&insert_lcache, (void*)targ);
                    // fprintf(stdout, "DEBUG_INFO: insert offset %llu incache.\n", beg_addr);
                }
                thpool_wait(tp);
                aiagent->stats[MAXCL_THNUM * 3 + aiagent->pn] = 0;
                aiagent->stats[MAXCL_THNUM * 4 + aiagent->pn] = 0;
                    
                beg_addr += PCACHE_CHUNK_LEN;
                if (targ) free(targ);
            }

            struct arg_update_redata* targ = NULL;
            
            if (app_rwbuf) {
                // This is write local
                uint64_t end_addr = beg - (beg % PCACHE_CHUNK_LEN) + PCACHE_CHUNK_LEN;
                beg_addr = beg;
                char* app_addr = app_rwbuf;
                while (1){
                    if (end_addr >= beg + len){
                        end_addr = beg + len;
                        int64_t loff = search_lcache(lcache, beg_addr);
                        char* dest = lcache->pcache + LOCAL_PCACHE_BEG + loff;
                        uint64_t nbytes = end_addr - beg_addr;

                        int iolevel = upper_iolevel(nbytes);
                        
                        aiagent->stats[MAXCL_THNUM * 3 + aiagent->pn] = iolevel;
                        pmem_memcpy(dest, app_addr, nbytes, PMEM_F_MEM_NONTEMPORAL);
                        asm volatile("sfence\n\t"::: "memory");
                        aiagent->stats[MAXCL_THNUM * 3 + aiagent->pn] = 0;

                        break;
                    }
                    int64_t loff = search_lcache(lcache, beg_addr);
                    char* dest = lcache->pcache + LOCAL_PCACHE_BEG + loff;
                    uint64_t nbytes = end_addr - beg_addr;

                    int iolevel = upper_iolevel(nbytes);
                    
                    aiagent->stats[MAXCL_THNUM * 3 + aiagent->pn] = iolevel;
                    pmem_memcpy(dest, app_addr, nbytes, PMEM_F_MEM_NONTEMPORAL);
                    asm volatile("sfence\n\t"::: "memory");
                    aiagent->stats[MAXCL_THNUM * 3 + aiagent->pn] = 0;

                    app_addr += (end_addr - beg_addr);
                    beg_addr = end_addr;
                    end_addr += PCACHE_CHUNK_LEN;
                }
            }
            else {
                // This is write back
                // fprintf(stderr, "RUN_INFO: Get kickout_leases before update remote\n");
                struct arg_update_redata* targ = (struct arg_update_redata*)malloc(sizeof(struct arg_update_redata));
                targ->lcache = lcache;
                targ->resc = resc;
                targ->conx_info_local = conx_info_local;
                targ->server_id = server_id;
                targ->beg = beg;
                targ->len = len;
                targ->app_addr = 0;
                targ->app_lkey = 0;
                aiagent->stats[MAXCL_THNUM * 5 + aiagent->pn] = 12;
                // thpool_add_work(tp, (void*)&update_redata, (void*)targ);
                update_redata((void*)targ);
            }
            // thpool_wait(tp); 
            // fprintf(stderr, "RUN_INFO: Get kickout_leases after update remote\n");
            if (targ) free(targ);

            aiagent->stats[MAXCL_THNUM * 5 + aiagent->pn] = 0;
            continue;
        }
    }
    else{
        fprintf(stderr, "[ERR]: Invalid op_name in excuting rpc_response.\n");
        return -1;
    }

    free(respo);
    return 0;
}



// For rw_flag: 1 is read, 2 is write
int kickout_leases(int mcnid, int rw_flag, int iid){
    struct rpc_respo* respo = NULL;
    struct lease_entry_periid* lease_entry;
    if (lcache->buffor_search_lease[0] >= 0) perror("[ERR]:kickout_leases_false_use");
    int old_lease_cnt;
    int new_lease_cnt;
    int j_lease;
    int j_buf;
    struct one_lease* cur_lease;

    if (rw_flag == 1){
        // This is for Read lease
        lease_entry = lcache->lease_groups_read + iid;
    }
    if (rw_flag == 2){
        // This is for Write lease
        lease_entry = lcache->lease_groups_write + iid;
    }

    old_lease_cnt = lease_entry->lease_cnt;
    void* tmpleasebuf = malloc(sizeof(struct one_lease)*old_lease_cnt);
    new_lease_cnt = old_lease_cnt + lcache->buffor_search_lease[0];
    if (new_lease_cnt < 0){
        perror("[ERR]: 0 old lease, BUT kickout");
        exit(1);
    }
    cur_lease = lease_entry->lease_list;

    char* rpc_buf = resc->rpc_buf[mcnid - 1];
    *((int*)(rpc_buf+0)) = -1;
    *((int*)(rpc_buf+4)) = 5;

    rpc_buf = resc->rpc_buf[mcnid - 1] + 8;
    memset(rpc_buf, 0, RPC_MR_SIZE - 8);
    j_lease = 0;

    int j_buf_cont = 0;
    // fprintf(stderr, "RUN_INFO: Get kickout_leases inproc will perform %d\n", old_lease_cnt - new_lease_cnt);
    for (j_buf = 2; j_buf-2 < old_lease_cnt - new_lease_cnt; ++j_buf){
        cur_lease = cur_lease + (lcache->buffor_search_lease[j_buf] - j_buf_cont);
        j_buf_cont = lcache->buffor_search_lease[j_buf];
        *((int*)rpc_buf) = iid;
        *((uint8_t*)(rpc_buf+4)) = (uint8_t)rw_flag;
        *((uint64_t*)(rpc_buf+8)) = cur_lease->start_off;
        *((uint64_t*)(rpc_buf+16)) = cur_lease->end_off - cur_lease->start_off;
        lease_entry->address_occupied -= (cur_lease->end_off - cur_lease->start_off);

        // fprintf(stderr, "RUN_INFO: Get kickout_leases inproc #%d before execute\n", j_buf-2);

        if (rw_flag == 2 && fileis_dirty[iid]){
            // We need a write back here
            respo = (struct rpc_respo*)malloc(sizeof(struct rpc_respo));
            respo->op_name = 2;
            respo->op_ordr = -1;
            respo->posib_iid = iid;
            respo->servr_mcnid = mcnid;
            respo->respo_cont = cur_lease->addr_list;
            execute_clientRW(resc, conx_info_local, respo, lcache, glob_thpool, 0);
        }
        free(cur_lease->addr_list);

        rpc_buf += 24;
        j_lease += 1;
        if (j_lease >= 40) {
            *((int*)rpc_buf) = -1;
            respo = issue_query(resc, mcnid, RPC_MR_SIZE);
            if ( !respo || (*((int*)(respo->respo_cont)) != 0) ) {
                fprintf(stderr, "[ERR]: Unlock query failed at bunch lease kickout.\n");
                return -1;
            }
            rpc_buf = resc->rpc_buf[mcnid - 1] + 8;
            memset(rpc_buf, 0, RPC_MR_SIZE - 8);
            j_lease = 0;

            free(respo->respo_cont);
            free(respo);
        }
        j_buf_cont += 1;
        int rest_lease_cnt = old_lease_cnt - j_buf_cont;
        if (rest_lease_cnt > 0) {
            memcpy(tmpleasebuf, cur_lease+1, sizeof(struct one_lease)*rest_lease_cnt);
            memcpy(cur_lease, tmpleasebuf, sizeof(struct one_lease)*rest_lease_cnt);
        }
    }
    *((int*)rpc_buf) = -1;
    respo = issue_query(resc, mcnid, RPC_MR_SIZE);
    if ( !respo || (*((int*)(respo->respo_cont)) != 0) ) {
        fprintf(stderr, "[ERR]: Unlock query failed at last lease kickout, iid %d, count %d\n", 
            iid, old_lease_cnt - new_lease_cnt);
        rpc_buf = resc->rpc_buf[mcnid - 1] + 8;
        fprintf(stderr, "[ERR_INFO]: flag: %d, start: %lu, length: %lu\n", rw_flag, 
            *((uint64_t*)(rpc_buf+8)), *((uint64_t*)(rpc_buf+16)) );
        return -1;
    }

    free(respo->respo_cont);
    free(respo);
    free(tmpleasebuf);

    lease_entry->lease_cnt = new_lease_cnt;
    return 0;
}


// This function id used for fsync-call
int leasewrite_backall(int mcnid, int iid){
    struct rpc_respo* respo = NULL;
    struct lease_entry_periid* lease_entry;
    
    int cur_lease_cnt;
    int j_buf;
    struct one_lease* cur_lease;

    lease_entry = lcache->lease_groups_write + iid;
    cur_lease_cnt = lease_entry->lease_cnt;
    cur_lease = lease_entry->lease_list;

    for (j_buf = 0; j_buf < cur_lease_cnt; ++j_buf){
        respo = (struct rpc_respo*)malloc(sizeof(struct rpc_respo));
        respo->op_name = 2;
        respo->op_ordr = -1;
        respo->posib_iid = iid;
        respo->servr_mcnid = mcnid;
        respo->respo_cont = cur_lease->addr_list;
        execute_clientRW(resc, conx_info_local, respo, lcache, glob_thpool, 0);
        cur_lease = cur_lease + 1;
    }

    return 0;
}


// o_flag for qdfs actually has only 3 values: 0(normal), 1(create), 2(stat)

struct openfile_ret* qdfs_client_open(char* filename, int o_flag){
    // First get handles (if not in space yet)
    getall_conflux_handles();

    struct rpc_query* query = (struct rpc_query*)malloc(sizeof(struct rpc_query));
    struct rpc_respo* respo;
    GLOB_OPCNT += 1;
    query->op_ordr = GLOB_OPCNT;
    query->op_name = 3;
    int center_mcn = Center_McnId;
    int qlength = generate_query_open(resc, center_mcn, query, filename, o_flag);
    respo = issue_query(resc, center_mcn, qlength);

    struct openfile_ret* ret = (struct openfile_ret*)malloc(sizeof(struct openfile_ret));
    ret->ord_fd = *((int*)(respo->respo_cont));
    // printf("RUN_INFO: Get %s's fd: %d\n", filename, fd);
    ret->file_mode = *((int*)(respo->respo_cont + 4));
    ret->stat_size = *((uint64_t*)(respo->respo_cont + 8));

    fortest_ofile += 1;
    free(query);

    return ret;
}


struct hashmap_s *dirnamehash = NULL;


struct openfile_ret* qdfs_client_stat(char* filename, int o_flag){
    // First get handles (if not in space yet)
    getall_conflux_handles();

    if (!dirnamehash){
        dirnamehash = (struct hashmap_s*)malloc(sizeof(struct hashmap_s));
        memset(dirnamehash, 0, sizeof(struct hashmap_s));
        int dirhashcons = hashmap_create((1<<15), dirnamehash);
        if (dirhashcons != 0) {fprintf(stderr, "[ERR]: dirnamehash establish FAILED.\n");}
    }

    int namelength = strlen(filename);
    if (NULL != hashmap_get(dirnamehash, filename, namelength)){
        // fprintf(stderr, "DEBUG INFO: dirstat on %s passed.\n", filename);
        struct openfile_ret* ret = (struct openfile_ret*)malloc(sizeof(struct openfile_ret));
        ret->ord_fd = 0;
        ret->file_mode = 1;
        ret->stat_size = 0;
        return ret;
    }

    struct rpc_query* query = (struct rpc_query*)malloc(sizeof(struct rpc_query));
    struct rpc_respo* respo;
    GLOB_OPCNT += 1;
    query->op_ordr = GLOB_OPCNT;
    query->op_name = 104;
    int center_mcn = Center_McnId;
    int qlength = generate_query_stat(resc, center_mcn, query, filename, o_flag);
    respo = issue_query(resc, center_mcn, qlength);

    struct openfile_ret* ret = (struct openfile_ret*)malloc(sizeof(struct openfile_ret));
    ret->ord_fd = *((int*)(respo->respo_cont));
    // printf("RUN_INFO: Get %s's fd: %d\n", filename, fd);
    ret->file_mode = *((int*)(respo->respo_cont + 4));
    ret->stat_size = *((uint64_t*)(respo->respo_cont + 8));

    if (ret->file_mode == 1){
        char* stored_pathname = (char*)malloc(namelength+1);
        memcpy(stored_pathname, filename, namelength);
        stored_pathname[namelength] = 0;
        int dirhashcons = hashmap_put(dirnamehash, stored_pathname, namelength, (void*)stored_pathname);
        if (dirhashcons != 0) {fprintf(stderr, "[ERR]: dirnamehash insert FAILED.\n");}
    }

    free(query);

    return ret;
}


int qdfs_client_close(int fd){
    
    struct rpc_query* query = (struct rpc_query*)malloc(sizeof(struct rpc_query));
    struct rpc_respo* respo;
    GLOB_OPCNT += 1;
    query->op_ordr = GLOB_OPCNT;
    query->op_name = 4;
    int center_mcn = Center_McnId;

    // Need to kickout all local write leases
    int tobe_evict = ((lcache->lease_groups_write) + lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE])->lease_cnt;
    if (tobe_evict > 0) {
        // fprintf(stderr, "DEBUG_INFO: Releasing %d write leases on close.\n", tobe_evict);
        lcache->buffor_search_lease[0] = 0 - tobe_evict;
        int i;
        for (i = 0; i < tobe_evict; ++i) lcache->buffor_search_lease[2 + i] = i;
        kickout_leases(center_mcn, 2, lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE]);
    }
    // fprintf(stderr, "RUN_INFO: Get kickout_leases passed: %d\n", fd);

    // Then formally query close
    int qlength = generate_query_close(resc, center_mcn, query, fd);
    respo = issue_query(resc, center_mcn, qlength);
    int closed = *((int*)(respo->respo_cont));
    // fprintf(stderr, "RUN_INFO: Get fd #%d close result: %d\n", fd, closed);

    lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE] = 0;

    fortest_ofile -= 1;
    if (fortest_ofile < 0) {
        fprintf(stderr, "[ERR]: No way! the fortest_ofile is negative.\n");
        return -1;
    }

    // Maybe we should comment this line when FileBench
    // if (fortest_ofile == 0) freeall_conflux_handle(NULL);

    free(query);

    free(respo->respo_cont);
    free(respo);
    return closed;
}


int qdfs_client_remove(char* filename){
    if (!dirnamehash){
        dirnamehash = (struct hashmap_s*)malloc(sizeof(struct hashmap_s));
        memset(dirnamehash, 0, sizeof(struct hashmap_s));
        int dirhashcons = hashmap_create((1<<15), dirnamehash);
        if (dirhashcons != 0) {fprintf(stderr, "[ERR]: dirnamehash establish FAILED.\n");}
    }

    int namelength = strlen(filename);
    if (NULL != hashmap_get(dirnamehash, filename, namelength)){
        fprintf(stderr, "[ERR]: try to remove dir %s.\n", filename);
        return -1;
    }
    struct openfile_ret* ret = (struct openfile_ret*)malloc(sizeof(struct openfile_ret));
    ret->ord_fd = 0;
    ret->file_mode = 1;
    ret->stat_size = 0;
    return ret->ord_fd;
}


ssize_t qdfs_client_pread(int fd, void* tgt_buf, size_t nbytes, off_t offset){
    struct rpc_query* query = (struct rpc_query*)malloc(sizeof(struct rpc_query));
    struct rpc_respo* respo = NULL;
    GLOB_OPCNT += 1;
    query->op_ordr = GLOB_OPCNT;
    int center_mcn = Center_McnId;
    // read
    query->op_name = 1;

    // Need to add meta-info cache, and query the cache here before asking pread-lock
    if (lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE] == 0) {
        int qlength = generate_query_pread(resc, center_mcn, query, fd, (uint64_t)offset, (uint64_t)nbytes);
        respo = issue_query(resc, center_mcn, qlength);
        if (!respo) {
            fprintf(stderr, "[ERR]: Read query failed at #%d.\n", query->op_ordr);
            exit(1);
            return -1;
        }
        lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE] = respo->posib_iid;
        if (*(uint64_t*)(respo->respo_cont + sizeof(uint64_t)*LENRPC_RESLIST) == 0xffffffffffff){
            (lcache->lease_groups_write + respo->posib_iid)->lease_cnt = 0;
            (lcache->lease_groups_write + respo->posib_iid)->address_occupied = 0;
            (lcache->lease_groups_read + respo->posib_iid)->lease_cnt = 0;
            (lcache->lease_groups_read + respo->posib_iid)->address_occupied = 0;
        }
    }

    search_lease(lcache, 2, lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE], offset, offset+nbytes);
    if (lcache->buffor_search_lease[0] < 0) {
        // need to kickout some lease
        // fprintf(stderr, "[WARN]: Lease need to kickout at QDFS user.\n");
        kickout_leases(center_mcn, 2, lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE]);
    }
    search_lease(lcache, 1, lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE], offset, offset+nbytes);
    if (lcache->buffor_search_lease[0] < 0) {
        // need to kickout some lease
        // fprintf(stderr, "[WARN]: Lease need to kickout at QDFS user.\n");
        kickout_leases(center_mcn, 1, lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE]);
    }
    if (lcache->buffor_search_lease[0] != 0) {
        // fprintf(stderr, "DEBUG_INFO: Read lease insert at query #%d.\n", query->op_ordr);
        if (respo == NULL) {
            int qlength = generate_query_pread(resc, center_mcn, query, fd, (uint64_t)offset, (uint64_t)nbytes);
            respo = issue_query(resc, center_mcn, qlength);
            if (!respo) {
                fprintf(stderr, "[ERR]: Read query failed at #%d.\n", query->op_ordr);
                exit(1);
                return -1;
            }
            if (*(uint64_t*)(respo->respo_cont + sizeof(uint64_t)*LENRPC_RESLIST) == 0xffffffffffff){
                (lcache->lease_groups_write + respo->posib_iid)->lease_cnt = 0;
                (lcache->lease_groups_write + respo->posib_iid)->address_occupied = 0;
                (lcache->lease_groups_read + respo->posib_iid)->lease_cnt = 0;
                (lcache->lease_groups_read + respo->posib_iid)->address_occupied = 0;
                lcache->buffor_search_lease[1] = 0;
            }
        }
        struct one_lease* new_lease = (struct one_lease*)malloc(sizeof(struct one_lease));
        new_lease->start_off = offset;
        new_lease->end_off = offset + nbytes;
        new_lease->addr_list = respo->respo_cont;
        add_one_lease(lcache, 0, respo->posib_iid, new_lease);
    }
    else{
        respo = (struct rpc_respo*)malloc(sizeof(struct rpc_respo));
        respo->op_name = 1;
        respo->op_ordr = GLOB_OPCNT;
        respo->posib_iid = lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE];
        respo->servr_mcnid = center_mcn;
        struct lease_entry_periid* tar_lease = lcache->lease_groups_read + respo->posib_iid;
        int lease_id = lcache->buffor_search_lease[1];
        respo->respo_cont = tar_lease->lease_list[lease_id].addr_list;
    }

    // may query blocked lease before io
    query->op_name = 6;
    int qlength = generate_query_blocked(resc, center_mcn, query);
    issue_query_blocked_1(resc, center_mcn, qlength);

    struct ibv_mr* app_mr = NULL;

    // before execute real io job, check the current newest flag of aiagent
    if (*(aiagent->newest_flag) > aiagent->version_flag){
        // fprintf(stderr, "DEBUG_INFO: an aiagent reload triggered.\n");
        aiagent->version_flag = *(aiagent->newest_flag);
        reload_tflite_model(aiagent->tflite_model, aiagent->version_flag);
    }
    execute_clientRW(resc, conx_info_local, respo, lcache, glob_thpool, tgt_buf);

    // finish blocked lease after io
    respo = issue_query_blocked_2(resc, center_mcn);

    free(query);

    // follow the recent respo
    free(respo->respo_cont);
    free(respo);

    return nbytes;
}


ssize_t qdfs_client_pwrite(int fd, void* tgt_buf, size_t nbytes, off_t offset){
    struct rpc_query* query = (struct rpc_query*)malloc(sizeof(struct rpc_query));
    struct rpc_respo* respo = NULL;
    GLOB_OPCNT += 1;
    query->op_ordr = GLOB_OPCNT;
    struct ibv_mr* app_mr = NULL;

    // // memory region register moved to per-io
    // if (Aggr_Flag){
    //     app_mr = ibv_reg_mr(resc->pd, tgt_buf, nbytes, GEN_MR_FLAG);
    //     app_lkey = app_mr->lkey;
    // }

    int center_mcn = Center_McnId;

    // write
    query->op_name = 2;

    // Need to add meta-info cache, and query the cache here before asking pwrite-lock
    if (lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE] == 0) {
        int qlength = generate_query_pwrite(resc, center_mcn, query, fd, (uint64_t)offset, (uint64_t)nbytes);
        respo = issue_query(resc, center_mcn, qlength);
        if (!respo) {
            fprintf(stderr, "[ERR]: Write query failed at #%d.\n", query->op_ordr);
            return -1;
        }
        lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE] = respo->posib_iid;
        if (*(uint64_t*)(respo->respo_cont + sizeof(uint64_t)*LENRPC_RESLIST) == 0xffffffffffff){
            (lcache->lease_groups_write + respo->posib_iid)->lease_cnt = 0;
            (lcache->lease_groups_write + respo->posib_iid)->address_occupied = 0;
            (lcache->lease_groups_read + respo->posib_iid)->lease_cnt = 0;
            (lcache->lease_groups_read + respo->posib_iid)->address_occupied = 0;
        }
    }

    search_lease(lcache, 3, lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE], offset, offset+nbytes);
    if (lcache->buffor_search_lease[0] < 0) {
        // need to kickout some lease
        // fprintf(stderr, "[WARN]: Lease need to kickout at QDFS user.\n");
        kickout_leases(center_mcn, 1, lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE]);
    }
    search_lease(lcache, 4, lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE], offset, offset+nbytes);
    if (lcache->buffor_search_lease[0] < 0) {
        // need to kickout some lease
        // fprintf(stderr, "[WARN]: Lease need to kickout at QDFS user.\n");
        kickout_leases(center_mcn, 2, lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE]);
    }

    if (lcache->buffor_search_lease[0] != 0) {
        // fprintf(stderr, "DEBUG_INFO: Write lease insert at query #%d.\n", query->op_ordr);
        if (respo == NULL) {
            int qlength = generate_query_pwrite(resc, center_mcn, query, fd, (uint64_t)offset, (uint64_t)nbytes);
            respo = issue_query(resc, center_mcn, qlength);
            if (!respo) {
                fprintf(stderr, "[ERR]: Write query failed at #%d.\n", query->op_ordr);
                return -1;
            }
            if (*(uint64_t*)(respo->respo_cont + sizeof(uint64_t)*LENRPC_RESLIST) == 0xffffffffffff){
                (lcache->lease_groups_write + respo->posib_iid)->lease_cnt = 0;
                (lcache->lease_groups_write + respo->posib_iid)->address_occupied = 0;
                (lcache->lease_groups_read + respo->posib_iid)->lease_cnt = 0;
                (lcache->lease_groups_read + respo->posib_iid)->address_occupied = 0;
                lcache->buffor_search_lease[1] = 0;
            }
        }
        struct one_lease* new_lease = (struct one_lease*)malloc(sizeof(struct one_lease));
        new_lease->start_off = offset;
        new_lease->end_off = offset + nbytes;
        new_lease->addr_list = respo->respo_cont;
        add_one_lease(lcache, 1, respo->posib_iid, new_lease);
    }
    else{
        respo = (struct rpc_respo*)malloc(sizeof(struct rpc_respo));
        respo->op_name = 2;
        respo->op_ordr = GLOB_OPCNT;
        respo->posib_iid = lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE];
        respo->servr_mcnid = center_mcn;
        struct lease_entry_periid* tar_lease = lcache->lease_groups_write + respo->posib_iid;
        int lease_id = lcache->buffor_search_lease[1];
        respo->respo_cont = tar_lease->lease_list[lease_id].addr_list;
    }

    // may query blocked lease before io
    query->op_name = 6;
    int qlength = generate_query_blocked(resc, center_mcn, query);
    issue_query_blocked_1(resc, center_mcn, qlength);

    execute_clientRW(resc, conx_info_local, respo, lcache, glob_thpool, tgt_buf);

    // finish blocked lease after io
    respo = issue_query_blocked_2(resc, center_mcn);

    free(query);
    free(respo->respo_cont);
    free(respo);
    
    return nbytes;
}


int qdfs_client_creat(char* filename, mode_t mode){
    // First get handles (if not in space yet)
    getall_conflux_handles();

    struct rpc_query* query = (struct rpc_query*)malloc(sizeof(struct rpc_query));
    struct rpc_respo* respo;
    GLOB_OPCNT += 1;
    query->op_ordr = GLOB_OPCNT;
    query->op_name = 3;
    int center_mcn = Center_McnId;
    int qlength = generate_query_open(resc, center_mcn, query, filename, 1);
    respo = issue_query(resc, center_mcn, qlength);
    int fd = *((int*)(respo->respo_cont));
    // printf("RUN_INFO: Get %s's fd: %d\n", filename, fd);

    fortest_ofile += 1;
    
    free(query);
    free(respo->respo_cont);
    free(respo);
    return fd;
}


int qdfs_client_mkdir(char* dirname, mode_t mode){
    // First get handles (if not in space yet)
    getall_conflux_handles();

    struct rpc_query* query = (struct rpc_query*)malloc(sizeof(struct rpc_query));
    struct rpc_respo* respo;
    GLOB_OPCNT += 1;
    query->op_ordr = GLOB_OPCNT;
    query->op_name = 101;
    int center_mcn = Center_McnId;
    int qlength = generate_query_mkdir(resc, center_mcn, query, dirname);
    respo = issue_query(resc, center_mcn, qlength);
    int mkdirres = *((int*)(respo->respo_cont));

    free(query);

    free(respo->respo_cont);
    free(respo);
    return mkdirres;
}


int qdfs_client_fsync(int fd){
    int mcnid = Center_McnId;
    int inodeid = lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE];
    if (inodeid == 0){
        // No write has been executed before
        return 0;
    }
    leasewrite_backall(mcnid, inodeid);
    return 0;
}


uint64_t qdfs_file_getlen(int fd){
    // query the fstat_len
    struct rpc_query* query = (struct rpc_query*)malloc(sizeof(struct rpc_query));
    struct rpc_respo* respo;
    GLOB_OPCNT += 1;
    query->op_ordr = GLOB_OPCNT;
    query->op_name = 102;
    int center_mcn = Center_McnId;
    if (file_pointer[fd-COPY_QXDFS_FDBASE] >= 0) return fstat_size[fd-COPY_QXDFS_FDBASE];
    int qlength = generate_query_getlen(resc, center_mcn, query, fd);
    respo = issue_query(resc, center_mcn, qlength);
    uint64_t getlenres = *((uint64_t*)(respo->respo_cont));

    free(query);

    free(respo->respo_cont);
    free(respo);
    return getlenres;
}


int qdfs_file_setlen(int fd, int64_t length){
    // change the fstat_len
    struct rpc_query* query = (struct rpc_query*)malloc(sizeof(struct rpc_query));
    struct rpc_respo* respo;
    GLOB_OPCNT += 1;
    query->op_ordr = GLOB_OPCNT;
    query->op_name = 103;
    int center_mcn = Center_McnId;
    int qlength = generate_query_setlen(resc, center_mcn, query, fd, length);
    respo = issue_query(resc, center_mcn, qlength);
    int setlenres = *((int*)(respo->respo_cont));

    free(query);
    free(respo->respo_cont);
    free(respo);
    return setlenres;
}


int Atemp_main(int argc, char* argv[]){

    if (!config_byfile){
        LPCACHE_POOLNAME = argv[optind++];
        fprintf(stdout, "RUN INFO: the lcache pool is at %s\n", LPCACHE_POOLNAME);
        MLX5_DEV_NAME = argv[optind++];
        fprintf(stdout, "RUN INFO: the rdma dev is %s\n", MLX5_DEV_NAME);
        char *mcnID_str = argv[optind++];
        fortest_mcnID = query_user_mcninfo(mcnID_str);
        if (fortest_mcnID < 0) exit(1);
    }

    int pn = 0;
    for (pn = 0; pn < PROC_NUM; pn++){
        int pid = fork();
        if (pid > 0) continue;
        
        int i;
        struct openfile_ret* rwfdret = qdfs_client_open("/MyTestDir/rwfile.txt", 0);
        int fd = rwfdret->ord_fd;

        char* app_buff = (char*)malloc(n_iosize);
        memset(app_buff, (int)'q', n_iosize); 
        uint64_t TestTime_nanoStep = (uint64_t)(1000000000) * TestTime_Sec / 4;
        int testres = 0;

        for (i = 0; i >= 0; ++i){
            // testres = qdfs_client_pread(fd, app_buff, n_iosize, PROC_SEG*pn + (i*n_iosize)%PROC_SEG);
            testres = qdfs_client_pwrite(fd, app_buff, n_iosize, PROC_SEG*pn + (i*n_iosize)%PROC_SEG);
            aiagent->stats[MAXCL_THNUM + aiagent->pn] += 1;

            uint64_t usetime_nano = stop_caltime_nano(aiagent->nano_timer);
            if (usetime_nano >= (uint64_t)(1000000000) * TestTime_Sec) break;
            // if (usetime_nano >= TestTime_nanoStep) {
            //     IoRatio_ToRemote -= 0.12;
            //     TestTime_nanoStep += ((uint64_t)(1000000000) * TestTime_Sec / 4);
            // }
            if (testres < 0) break;
        }
        fprintf(stderr, "RUN_INFO: Proc %d iodone-num is %d.\n", aiagent->pn, aiagent->stats[MAXCL_THNUM + aiagent->pn]);

        int closed = qdfs_client_close(fd);

        fprintf(stderr, "RUN_INFO: all queries finished.\n");
        
        exit(0);
    }
    for (pn = 0; pn < PROC_NUM; pn++) wait(NULL);

    // To calculate throughput
    int pcstat_shmfd = shm_open("QZLSHM_PCSTAT", O_RDWR, 0666);
    int* stats = (int*)mmap(NULL, sizeof(int) * MAXCL_THNUM * 6, PROT_WRITE, MAP_SHARED, pcstat_shmfd, 0);
    int allio_done = 0;
    for (pn = 0; pn < MAXCL_THNUM; pn++) allio_done += stats[MAXCL_THNUM + pn];
    fprintf(stderr, "RUN_INFO: iodone is %d, BW in total is %f MB/s.\n", allio_done, 
        allio_done*((float)n_iosize / (1<<20)) / TestTime_Sec);
    munmap(stats, sizeof(int) * MAXCL_THNUM * 6);
    shm_unlink("QZLSHM_PCSTAT");

    return 0;
}


 
#endif
