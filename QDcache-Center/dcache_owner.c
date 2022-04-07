#ifndef __DCACHE_OWNER
#define __DCACHE_OWNER


#include <stdint.h>
#include "dcache_rdmaconx.c"
#include "thpool.h"
#include "qdfs_centers.c"


static int User_Cur_ProcNum = 0;
const int LENRPC_RESLIST = 122;

void* query_buf_POOL;
void* name_BUF_POOL;

struct rpc_query{
    int op_ordr;
    int op_name;
    char* file_name;
    uint64_t offset;
    uint64_t length;
};


struct rpc_respo{
    int op_ordr;
    int op_name;
    uint64_t* segm_list;
};



struct argFor_response_user_query{
    struct ibdev_resc* resc;
    int procid;
};

void* response_user_query(void* targ){
    // fprintf(stdout, "RUN_INFO: get query from %d\n", mcnid);
    int *ret = (int*)malloc(sizeof(int));
    struct argFor_response_user_query* arg = (struct argFor_response_user_query*)targ;
    struct ibdev_resc* resc = arg->resc;
    int procid = arg->procid;
    char * respo = resc->rpc_buf[procid];

    struct caltime_nano* User_PreTimer = init_caltime_nano();
    start_caltime_nano(User_PreTimer);
    
    do{
        // check this proc's sync halt signal
        if (resc->procState[procid] == 'E') break;

        // waiting for recv to comein
        uint64_t user_pretime = stop_caltime_nano(User_PreTimer);
        int user_pass_pre = 0;
        if (user_pretime > (uint64_t)1000000000 * MANUAL_TIME) user_pass_pre = 1;
        int curqpn = ibpoll_completion(resc->procState, resc->cq_recv[procid], CQE_IF_TIMEOUT, 0, user_pass_pre);
        if (curqpn < 0) {
            fprintf(stderr, "[ERR]: wrong cq element found.\n");
            *ret = 1;
            return (void*)ret;
        }
        if (curqpn == 0) {
            *ret = 0;
            return (void*)ret;
        }

        char* query_buf = (char*)(query_buf_POOL + RPC_MR_SIZE * procid);

        if (curqpn == resc->qp_list[procid]->qp_num) {
            memcpy(query_buf, (resc->rpc_buf[procid])+RPC_MR_SIZE, RPC_MR_SIZE);
            post_receive(resc, -1, procid, RPC_MR_SIZE);
        }
        else if (ALLOW_DM_RPC && curqpn == resc->qpdm_list[procid]->qp_num) {
            fprintf(stderr, "opname ::1 %d\n", procid);
            ibv_memcpy_from_dm(query_buf, resc->dm_buf, PRC_DM_SIZE*2*procid+PRC_DM_SIZE, PRC_DM_SIZE);
            post_receive(resc, -1, procid, DM_FLAG_LENT + PRC_DM_SIZE);
        }
        else {
            fprintf(stderr, "[ERR]: mismatch cq element found.\n");
            *ret = 1;
            return (void*)ret;
        }

        // process query_buf and calculate queried info
        int op_ordr = *((int*)(query_buf+0));
        int op_name = *((int*)(query_buf+4));

        int response_len = 0;

        if (op_name == 1){
            // fprintf(stderr, "*** Client read at proc %d ***\n", procid);
            // this is read query
            *((int*)(respo+0)) = op_ordr;
            *((int*)(respo+4)) = op_name;
            int fd = *((int*)(query_buf+8));
            uint64_t offset = *((uint64_t*)(query_buf+16));
            uint64_t length = *((uint64_t*)(query_buf+24));
            uint64_t* segm_list = getaddrlist_fromqfs(fd, offset, length, procid);
            if (segm_list != NULL){
                int lockretv = userquery_lockop(procid, fd, -1, offset, length, 1, 1);
                if (lockretv >= 0){
                    *((int*)(respo+8)) = lockretv;
                    memcpy(respo + 16, segm_list, sizeof(uint64_t)*LENRPC_RESLIST);
                }
                else {
                    *((int*)(respo+8)) = lockretv;
                    userquery_inflightlock(procid, respo + 16);
                }
                if (segm_list[LENRPC_RESLIST] == 0xffffffffffff)
                    *((int*)(respo+12)) = -1;
                else 
                    *((int*)(respo+12)) = 0;
            }
            // fprintf(stderr, "*** Client read finished at proc %d ***\n", procid);
            response_len = RPC_MR_SIZE;
        }
        else if (op_name == 2){
            // fprintf(stderr, "*** Client write at proc %d ***\n", procid);
            // this is write query
            *((int*)(respo+0)) = op_ordr;
            *((int*)(respo+4)) = op_name;
            int fd = *((int*)(query_buf+8));
            uint64_t offset = *((uint64_t*)(query_buf+16));
            uint64_t length = *((uint64_t*)(query_buf+24));
            uint64_t* segm_list = getaddrlist_fromqfs(fd, offset, length, procid);
            if (segm_list != NULL){
                int lockretv = userquery_lockop(procid, fd, -1, offset, length, 2, 1);
                if (lockretv >= 0){
                    *((int*)(respo+8)) = lockretv;
                    memcpy(respo + 16, segm_list, sizeof(uint64_t)*LENRPC_RESLIST);
                }
                else {
                    *((int*)(respo+8)) = lockretv;
                    userquery_inflightlock(procid, respo + 16);
                }
                if (segm_list[LENRPC_RESLIST] == 0xffffffffffff)
                    *((int*)(respo+12)) = -1;
                else 
                    *((int*)(respo+12)) = 0;
            }
            // fprintf(stderr, "*** Client write finished at proc %d, fd %d ***\n", procid, fd);
            response_len = RPC_MR_SIZE;
        }
        else if (op_name == 3){
            // fprintf(stderr, "*** Client open at proc %d ***\n", procid);
            // this is open query
            *((int*)(respo+0)) = op_ordr;
            *((int*)(respo+4)) = op_name;
            int fname_len = *((int*)(query_buf+8));
            int o_flag = *((int*)(query_buf+12));
            char *fname = (char*)(name_BUF_POOL + RPC_MR_SIZE * procid);
            memcpy(fname, query_buf+16, fname_len);
            fname[fname_len] = '\0';
            struct openfile_ret* ret = qxdfs_open_file(fname, o_flag, procid);
            *((int*)(respo+8)) = ret->ord_fd;
            *((int*)(respo+12)) = ret->file_mode;
            *((uint64_t*)(respo+16)) = ret->stat_size;

            // fprintf(stderr, "*** Client open finished at proc %d, name: %s ***\n", procid, fname);
            response_len = 32;
        }
        else if (op_name == 4){
            // fprintf(stderr, "*** Client close at proc %d ***\n", procid);
            // this is close query
            *((int*)(respo+0)) = op_ordr;
            *((int*)(respo+4)) = op_name;
            int fd = *((int*)(query_buf+8));
            int call_ret = qxdfs_close_file(fd, procid);
            *((int*)(respo+8)) = call_ret;

            // fprintf(stderr, "*** Client close finished at proc %d ***\n", procid);
            response_len = 16;
        }
        else if (op_name == 5){
            // this is unlock query
            *((int*)(respo+0)) = op_ordr;
            *((int*)(respo+4)) = op_name;
            char* locklistbuf = query_buf+8;
            int fd_iid = *((int*)locklistbuf);
            while (fd_iid >= 0){
                uint8_t rw_flag = *((uint8_t*)(locklistbuf+4));
                uint64_t offset = *((uint64_t*)(locklistbuf+8));
                uint64_t length = *((uint64_t*)(locklistbuf+16));
                if (fd_iid >= QXDFS_FDBASE) {
                    // fprintf(stderr, "RUN__LOG: unlocking file#%d, offset %llu, length %llu.\n", fd_iid, offset, length);
                    userquery_lockop(procid, fd_iid, -1, offset, length, rw_flag, 2);
                }
                else {
                    // fprintf(stderr, "RUN__LOG: unlocking file#%d, offset %llu, length %llu.\n", fd_iid, offset, length);
                    userquery_lockop(procid, -1, fd_iid, offset, length, rw_flag, 2);
                }
                locklistbuf = locklistbuf + 24;
                fd_iid = *((int*)locklistbuf);
            }
            *((int*)(respo+8)) = 0;
            response_len = 16;
        }
        else if (op_name == 6){
            // this is other-proc-locking query
            *((int*)(respo+0)) = op_ordr;
            *((int*)(respo+4)) = op_name;
            userquery_inflightlock(procid, respo + 8);
            response_len = RPC_MR_SIZE;
        }
        else if (op_name == 101){
            // this is mkdir query
            *((int*)(respo+0)) = op_ordr;
            *((int*)(respo+4)) = op_name;
            
            int dname_len = *((int*)(query_buf+8));
            char *dname = (char*)(name_BUF_POOL + RPC_MR_SIZE * procid);
            memcpy(dname, query_buf+12, dname_len);
            dname[dname_len] = '\0';
            dname[dname_len+1] = '\0';
            
            xxx_create_dir(dname, procid);
            *((int*)(respo+8)) = 0;
            response_len = 16;
        }
        else if (op_name == 102){
            // this is get fstat_len query
            *((int*)(respo+0)) = op_ordr;
            *((int*)(respo+4)) = op_name;

            int fd = *((int*)(query_buf+8));
            int realfd = fd - QXDFS_FDBASE;
            int iid = self_fdarr[realfd];
            struct inode_ele* ind = load_inode_struct(self_meta, iid);

            *((uint64_t*)(respo+8)) = ind->fstat_len;
            response_len = 32;
        }
        else if (op_name == 103){
            // this is set fstat_len request
            *((int*)(respo+0)) = op_ordr;
            *((int*)(respo+4)) = op_name;

            int fd = *((int*)(query_buf+8));
            int64_t length = *((int64_t*)(query_buf+16));
            int realfd = fd - QXDFS_FDBASE;
            int iid = self_fdarr[realfd];
            struct inode_ele* ind = load_inode_struct(self_meta, iid);

            if (ind->fstat_len >= length){
                ind->fstat_len = length;
                stor_inode_struct(self_meta, ind->inode_id);
                *((int*)(respo+8)) = 0;
            }
            else{
                xxx_append_file(NULL, length - ind->fstat_len, iid, MAXCL_THNUM);
                *((int*)(respo+8)) = 0;
            }
            response_len = 16;
        }
        else if (op_name == 104){
            // this is stat (originally in open) request
            *((int*)(respo+0)) = op_ordr;
            *((int*)(respo+4)) = op_name;
            int fname_len = *((int*)(query_buf+8));
            int o_flag = *((int*)(query_buf+12));
            char *fname = (char*)(name_BUF_POOL + RPC_MR_SIZE * procid);
            memcpy(fname, query_buf+16, fname_len);
            fname[fname_len] = '\0';
            struct openfile_ret* ret = qxdfs_stat_file(fname, o_flag, procid);
            *((int*)(respo+8)) = ret->ord_fd;
            *((int*)(respo+12)) = ret->file_mode;
            *((uint64_t*)(respo+16)) = ret->stat_size;
            free(ret);
            response_len = 32;
        }
        else{
            fprintf(stderr, "[ERR]: Invalid op_name from QDFS center.\n");
            *ret = 1;
            return (void*)ret;
        }

        if (ALLOW_DM_RPC && response_len <= PRC_DM_SIZE){
            fprintf(stderr, "opname ::2 %d\n", procid);
            ibv_memcpy_to_dm(resc->dm_buf, PRC_DM_SIZE*2*procid, respo, PRC_DM_SIZE);
            response_len += DM_FLAG_LENT;
        }

        post_send(resc, -1, procid, response_len);
        // fprintf(stderr, "opname ::3 %d\n", op_name);
        curqpn = ibpoll_completion(resc->procState, resc->cq_send[procid], CQE_RT_TIMEOUT, 0, user_pass_pre);
        if (curqpn < 0) {
            fprintf(stderr, "[ERR]: wrong cq element found at send #%d.\n", op_ordr);
            *ret = 1;
            return (void*)ret;
        }
        if (curqpn == 0) {
            *ret = 0;
            return (void*)ret;
        }
        // fprintf(stderr, "opname ::4 %d\n", procid);

    }while(1);

    *ret = 0;
    return (void*)ret;
}


struct argFor_serve_Allrecv{
    struct ibdev_resc* resc;
    struct ib_conx_info** conx_info_local;
};

void* serve_Allrecv(void* arg){
    int *ret = (int*)malloc(sizeof(int));
    *ret = 0;
    struct ibdev_resc* resc = ((struct argFor_serve_Allrecv*)arg)->resc;
    struct ib_conx_info** conx_info_local = ((struct argFor_serve_Allrecv*)arg)->conx_info_local;

    threadpool tp = thpool_init(MAXCL_THNUM);
    int* userpstatbits = (int*)malloc(sizeof(int)*MAXCL_THNUM);
    int i;
    for (i = 0; i < MAXCL_THNUM; ++i) userpstatbits[i] = 0;

    struct caltime_nano* User_PreTimer = init_caltime_nano();
    start_caltime_nano(User_PreTimer);

    // Poll for all user's proc incoming
    int in_proc_cnt = -1;
    do{
        if (in_proc_cnt >= 0) in_proc_cnt = 0;
        for (i = 0; i < MAXCL_THNUM; ++i){
            if (userpstatbits[i] == 0 && resc->procState[i] == 'S'){
                if (in_proc_cnt < 0) in_proc_cnt = 0;
                userpstatbits[i] = 1;
                while (conx_info_local[i]->mcnID == 0){
                    int emp = (resc->procState[i] == 'S'); 
                }
                post_receive(resc, -1, i, RPC_MR_SIZE);
                if (ALLOW_DM_RPC) post_receive(resc, -1, i, DM_FLAG_LENT + PRC_DM_SIZE);
                struct argFor_response_user_query *targ;
                targ = (struct argFor_response_user_query *)malloc(sizeof(struct argFor_response_user_query));
                targ->resc = resc;
                targ->procid = i;
                thpool_add_work(tp, (void*)&response_user_query, (void*)targ);
            }
            if (userpstatbits[i] == 1 && resc->procState[i] == 'H')
                userpstatbits[i] = 2;
            if (userpstatbits[i] == 1) in_proc_cnt += 1;
        }
        uint64_t user_pretime = stop_caltime_nano(User_PreTimer);
        int user_pass_pre = 0;
        if (user_pretime > (uint64_t)1000000000 * MANUAL_TIME) user_pass_pre = 1;
        if (user_pass_pre == 1 && in_proc_cnt == 0) break;
    }while(1);

    fprintf(stderr, "DEBUG INFO: ...... Poll for all user's proc incoming END ......\n");

    // thpool_wait(tp);
    // thpool_pause(tp);
    // thpool_destroy(tp);

    return (void*)ret;
}


static int mcnID_Proc;


int main(int argc, char* argv[]){
    if (0) {
        // config by parameter
        PMEM_POOL_NAME = argv[optind++];
        fprintf(stdout, "RUN INFO: the pmem pool is at %s\n", PMEM_POOL_NAME);
        MLX5_DEV_NAME = argv[optind++];
        fprintf(stdout, "RUN INFO: the rdma dev is %s\n", MLX5_DEV_NAME);
        char *mcnID_str = argv[optind++];
        fprintf(stdout, "RUN INFO: the macIDstr is %s\n", mcnID_str);
        mcnID_Proc = query_user_mcninfo(mcnID_str);
        if (mcnID_Proc < 0) exit(1);
    }
    else {
        FILE *config_file = fopen(conflux_config_file, "r");
        PMEM_POOL_NAME = malloc(80);
        memset(PMEM_POOL_NAME, 0, 80);
        fscanf(config_file, "%[^\n] ", PMEM_POOL_NAME);
        fprintf(stdout, "RUN INFO: the pmem pool is at %s\n", PMEM_POOL_NAME);
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

        mcnID_Proc = query_user_mcninfo(mcnID_str);
        if (mcnID_Proc < 0) exit(1);
    }

    filenamehash = (struct hashmap_s*)malloc(sizeof(struct hashmap_s)*(MAXCL_THNUM+1));
    int hashiter;
    for (hashiter = 0; hashiter <= MAXCL_THNUM; ++hashiter){
        memset(&(filenamehash[hashiter]), 0, sizeof(struct hashmap_s));
        int filehashcons = hashmap_create((1<<16), &(filenamehash[hashiter]));
        if (filehashcons != 0) {fprintf(stderr, "[ERR]: filenamehash establish FAILED.\n");}
    }
    hashvalues = (int*)malloc(sizeof(int) * MAX_INODE);
    for (hashiter = 0; hashiter < MAX_INODE; ++hashiter){
        hashvalues[hashiter] = hashiter;
    }
    for (hashiter = 0; hashiter <= MAXCL_THNUM; ++hashiter)
    hashmap_put(&(filenamehash[hashiter]), "/", 1, &(hashvalues[0]));

    inproc_init_fsmeta();

    /* Naive init for an FIO test */
    xxx_create_dir("/MyTestDir", MAXCL_THNUM);
    xxx_create_file("/MyTestDir/rwfile.txt", MAXCL_THNUM);
    xxx_append_file("/MyTestDir/rwfile.txt", (uint64_t)(1<<20) * 2400, -1, MAXCL_THNUM);

    /* Naive init for an FileBench test */
    xxx_create_dir("/workdir", MAXCL_THNUM);
    xxx_create_dir("/workdir/StartFile", MAXCL_THNUM);
    xxx_create_dir("/workdir/testDIR", MAXCL_THNUM);
    xxx_create_dir("/workdir/bigfileset", MAXCL_THNUM);
    xxx_create_dir("/workdir/logfiles", MAXCL_THNUM);

    inproc_free_fsmeta();

    int pn = 0;
    for (pn = 0; pn < USER_NUM; pn++){
    // for (pn = 0; pn < 10; pn++){
        int pid = fork();
        if (pid > 0) continue;

        filenamehash = (struct hashmap_s*)malloc(sizeof(struct hashmap_s)*(MAXCL_THNUM+1));
        int hashiter;
        for (hashiter = 0; hashiter <= MAXCL_THNUM; ++hashiter){
            memset(&(filenamehash[hashiter]), 0, sizeof(struct hashmap_s));
            int filehashcons = hashmap_create((1<<16), &(filenamehash[hashiter]));
            if (filehashcons != 0) {fprintf(stderr, "[ERR]: filenamehash establish FAILED.\n");}
        }
        hashvalues = (int*)malloc(sizeof(int) * MAX_INODE);
        for (hashiter = 0; hashiter < MAX_INODE; ++hashiter){
            hashvalues[hashiter] = hashiter;
        }
        for (hashiter = 0; hashiter <= MAXCL_THNUM; ++hashiter)
        hashmap_put(&(filenamehash[hashiter]), "/", 1, &(hashvalues[0]));

        inproc_init_fsmeta();

        query_buf_POOL = malloc(RPC_MR_SIZE * MAXCL_THNUM);
        name_BUF_POOL = malloc(RPC_MR_SIZE * MAXCL_THNUM);

        char *raw_pool = (char*)(self_meta->pmpool);
        struct ibdev_resc* resc = create_ibdev_resc(MLX5_DEV_NAME, raw_pool, 0, pn);
        struct ib_conx_info** conx_info_local = ibconx_info_init(resc, 0);
        center_tcp_listen(mcnID_Proc, resc, conx_info_local, user_mcnid[pn]);

        struct argFor_serve_Allrecv ser_arg; 
        ser_arg.resc = resc;
        ser_arg.conx_info_local = conx_info_local;

        pthread_t servept;
        pthread_create(&servept, NULL, (void*)(&serve_Allrecv), (void*)(&ser_arg));

        waitMiliSec(1000*2);
        
        center_issue_halt(resc, conx_info_local);
        
        pthread_join(servept, NULL);
        destroy_ibdev_resc(resc, 0);

        munmap(raw_pool, PMEM_POOL_LENT);

        // general exit code
        inproc_free_fsmeta();
        exit(0);
    }

    fprintf(stderr, "RUN INFO: About to wait %d working proc.\n", USER_NUM);
    for (pn = 0; pn < USER_NUM; pn++) wait(NULL);
    // for (pn = 0; pn < 10; pn++) wait(NULL);

    return 0;
}


 
#endif
