#ifndef __DCACHE_RDMACONX
#define __DCACHE_RDMACONX


#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <netdb.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <getopt.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <memory.h>
#include <stdint.h>
#include <syscall.h>
#include "dcache_timer.c"
#include "dcache_tcpfunc.c"
#include "thpool.h"


const char* conflux_config_file = "../Conflux-Scripts/.conflux_config";

const char* mcnip_prefix = "IP-Prefix24";
const int TCP_PORT_BASE = 20009;
const int MAXCL_THNUM = 50;
const int MAXUSER_NUM = 12;

int MCN_NUM;
int *mcnip_suffix;
int CENTER_NUM;
uint16_t *center_mcnid;
uint16_t *center_order;
int USER_NUM;
uint16_t *user_mcnid;
uint16_t *user_order;

// const int MCN_NUM = 8;
// int mcnip_suffix[8] = {40,41,42,43,71,72,73,74};
// const int CENTER_NUM = 1;
// uint16_t center_mcnid[1] = {7};
// uint16_t center_order[8] = {0,0,0,0,0,0,1,0};
// const int USER_NUM = 1;
// uint16_t user_mcnid[1] = {8};
// uint16_t user_order[8] = {0,0,0,0,0,0,0,1};


char* MLX5_DEV_NAME;
const int IB_PORT_ID = 1;
const int IB_CQ_SIZE = 8;
const uint64_t CENTER_MR_SIZE = (uint64_t)(1 << 30) * 64;
const uint64_t USER_MR_SIZE = (uint64_t)(1 << 30) * 17;
const int RPC_MR_NUM = 8;
const int RPC_MR_SIZE = 1024;
const int DM_FLAG_LENT = (INT32_MAX >> 1);
const int PRC_DM_SIZE = 64;
const int RPC_RECV_PRE = 4;
const int CQE_RT_TIMEOUT = 1500;
const int CQE_IF_TIMEOUT = INT32_MAX;
const int ALLOW_DM_RPC = 0;


struct ib_conx_info{
    int syncfd;
    uint16_t mcnID;
    uint16_t lid;
    uint32_t qpn_rpc;
    uint32_t qpn_dmrpc;
    uint32_t qpn_rw;
    uint32_t rkey_rw;
    uint64_t addr_rw;
};


struct ibdev_resc {
    int resc_id;
    struct ibv_context* dev_ctx;
    struct ibv_port_attr port_attr;
    struct ibv_device_attr dev_attr;
    struct ibv_pd * pd;
    struct ibv_cq ** cq_send;
    struct ibv_cq ** cq_recv;
    struct ibv_cq ** cq_rw;
    struct ibv_qp ** qp_list;
    struct ibv_qp ** qpdm_list;
    struct ibv_qp ** rw_qplist;
    uint64_t rpcmr_cnt;
    struct ibv_mr ** rpcmr_list;
    struct ibv_mr ** dm_mr_list;
    struct ibv_mr * datamr;
    char ** rpc_buf;
    struct ibv_dm * dm_buf;
    char * data_buf;
    struct ibv_mr * chunkmr;
    char * chunk_cache;
    char * procState; // 0 -> S -> H -> E
};


struct tcp_exchange_arg{
    uint16_t loacl_mcnID;
    uint16_t remot_mcnID;
    int portid;
    struct ibdev_resc* resc;
    struct ib_conx_info** conx_info_local;
    int conx_cnt;
    int role; // 0 as center, 1 as user
};


int cluster_info_parser(){
    int i,j;
    FILE *config_file = fopen(conflux_config_file, "r");
    char* PMEM_POOL_NAME = malloc(64);
    fscanf(config_file, "%[^\n] ", PMEM_POOL_NAME);
    MLX5_DEV_NAME = malloc(64);
    fscanf(config_file, "%[^\n] ", MLX5_DEV_NAME);
    char *mcnID_str = malloc(64);
    fscanf(config_file, "%[^\n] ", mcnID_str);
    
    char* MCN_NUM_str = malloc(32);
    memset(MCN_NUM_str, 0, 32);
    fscanf(config_file, "%[^\n] ", MCN_NUM_str);
    MCN_NUM = 0;
    for (j = 0; j < strlen(MCN_NUM_str); ++j){
        if (MCN_NUM_str[j] >= '0' && MCN_NUM_str[j] <= '9')
        MCN_NUM = MCN_NUM * 10 + (int)(MCN_NUM_str[j] - '0');
    }
    fprintf(stdout, "the number of machine configured: %d\n", MCN_NUM);

    mcnip_suffix = (int*)malloc(sizeof(int)*MCN_NUM);
    for (j = 0; j < MCN_NUM; ++j) mcnip_suffix[j] = 0;
    char* mcnip_suffix_str = malloc(128);
    memset(mcnip_suffix_str, 0, 128);
    fscanf(config_file, "%[^\n] ", mcnip_suffix_str);
    j = 0;
    for (i = 0; i < MCN_NUM; ++i){
        int numtmp = 0;
        for (; j < 128; ++j){
            if (mcnip_suffix_str[j] >= '0' && mcnip_suffix_str[j] <= '9')
            numtmp = numtmp * 10 + (int)(mcnip_suffix_str[j] - '0');
            else break;
        }
        if (mcnip_suffix_str[j] != ',') perror("Config_File_Broken");
        j += 1;
        mcnip_suffix[i] = numtmp;
    }

    char* CENTER_NUM_str = malloc(32);
    memset(CENTER_NUM_str, 0, 32);
    fscanf(config_file, "%[^\n] ", CENTER_NUM_str);
    CENTER_NUM = 0;
    for (j = 0; j < strlen(CENTER_NUM_str); ++j){
        if (CENTER_NUM_str[j] >= '0' && CENTER_NUM_str[j] <= '9')
        CENTER_NUM = CENTER_NUM * 10 + (int)(CENTER_NUM_str[j] - '0');
    }
    fprintf(stdout, "the number of centres: %d\n", CENTER_NUM);

    center_mcnid = (uint16_t*)malloc(sizeof(int)*CENTER_NUM);
    for (j = 0; j < CENTER_NUM; ++j) center_mcnid[j] = 0;
    char* center_mcnid_str = malloc(128);
    memset(center_mcnid_str, 0, 128);
    fscanf(config_file, "%[^\n] ", center_mcnid_str);
    j = 0;
    for (i = 0; i < CENTER_NUM; ++i){
        int numtmp = 0;
        for (; j < 128; ++j){
            if (center_mcnid_str[j] >= '0' && center_mcnid_str[j] <= '9')
            numtmp = numtmp * 10 + (int)(center_mcnid_str[j] - '0');
            else break;
        }
        if (center_mcnid_str[j] != ',') perror("Config_File_Broken");
        j += 1;
        center_mcnid[i] = numtmp;
    }
    // for (j = 0; j < CENTER_NUM; ++j) fprintf(stdout, "%d,", (int)(center_mcnid[j]));
    // fprintf(stdout, "\n");

    center_order = (uint16_t*)malloc(sizeof(int)*MCN_NUM);
    for (j = 0; j < MCN_NUM; ++j) center_order[j] = 0;
    char* center_order_str = malloc(128);
    memset(center_order_str, 0, 128);
    fscanf(config_file, "%[^\n] ", center_order_str);
    j = 0;
    for (i = 0; i < MCN_NUM; ++i){
        int numtmp = 0;
        for (; j < 128; ++j){
            if (center_order_str[j] >= '0' && center_order_str[j] <= '9')
            numtmp = numtmp * 10 + (int)(center_order_str[j] - '0');
            else break;
        }
        if (center_order_str[j] != ',') perror("Config_File_Broken");
        j += 1;
        center_order[i] = numtmp;
    }
    // for (j = 0; j < MCN_NUM; ++j) fprintf(stdout, "%d,", (int)(center_order[j]));
    // fprintf(stdout, "\n");

    char* USER_NUM_str = malloc(32);
    memset(USER_NUM_str, 0, 32);
    fscanf(config_file, "%[^\n] ", USER_NUM_str);
    USER_NUM = 0;
    for (j = 0; j < strlen(USER_NUM_str); ++j){
        if (USER_NUM_str[j] >= '0' && USER_NUM_str[j] <= '9')
        USER_NUM = USER_NUM * 10 + (int)(USER_NUM_str[j] - '0');
    }
    fprintf(stdout, "the number of users: %d\n", USER_NUM);

    user_mcnid = (uint16_t*)malloc(sizeof(int)*USER_NUM);
    for (j = 0; j < USER_NUM; ++j) user_mcnid[j] = 0;
    char* user_mcnid_str = malloc(128);
    memset(user_mcnid_str, 0, 128);
    fscanf(config_file, "%[^\n] ", user_mcnid_str);
    j = 0;
    for (i = 0; i < USER_NUM; ++i){
        int numtmp = 0;
        for (; j < 128; ++j){
            if (user_mcnid_str[j] >= '0' && user_mcnid_str[j] <= '9')
            numtmp = numtmp * 10 + (int)(user_mcnid_str[j] - '0');
            else break;
        }
        if (user_mcnid_str[j] != ',') perror("Config_File_Broken");
        j += 1;
        user_mcnid[i] = numtmp;
    }
    // for (j = 0; j < USER_NUM; ++j) fprintf(stdout, "%d,", (int)(user_mcnid[j]));
    // fprintf(stdout, "\n");

    user_order = (uint16_t*)malloc(sizeof(int)*MCN_NUM);
    for (j = 0; j < MCN_NUM; ++j) user_order[j] = 0;
    char* user_order_str = malloc(128);
    memset(user_order_str, 0, 128);
    fscanf(config_file, "%[^\n] ", user_order_str);
    j = 0;
    for (i = 0; i < MCN_NUM; ++i){
        int numtmp = 0;
        for (; j < 128; ++j){
            if (user_order_str[j] >= '0' && user_order_str[j] <= '9')
            numtmp = numtmp * 10 + (int)(user_order_str[j] - '0');
            else break;
        }
        if (user_order_str[j] != ',') perror("Config_File_Broken");
        j += 1;
        user_order[i] = numtmp;
    }
    // for (j = 0; j < MCN_NUM; ++j) fprintf(stdout, "%d,", (int)(user_order[j]));
    // fprintf(stdout, "\n");

    fclose(config_file);
    return 0;
}


int query_user_mcninfo(char* mcnID_str){
    int mcnID = 0;
    int mcnID_strLen = strlen(mcnID_str);
    int j;
    for (j = 0; j < mcnID_strLen; ++j){
        if (mcnID_str[j] >= '0' && mcnID_str[j] <= '9')
        mcnID = mcnID * 10 + (int)(mcnID_str[j] - '0');
    }
    fprintf(stdout, "RUN_INFO: The server's ID in cluster: %d\n", mcnID);

    char *local_ip = (char*)malloc(20);
    memset(local_ip, 0, 20);
    sprintf(local_ip, "%s%d", mcnip_prefix, mcnip_suffix[mcnID-1]);

    return mcnID;
}


int modify_qp_to_init(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    int attr_mask;
    int a;
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = IB_PORT_ID;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    attr_mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    a = ibv_modify_qp(qp, &attr, attr_mask);
    if (a) fprintf(stderr, "[ERR]: ibv_modify_qp to INIT failed!\n");
    return 0;
}


int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid) {
    struct ibv_qp_attr attr;
    int attr_mask;
    int a;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_256;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 0xc;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = IB_PORT_ID;
    attr_mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    a = ibv_modify_qp(qp, &attr, attr_mask);
    if (a) fprintf(stderr, "[ERR]: failed to modify QP state to RTR\n");
    return a;
}


int modify_qp_to_rts(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    int attr_mask;
    int a;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 0xc;
    attr.retry_cnt = 2;
    attr.rnr_retry = 0;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    attr_mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    a = ibv_modify_qp(qp, &attr, attr_mask);
    if (a) fprintf(stderr, "[ERR]: failed to modify QP state to RTS\n");
    return a;
}


struct ib_conx_info** ibconx_info_init(struct ibdev_resc* resc, int roleBit){
    int i;

    int pNUM = 0;
    if (roleBit == 0) pNUM = MAXCL_THNUM;
    else if (roleBit == 1) pNUM = MCN_NUM;
    else {
        fprintf(stderr, "[WARN]: invalid rolebit, cannot sign conx_info size.\n");
    }

    for (i = 0; i < pNUM; ++i){
        modify_qp_to_init(resc->qp_list[i]);
        modify_qp_to_init(resc->qpdm_list[i]);
        modify_qp_to_init(resc->rw_qplist[i]);
    }
    struct ib_conx_info** conx_info_local = (struct ib_conx_info**)malloc(pNUM * sizeof(struct ib_conx_info*));
    for (i = 0; i < pNUM; ++i){
        conx_info_local[i] = (struct ib_conx_info*)malloc(sizeof(struct ib_conx_info));
        memset(conx_info_local[i], 0, sizeof(struct ib_conx_info));
    }
    return conx_info_local;
}


int update_conx_info(struct ib_conx_info* conx_info, 
                uint16_t mcnID, 
                uint16_t lid,
                uint32_t qpn_rpc,
                uint32_t qpn_dmrpc,
                uint32_t qpn_rw,
                uint32_t rkey_rw,  
                uint64_t addr_rw){
    conx_info->mcnID = mcnID;
    conx_info->lid = lid;
    conx_info->qpn_rpc = qpn_rpc;
    conx_info->qpn_dmrpc = qpn_dmrpc;
    conx_info->qpn_rw = qpn_rw;
    conx_info->rkey_rw = rkey_rw;
    conx_info->addr_rw = addr_rw;
    return 0;
}


void * tcp_exchange(void* arg){
    int *ret = (int*)malloc(sizeof(int));
    *ret = 0;
    struct tcp_exchange_arg* t_arg = (struct tcp_exchange_arg*)arg;
    uint16_t localMid = t_arg->loacl_mcnID;
    uint16_t remotMid = t_arg->remot_mcnID;
    int cur_port = t_arg->portid;
    struct ibdev_resc* resc = t_arg->resc;
    struct ib_conx_info** conx_info_local = t_arg->conx_info_local;

    char* serverip;
    // note here the role bit is somewhat different with ib qp rolebit
    if (t_arg->role == 0) 
        serverip = NULL;
    else{
        serverip = (char*)malloc(20);
        memset(serverip, 0, 20);
        sprintf(serverip, "%s%d", mcnip_prefix, mcnip_suffix[remotMid-1]);
    }

    int sockfd;

    int stoppable = 0;
    while (!stoppable) {

        sockfd = -1;
        while (sockfd < 0){
            sockfd = sock_connect(serverip, cur_port);
            cur_port += (MCN_NUM * MAXCL_THNUM);
            if (cur_port >= TCP_PORT_BASE * 2) break;
        }
        if (t_arg->role == 0) 
            fprintf(stdout, "DEBUG_INFO: Waiting on local port: %d\n", cur_port);
        else
            fprintf(stdout, "DEBUG_INFO: Linking to server: %s\n", serverip);
        // above, Both server and client will search in multi-port

        if (sockfd < 0){
            if (t_arg->role == 0) 
                fprintf(stderr, "[WARN]: TCP open {localhost:%d} not success\n", cur_port);
            else 
                fprintf(stderr, "[WARN]: TCP connection to {%s} not success\n", serverip);
            *ret = 1;
            return (void*)ret;
        }

        struct ib_conx_info conx_info_ready;
        int conx_id = t_arg->conx_cnt;
        update_conx_info(&conx_info_ready, 
                    htons(localMid), 
                    htons(resc->port_attr.lid), 
                    htonl(resc->qp_list[conx_id]->qp_num),
                    htonl(resc->qpdm_list[conx_id]->qp_num),
                    htonl(resc->rw_qplist[conx_id]->qp_num),
                    htonl(resc->datamr->rkey), 
                    htonll((uint64_t)(resc->data_buf))
        );
        struct ib_conx_info conx_info_tmp;
        int a = sock_sync_data(sockfd, sizeof(struct ib_conx_info), 
            (char*)(&conx_info_ready), (char*)(&conx_info_tmp));
        if (a < 0) {
            fprintf(stderr, "[ERR]: Sync data via TCP failed!\n");
            *ret = 1;
            return (void*)ret;
        }
        // fprintf(stdout, "DEBUG_INFO: Get conx Info from machine #%d \n", remotMid);
        struct ib_conx_info *conx_info_remot = conx_info_local[conx_id];
        update_conx_info(conx_info_remot, 
                        ntohs(conx_info_tmp.mcnID), 
                        ntohs(conx_info_tmp.lid), 
                        ntohl(conx_info_tmp.qpn_rpc),
                        ntohl(conx_info_tmp.qpn_dmrpc),
                        ntohl(conx_info_tmp.qpn_rw),
                        ntohl(conx_info_tmp.rkey_rw), 
                        ntohll(conx_info_tmp.addr_rw)
        );
        // fprintf(stdout, "DEBUG_INFO: Remote lid: %d, Remote rw_qp: %d, Rkey: %p\n", 
        //     conx_info_remot->lid, conx_info_remot->qpn_rw, conx_info_remot->rkey_rw);
        
        conx_info_remot->syncfd = sockfd;

        // connect rdma qps here
        if (t_arg->role == 0) {
            uint32_t qpn_rpc = conx_info_local[conx_id]->qpn_rpc;
            uint32_t qpn_dmrpc = conx_info_local[conx_id]->qpn_dmrpc;
            uint32_t qpn_rw = conx_info_local[conx_id]->qpn_rw;
            uint16_t rlid = conx_info_local[conx_id]->lid;

            modify_qp_to_rtr(resc->qp_list[conx_id], qpn_rpc, rlid);
            modify_qp_to_rts(resc->qp_list[conx_id]);

            modify_qp_to_rtr(resc->qpdm_list[conx_id], qpn_dmrpc, rlid);
            modify_qp_to_rts(resc->qpdm_list[conx_id]);

            modify_qp_to_rtr(resc->rw_qplist[conx_id], qpn_rw, rlid);
            modify_qp_to_rts(resc->rw_qplist[conx_id]);

            fprintf(stdout, "DEBUG_INFO: rdma qp connected with machine #%d\n", conx_info_local[conx_id]->mcnID);
        }
        else{
            int mcnid = conx_id + 1;
            uint32_t qpn_rpc = conx_info_local[mcnid-1]->qpn_rpc;
            uint32_t qpn_dmrpc = conx_info_local[mcnid-1]->qpn_dmrpc;
            uint32_t qpn_rw = conx_info_local[mcnid-1]->qpn_rw;
            uint16_t rlid = conx_info_local[mcnid-1]->lid;

            modify_qp_to_rtr(resc->qp_list[mcnid-1], qpn_rpc, rlid);
            modify_qp_to_rts(resc->qp_list[mcnid-1]);

            modify_qp_to_rtr(resc->qpdm_list[mcnid-1], qpn_dmrpc, rlid);
            modify_qp_to_rts(resc->qpdm_list[mcnid-1]);

            modify_qp_to_rtr(resc->rw_qplist[mcnid-1], qpn_rw, rlid);
            modify_qp_to_rts(resc->rw_qplist[mcnid-1]);

            fprintf(stdout, "DEBUG_INFO: rdma qp connected with machine #%d\n", mcnid);
        }

        // ensure that this ibconx can start io function
        if (1){
            int sockfd = conx_info_local[conx_id]->syncfd;
            // sync command
            if (sockfd < 0){
                fprintf(stderr, "[ERR]: socket recall error in sync_start.\n");
                *ret = 1;
                return (void*)ret;
            }
            int a = sock_sync_data(sockfd, 1, "S", resc->procState + conx_id);
            if (a) {
                fprintf(stderr, "[ERR]: socket transfer error in sync_start.\n");
                *ret = 1;
                return (void*)ret;
            }
            if (t_arg->role == 0) 
                fprintf(stdout, "DEBUG_INFO: Center sync start success, at user %d, proc %d\n", resc->resc_id, conx_id);
            else 
                fprintf(stdout, "DEBUG_INFO: User sync start success, at proc %d, center %d\n", resc->resc_id, conx_id);
        }

        if (t_arg->role == 1) stoppable = 1;
    }

    return (void*)ret;
}


int center_tcp_listen(uint16_t mcnID, struct ibdev_resc* resc, struct ib_conx_info** conx_info_local, int remote_mcn){
    int i;
    pthread_t* pids = (pthread_t*)malloc(MAXCL_THNUM * sizeof(pthread_t));
    for (i = 0; i < MAXCL_THNUM; ++i){
        int cur_port = TCP_PORT_BASE + resc->resc_id * MAXCL_THNUM + i;
        struct tcp_exchange_arg *arg = (struct tcp_exchange_arg*)malloc(sizeof(struct tcp_exchange_arg));
        arg->loacl_mcnID = mcnID;
        arg->remot_mcnID = remote_mcn;
        arg->portid = cur_port;
        arg->resc = resc;
        arg->conx_info_local = conx_info_local;
        arg->conx_cnt = i;
        arg->role = 0;
        pthread_create(&(pids[i]), NULL, (void*)(&tcp_exchange), (void*)arg);
    }
    return 0;
}


int user_tcp_polling(uint16_t mcnID, struct ibdev_resc* resc, struct ib_conx_info** conx_info_local){
    int i;
    pthread_t* pids = (pthread_t*)malloc(CENTER_NUM * sizeof(pthread_t));
    for (i = 0; i < CENTER_NUM; ++i){
        int cur_port = TCP_PORT_BASE + (user_order[mcnID-1] - 1) * MAXCL_THNUM + resc->resc_id;
        struct tcp_exchange_arg *arg = (struct tcp_exchange_arg*)malloc(sizeof(struct tcp_exchange_arg));
        arg->loacl_mcnID = mcnID;
        arg->remot_mcnID = center_mcnid[i];
        arg->portid = cur_port;
        arg->resc = resc;
        arg->conx_info_local = conx_info_local;
        arg->conx_cnt = center_mcnid[i] - 1;
        arg->role = 1;
        pthread_create(&(pids[i]), NULL, (void*)(&tcp_exchange), (void*)arg);
    }
    return 0;
}


struct send_halt_arg{
    struct ib_conx_info** conx_info_local;
    int i;
    char* remote;
};


const uint64_t MANUAL_TIME = 25;


void* send_halt_func(void* arg){
    struct send_halt_arg* targ = (struct send_halt_arg*)arg;

    char* remote = targ->remote;
    struct ib_conx_info** conx_info_local = targ->conx_info_local;
    int targi = targ->i;
    int sockfd = conx_info_local[targi]->syncfd;

    struct caltime_nano* timer = init_caltime_nano();
    start_caltime_nano(timer);

    int userID = conx_info_local[targi]->mcnID;
    while (userID == 0)
        userID = conx_info_local[targi]->mcnID;

    int a = sock_sync_data(sockfd, 1, "H", remote);
    if (a) {
        fprintf(stderr, "[ERR]: first socket tansfer error in sync_halt.\n");
        return NULL;
    }
    uint64_t t1 = stop_caltime_nano(timer);

    char C = 0;
    if (t1 <= MANUAL_TIME * (uint64_t)1000000000) C = 'h';
    else C = 'H';

    a = sock_sync_data(sockfd, 1, &C, remote);
    if (a) {
        fprintf(stderr, "[ERR]: second socket tansfer error in sync_halt.\n");
        return NULL;
    }

    close(sockfd);
    fprintf(stdout, "DEBUG_INFO: Center sync_halt success(%c) at thread %d\n", C, targi);
    return NULL;
}


int center_issue_halt(struct ibdev_resc* resc, struct ib_conx_info** conx_info_local){
    int i;
    // array for all sockfd to close, NOT used now
    // int *sockfd = (int*)malloc(sizeof(int)*MAXCL_THNUM);

    // threadpool halt_pool = thpool_init(MAXCL_THNUM);

    struct caltime_nano* timer = init_caltime_nano();
    start_caltime_nano(timer);

    int cnttt = 0;
    while (cnttt == 0) {
        cnttt = 0;
        for (i = 0; i < MAXCL_THNUM; ++i) 
            if (conx_info_local[i]->mcnID != 0) {
                fprintf(stderr, "Find one in-proc conx %d in Center-Issue-Halt.\n", i);
                cnttt += 1;
            }
    }
    waitMiliSec(4000);

    for (i = 0; i < MAXCL_THNUM; ++i){

        struct send_halt_arg* targ = (struct send_halt_arg*)malloc(sizeof(struct send_halt_arg));
        targ->conx_info_local = conx_info_local;
        targ->i = i;
        targ->remote = resc->procState + i;

        // thpool_add_work(halt_pool, (void*)&send_halt_func, (void*)targ);

        char* remote = targ->remote;
        struct ib_conx_info** conx_info_local = targ->conx_info_local;
        int targi = targ->i;
        int sockfd = conx_info_local[targi]->syncfd;

        int userID = conx_info_local[targi]->mcnID;
        if (userID == 0) {
            fprintf(stdout, "DEBUG_INFO: Center sync_halt jumping at thread %d\n", targi);
            continue;
        }

        int a = sock_sync_data(sockfd, 1, "H", remote);
        if (a) {
            fprintf(stderr, "[ERR]: first socket tansfer error in sync_halt.\n");
            return 0;
        }
        uint64_t t1 = stop_caltime_nano(timer);

        char C = 0;
        if (t1 < MANUAL_TIME * (uint64_t)1000000000) C = 'h';
        else C = 'H';

        a = sock_sync_data(sockfd, 1, &C, remote);
        if (a) {
            fprintf(stderr, "[ERR]: second socket tansfer error in sync_halt.\n");
            return 0;
        }

        close(sockfd);
        fprintf(stdout, "DEBUG_INFO: Center sync_halt success(%c) at thread %d\n", C, targi);

        if (C == 'h') waitMiliSec(4000);
    }

    // waitMiliSec(MANUAL_TIME * 4 * 1000);
    // thpool_pause(halt_pool);
    // thpool_destroy(halt_pool);
    
    return 0;
}


int user_waiting_halt(struct ibdev_resc* resc, struct ib_conx_info** conx_info_local){
    int center_mainID = center_mcnid[0];
    char* serverip = (char*)malloc(20);
    memset(serverip, 0, 20);
    sprintf(serverip, "%s%d", mcnip_prefix, mcnip_suffix[center_mainID-1]);

    int sockfd = conx_info_local[center_mainID-1]->syncfd;
    // sync command
    if (sockfd < 0){
        fprintf(stderr, "[ERR]: socket reuse error in sync_halt.\n");
        return 1;
    }

    int a = sock_sync_data(sockfd, 1, "H", resc->procState + center_mainID-1);
    if (a) {
        // fprintf(stderr, "[ERR]: socket tansfer error in sync_halt.\n");
        return 1;
    }
    fprintf(stdout, "DEBUG_INFO: User sync_halt #1 success\n");
    a = sock_sync_data(sockfd, 1, "H", resc->procState + center_mainID-1);
    if (a) {
        // fprintf(stderr, "[ERR]: socket tansfer error in sync_halt.\n");
        return 1;
    }
    fprintf(stdout, "DEBUG_INFO: User sync_halt #2 success\n");

    char center_char = *(resc->procState + center_mainID-1);
    close(sockfd);
    if (center_char == 'H'){
        return 0;
    }
    if (center_char == 'h'){
        return 1;
    }
    return -1;
}


struct ibdev_resc* create_ibdev_resc(const char* dev_name, 
                                    char* data_reg, // for center, pmem pool; for user, pmem cache
                                    uint8_t roleBit, // 0 as center, 1 as user
                                    int resc_id // get as proc-create order
){
    // fprintf(stdout, "DEBUG_INFO: Here is the ibdev get function \n");
    struct ibv_device **dev_list;
    int num_dev;
    dev_list = ibv_get_device_list(&num_dev);
    // fprintf(stdout, "DEBUG_INFO: The total number of ib devices is: %d\n", num_dev);
    struct ibv_device *dev;
    const char* cur_devname;
    int i;
    for (i = 0; i < num_dev; ++i){
        cur_devname =  ibv_get_device_name(dev_list[i]);
        if (!strcmp(dev_name, cur_devname)){
            // fprintf(stdout, "DEBUG_INFO: Target rdma NIC device found!\n");
            dev = dev_list[i];
            break;
        }
    }
    ibv_free_device_list(dev_list);
    if (i >= num_dev) fprintf(stderr, "[ERR]: NO matched IB device found!\n");
    
    // fprintf(stdout, "DEBUG_INFO: Here is the create resource function\n");
    struct ibdev_resc* resc = (struct ibdev_resc*)malloc(sizeof(struct ibdev_resc));
    resc->resc_id = resc_id;

    resc->dev_ctx = ibv_open_device(dev); 
    int a = ibv_query_port(resc->dev_ctx, IB_PORT_ID, &resc->port_attr);
    fprintf(stdout, "DEBUG_INFO: local ib port's id: %d\n", resc->port_attr.lid);
    a = ibv_query_device(resc->dev_ctx, &resc->dev_attr);
    fprintf(stdout, "DEBUG_INFO: local device's max qp: %d\n", resc->dev_attr.max_qp);
    // fprintf(stdout, "DEBUG_INFO: local device's max mr: %d\n", resc->dev_attr.max_mr);
    fprintf(stdout, "DEBUG_INFO: local device's atomic cap: %d\n", resc->dev_attr.atomic_cap);

    struct ibv_device_attr_ex attr_ex; 
    a = ibv_query_device_ex(resc->dev_ctx, NULL, &attr_ex);
    // fprintf(stdout, "DEBUG_INFO: local device's max_dm_size: %d\n", attr_ex.max_dm_size);
    fprintf(stdout, "DEBUG_INFO: local device's pci_atomic_caps: %d\n", attr_ex.pci_atomic_caps);

    int pNUM = 0;
    if (roleBit == 0) pNUM = MAXCL_THNUM;
    else if (roleBit == 1) pNUM = MCN_NUM;
    else {
        fprintf(stderr, "[WARN]: invalid rolebit, cannot sign rdma_meta size.\n");
    }

    resc->pd = ibv_alloc_pd(resc->dev_ctx);

    resc->cq_send = (struct ibv_cq**)malloc(pNUM * sizeof(struct ibv_cq*));
    resc->cq_recv = (struct ibv_cq**)malloc(pNUM * sizeof(struct ibv_cq*));
    resc->cq_rw = (struct ibv_cq**)malloc(pNUM * sizeof(struct ibv_cq*));
    for (i = 0; i < pNUM; ++i){
        resc->cq_send[i] = ibv_create_cq(resc->dev_ctx, IB_CQ_SIZE, NULL, NULL, 0);
        resc->cq_recv[i] = ibv_create_cq(resc->dev_ctx, IB_CQ_SIZE, NULL, NULL, 0);
        resc->cq_rw[i] = ibv_create_cq(resc->dev_ctx, IB_CQ_SIZE, NULL, NULL, 0);
    }

    resc->qp_list = (struct ibv_qp**)malloc(pNUM * sizeof(struct ibv_qp*));
    resc->qpdm_list = (struct ibv_qp**)malloc(pNUM * sizeof(struct ibv_qp*));
    resc->rw_qplist = (struct ibv_qp**)malloc(pNUM * sizeof(struct ibv_qp*));

    struct ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    qp_init_attr.cap.max_send_wr = 1;
    qp_init_attr.cap.max_recv_wr = RPC_RECV_PRE * 2;
    for (i = 0; i < pNUM; ++i){
        qp_init_attr.send_cq = resc->cq_send[i];
        qp_init_attr.recv_cq = resc->cq_recv[i];
        resc->qp_list[i] = ibv_create_qp(resc->pd, &qp_init_attr);
        if (!i) fprintf(stdout, "DEBUG_INFO: QP max inline data %d\n", qp_init_attr.cap.max_inline_data);
        resc->qpdm_list[i] = ibv_create_qp(resc->pd, &qp_init_attr);
    }
    for (i = 0; i < pNUM; ++i){
        qp_init_attr.send_cq = resc->cq_rw[i];
        qp_init_attr.recv_cq = resc->cq_rw[i];
        resc->rw_qplist[i] = ibv_create_qp(resc->pd, &qp_init_attr);
    }
    fprintf(stdout, "DEBUG_INFO: rpcQPs are created, qp[0]n=%d\n", resc->qp_list[0]->qp_num);
    // fprintf(stdout, "DEBUG_INFO: dmQPs are created, qp[0]n=%d\n", resc->qpdm_list[0]->qp_num);
    fprintf(stdout, "DEBUG_INFO: rwQPs are created, qp[0]n=%d\n", resc->rw_qplist[0]->qp_num);

    resc->rpcmr_cnt = 0;
    
    resc->rpcmr_list = (struct ibv_mr**)malloc(pNUM * sizeof(struct ibv_mr*));
    resc->rpc_buf = (char**)malloc(pNUM * sizeof(char*));
    for (i = 0; i < pNUM; ++i){
        resc->rpc_buf[i] = (char*)malloc(RPC_MR_SIZE*2);
        memset(resc->rpc_buf[i], 0, RPC_MR_SIZE*2);
    }
    int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    for (i = 0; i < pNUM; ++i){
        resc->rpcmr_cnt += 1;
        resc->rpcmr_list[i] = ibv_reg_mr(resc->pd, resc->rpc_buf[i], RPC_MR_SIZE*2, mr_flags);
        if (!resc->rpcmr_list[i]) fprintf(stderr, "[ERR]: Allocate MR failed!\n");
    }
    fprintf(stdout, "DEBUG_INFO: MRs created, mr 0's addr: %p, rkey: %p\n", resc->rpc_buf[0], resc->rpcmr_list[0]->rkey);

    if (ALLOW_DM_RPC) {
        struct ibv_alloc_dm_attr *dm_attr = (struct ibv_alloc_dm_attr*)malloc(sizeof(struct ibv_alloc_dm_attr));
        memset(dm_attr, 0, sizeof(struct ibv_alloc_dm_attr));
        dm_attr->length = PRC_DM_SIZE * 2 * pNUM;
        dm_attr->log_align_req = 2;
        
        char* test_buf = (char*)malloc(PRC_DM_SIZE * 2 * (pNUM + 1));
        memset(test_buf, 0, PRC_DM_SIZE * 2 * (pNUM + 1));
        resc->dm_buf = ibv_alloc_dm(resc->dev_ctx, dm_attr);
        for (i = 0; i < 50; ++i){
            memset(test_buf, 'A' + i, PRC_DM_SIZE * 2 * pNUM);
            ibv_memcpy_to_dm(resc->dm_buf, 0, test_buf, PRC_DM_SIZE * 2 * pNUM); // set as zeros
            ibv_memcpy_from_dm(test_buf + PRC_DM_SIZE * 2 * pNUM, resc->dm_buf, PRC_DM_SIZE * 2 * (i % pNUM), PRC_DM_SIZE * 2);
            // fprintf(stderr, "DEBUG_INFO: in dm, char is %c\n", test_buf[PRC_DM_SIZE * 2 * pNUM + PRC_DM_SIZE]);
        }
        resc->dm_mr_list = (struct ibv_mr**)malloc(pNUM * sizeof(struct ibv_mr*));
        int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_ZERO_BASED;
        for (i = 0; i < pNUM; ++i) {
            resc->dm_mr_list[i] = ibv_reg_dm_mr(resc->pd, resc->dm_buf, PRC_DM_SIZE * 2 * i, PRC_DM_SIZE * 2, mr_flags);
            if (!resc->dm_mr_list[i]) fprintf(stderr, "[ERR]: Allocate device memory mr failed!\n");
        }
        // fprintf(stdout, "DEBUG_INFO: dmMRs created, addr: %p, lkey0: %d\n", resc->dm_buf, resc->dm_mr_list[0]->lkey);
    }

    resc->data_buf = data_reg;
    uint64_t data_bufLen = 0;
    if (roleBit == 0) data_bufLen = CENTER_MR_SIZE;
    else if (roleBit == 1) data_bufLen = USER_MR_SIZE;
    else {
        fprintf(stderr, "[WARN]: invalid rolebit, no Data-MR.\n");
    }
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    resc->datamr = ibv_reg_mr(resc->pd, resc->data_buf, data_bufLen, mr_flags);
    fprintf(stdout, "RUN INFO: Data-MR created at addr: %p, res is %p.\n", resc->data_buf, resc->datamr);
    
    resc->procState = (char*)malloc(MAXCL_THNUM + 1); 
    memset(resc->procState, 0, MAXCL_THNUM + 1);

    resc->chunk_cache = (char*)malloc((1<<20));
    memset(resc->chunk_cache, 0, (1<<20));
    resc->chunkmr = ibv_reg_mr(resc->pd, resc->chunk_cache, (1<<20), mr_flags | IBV_ACCESS_REMOTE_ATOMIC);
    if (!resc->chunkmr) fprintf(stderr, "[ERR]: Allocate single-chunk memory mr failed!\n");

    return resc; 
}


int destroy_ibdev_resc(struct ibdev_resc* resc, int roleBit){
    fprintf(stdout, "DEBUG_INFO: ready to destroy ibdev resources\n");

    int pNUM;
    if (roleBit == 0) pNUM = MAXCL_THNUM;
    else if (roleBit == 1) pNUM = MCN_NUM;
    // sprintf(resc->procState, "E");
    memset(resc->procState, 'E', pNUM);

    // Actually wait for cq polling to stop.
    waitMiliSec(500);

    int i, a;
    for (i = 0; i < pNUM; ++i){
        a = ibv_dereg_mr(resc->rpcmr_list[i]);
        if (a) fprintf(stderr, "[ERR]: ibv_dereg_mr error!\n");
        if(ALLOW_DM_RPC) a = ibv_dereg_mr(resc->dm_mr_list[i]);
        if (a) fprintf(stderr, "[ERR]: ibv_dereg_mr error!\n");
        free(resc->rpc_buf[i]);
    }
    free(resc->rpcmr_list);
    if(ALLOW_DM_RPC) a = ibv_free_dm(resc->dm_buf);
    if (a) fprintf(stderr, "[ERR]: ibv_free_dm error!\n");
    if(ALLOW_DM_RPC) free(resc->dm_mr_list);

    if (resc->datamr){
        a = ibv_dereg_mr(resc->datamr);
        if (a) fprintf(stderr, "[ERR]: ibv_dereg_mr error!\n");
        // no need to free the data_buf
    }

    for (i = 0; i < pNUM; ++i){
        a = ibv_destroy_qp(resc->qp_list[i]);
        if (a) fprintf(stderr, "[ERR]: ibv_destroy_qp error!\n");
        if(ALLOW_DM_RPC) a = ibv_destroy_qp(resc->qpdm_list[i]);
        if (a) fprintf(stderr, "[ERR]: ibv_destroy_qp error!\n");
    }
    free(resc->qp_list);
    if(ALLOW_DM_RPC) free(resc->qpdm_list);

    for (i = 0; i < pNUM; ++i){
        a = ibv_destroy_qp(resc->rw_qplist[i]);
        if (a) fprintf(stderr, "[ERR]: ibv_destroy_qp error!\n");
    }
    free(resc->rw_qplist);
    
    for (i = 0; i < pNUM; ++i){
        a = ibv_destroy_cq(resc->cq_send[i]);
        if (a) perror("ibv_destroy_cq:");
        a = ibv_destroy_cq(resc->cq_recv[i]);
        if (a) perror("ibv_destroy_cq:");
        a = ibv_destroy_cq(resc->cq_rw[i]);
        if (a) perror("ibv_destroy_cq:");
    }
    free(resc->cq_send);
    free(resc->cq_recv);
    free(resc->cq_rw);

    a = ibv_dealloc_pd(resc->pd);
    if (a) fprintf(stderr, "[ERR]: ibv_dealloc_pd error!\n");
    a = ibv_close_device(resc->dev_ctx);
    if (a) fprintf(stderr, "[ERR]: ibv_close_device error!\n");

    free(resc);
    return 0;
}


int post_send(struct ibdev_resc *resc, int target_mcnid, int target_procid, uint32_t length)
{
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int a;
    memset(&sge, 0, sizeof(sge));

    int target;
    if (target_mcnid >= 0) target = target_mcnid - 1;
    if (target_procid >= 0) target = target_procid;

    if (length < DM_FLAG_LENT) {
        sge.addr = (uint64_t)resc->rpc_buf[target];
        sge.length = length;
        if (sge.length > RPC_MR_SIZE) perror("RPC buffer OverFLow!\n");
        sge.lkey = resc->rpcmr_list[target]->lkey;
    }
    else {
        sge.addr = 0;
        sge.length = length - DM_FLAG_LENT;
        if (sge.length > PRC_DM_SIZE) perror("RPC dm buffer OverFLow!\n");
        sge.lkey = resc->dm_mr_list[target]->lkey;
    }

    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_SEND;
    sr.send_flags = IBV_SEND_SIGNALED;

    if (length < DM_FLAG_LENT) {
        if (sge.length <= 60) sr.send_flags |= IBV_SEND_INLINE;
        a = ibv_post_send(resc->qp_list[target], &sr, &bad_wr);
    }
    else {
        a = ibv_post_send(resc->qpdm_list[target], &sr, &bad_wr);
    }

    if (a) {
        pid_t tid = syscall(__NR_gettid);
        fprintf(stderr, "Thread # %d failed to post Rdma Send\n", tid);
        perror("Rdma Send Failed err: ");
    }
    // else fprintf(stdout, "DEBUG_INFO: Send Request posted\n");
    return a;
}


int post_receive(struct ibdev_resc *resc, int target_mcnid, int target_procid, int length)
{
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;
    int a;
    memset(&sge, 0, sizeof(sge));

    int target;
    if (target_mcnid >= 0) target = target_mcnid - 1;
    if (target_procid >= 0) target = target_procid;

    if (length < DM_FLAG_LENT) {
        sge.addr = (uint64_t)resc->rpc_buf[target] + RPC_MR_SIZE;
        sge.length = length;
        if (sge.length > RPC_MR_SIZE) perror("RPC buffer OverFLow!\n");
        sge.lkey = resc->rpcmr_list[target]->lkey;
    }
    else {
        sge.addr = PRC_DM_SIZE;
        sge.length = length - DM_FLAG_LENT;
        if (sge.length > PRC_DM_SIZE) perror("RPC dm buffer OverFLow!\n");
        sge.lkey = resc->dm_mr_list[target]->lkey;
    }

    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;

    if (length < DM_FLAG_LENT) {
        a = ibv_post_recv(resc->qp_list[target], &rr, &bad_wr);
    }
    else{
        a = ibv_post_recv(resc->qpdm_list[target], &rr, &bad_wr);
    }

    if (a) perror("failed to post Rdma Receive:\n");
    // else fprintf(stdout, "DEBUG_INFO: Receive Request posted\n");
    return a;
}


int post_read(struct ibdev_resc *resc, struct ib_conx_info** conx_info_local, 
        int target_mcnid, int target_procid, uint32_t length, uint64_t loff, uint64_t roff,
        uint64_t addr, uint32_t lkey)
{
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int a;
    memset(&sge, 0, sizeof(sge));

    int target;
    if (target_mcnid >= 0) target = target_mcnid - 1;
    if (target_procid >= 0) target = target_procid;

    if (!addr) sge.addr = (uint64_t)(resc->data_buf) + loff;
    else sge.addr = addr;
    sge.length = length;
    if (!addr) sge.lkey = resc->datamr->lkey;
    else sge.lkey = lkey;
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_RDMA_READ;
    sr.send_flags = IBV_SEND_SIGNALED;
    uint64_t r_addr = conx_info_local[target]->addr_rw;
    uint32_t r_key = conx_info_local[target]->rkey_rw;
    sr.wr.rdma.remote_addr = r_addr + roff;
    sr.wr.rdma.rkey = r_key;
    a = ibv_post_send(resc->rw_qplist[target], &sr, &bad_wr);
    if (a) perror("failed to post Rdma Read:\n");
    // else fprintf(stdout, "DEBUG_INFO: Rdma Read posted at offset %llu\n", roff);
    return a;
}


int post_write(struct ibdev_resc *resc, struct ib_conx_info** conx_info_local, 
        int target_mcnid, int target_procid, uint32_t length, uint64_t loff, uint64_t roff, 
        uint64_t addr, uint32_t lkey)
{
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int a;
    memset(&sge, 0, sizeof(sge));
    
    int target;
    if (target_mcnid >= 0) target = target_mcnid - 1;
    if (target_procid >= 0) target = target_procid;

    if (!addr) sge.addr = (uint64_t)(resc->data_buf) + loff;
    else sge.addr = addr;
    sge.length = length;
    if (!addr) sge.lkey = resc->datamr->lkey;
    else sge.lkey = lkey;
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_RDMA_WRITE;
    sr.send_flags = IBV_SEND_SIGNALED;
    uint64_t r_addr = conx_info_local[target]->addr_rw;
    uint32_t r_key = conx_info_local[target]->rkey_rw;
    sr.wr.rdma.remote_addr = r_addr + roff;
    sr.wr.rdma.rkey = r_key;
    a = ibv_post_send(resc->rw_qplist[target], &sr, &bad_wr);
    if (a) perror("failed to post Rdma Write");
    // else fprintf(stdout, "DEBUG_INFO: Rdma Write posted at offset %llu\n", roff);
    return a;
}


int ibpoll_completion(char* procState, struct ibv_cq *target_cq, int timeout, int roleBit, int pass_pre)
{   
    int qNUM = 0;
    if (roleBit == 0) qNUM = MAXCL_THNUM;
    if (roleBit == 1) qNUM = MCN_NUM;
    int sNUM, hNUM, eNUM;

    struct ibv_wc wc;
    unsigned long start_time_msec;
    unsigned long cur_time_msec;
    struct timeval cur_time;
    int poll_result;
    gettimeofday(&cur_time, NULL);
    start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);

    // fprintf(stderr, "opname ::1.6 %d\n", roleBit);

    do
    {   
        poll_result = ibv_poll_cq(target_cq, 1, &wc);
        gettimeofday(&cur_time, NULL);
        cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    } while ((poll_result == 0) && ((cur_time_msec - start_time_msec) < timeout));

    // fprintf(stderr, "opname ::1.8 %d\n", roleBit);

    if (poll_result < 0) {
        fprintf(stderr, "[ERR]: poll CQ failed.\n"); 
        return -1;
    }else if (poll_result == 0) {
        fprintf(stderr, "[ERR]: no completion after timeout!\n");
        return -1;
    }else if (wc.status != IBV_WC_SUCCESS){
        fprintf(stderr, "[ERR]: bad completion: 0x%x, vendor syndrome: 0x%x\n", 
            wc.status, wc.vendor_err);
        return -1;
    }else{
        // fprintf(stdout, "DEBUG_INFO: completion from qp %d found.\n", wc.qp_num);
    }
    int ret = wc.qp_num;
    return ret;
}


// Struct for in-flight locks information
struct inflight_lockinfo{
    uint32_t infs_iid;
    uint16_t rw_flag;
    uint16_t lostat;
    uint64_t beg;
    uint64_t length;
};


#endif
