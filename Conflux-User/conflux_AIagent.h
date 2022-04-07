#ifndef __CONFLUX_AIAGENT_H
#define __CONFLUX_AIAGENT_H


#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <math.h>
#include <time.h>
#include <stdint.h>
#include <tensorflow/c/c_api.h>
#include <tensorflow/lite/c_api.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include "dcache_timer.c"


#define APPLY_NVMMIO 0
#define APPLY_RDMAIO 1
#define MODEL_NINPUT 2
#define MODEL_NOUTPUT 1
#define DEVIO_CLASS 12


const char* TF_MODEL_DIR = "Path-to-Conflux/Conflux-User/PyTfcode/model/";
const char* TFLITE_FILE = "Path-to-Conflux/Conflux-User/PyTfcode/model.tflite";


int stdrand_init(){
    struct timespec times;
    clock_gettime(CLOCK_MONOTONIC, &times);
    uint32_t a = (uint32_t)(times.tv_nsec);
    srand(a);
    int b = rand();
    b = rand();
    return 1;
}


static int stdrand_initflag = 0;


int random_decision(uint64_t iosize, uint64_t iosize_thrsh, float randf){
    if (!stdrand_init) stdrand_initflag = stdrand_init();
    if (iosize <= iosize_thrsh)
        return APPLY_NVMMIO;
    float r = (float)rand() / (float)RAND_MAX;
    if (r >= randf) return APPLY_NVMMIO;
    else return APPLY_RDMAIO;
}


static const float MY_PIF = 3.14159265;
static const float MY_PIF_harf = 1.57079633;
static inline float split_curve(float x){
    return (sinf(MY_PIF * x - MY_PIF_harf) * 0.998 + 1) / 2;
}


// Change this by hand, 3 lines below are related.
// Larger stage means more similarity to 0-1 jump function.
// But considering the convergence of fix-point interation,
// stage can not be too large to make f'r*g'v>1
static const float approx_func_stage = 2.5; 
static int approx_stage_int = 2;
static float approx_stage_res = 0.5;
static int admis_thres = 6;



const char* pytf_shmname = "QZLSHM_pyscope";
const char* model_tags = "serve";
const char* input01_name = "serving_default_input_1";
const char* input02_name = "serving_default_input_2";
const char* output_name = "StatefulPartitionedCall";
int C_qndims = 2;
int C_nqdata = sizeof(float) * (DEVIO_CLASS * 4); 
int64_t C_qdims[] = {1,(DEVIO_CLASS * 4)};
int C_xndims = 1;
int C_nxdata = sizeof(int32_t);
int64_t C_xdims[] = {1};
int C_yndims = 1;
int C_nydata = sizeof(float);
int64_t C_ydims[] = {1};

const char* PROCOWN_DATA_PRE = "QZLSHM_PROCOWN_DATA";

const uint32_t PYSHM_HEAD_LEN = 64;
const uint32_t PYSHM_QDATA_LEN = 4 * (DEVIO_CLASS * 4) * 100000;
const uint32_t PYSHM_XDATA_LEN = 4 * 1 * 100000;
const uint32_t PYSHM_YDATA_LEN = 4 * 1 * 100000;
const uint32_t PYSHM_ENTRY_NUM = 100000;
const uint32_t PYSHM_LENT = 64 + 4 * (DEVIO_CLASS * 4 + 2) * 100000;
const uint32_t PYSHM_QDATA_OFF = 64;
const uint32_t PYSHM_XDATA_OFF = 64 + 4*(DEVIO_CLASS * 4)*100000;
const uint32_t PYSHM_YDATA_OFF = 64 + 4*(DEVIO_CLASS * 4 + 1)*100000;


struct TFkeras_Model{
    const char* tfver;
    uint64_t model_ver;
    char* pyshm;
    char* model_path;
    TF_Graph* Graph;
    TF_Status* Status;
    TF_SessionOptions* SessionOpts;
    TF_Buffer* RunOpts;
    TF_Session* Session;
    TF_Output* Input;
    TF_Output* Output;
    int NumInputs;
    int NumOutputs;
    TF_Tensor** InputValues;
    TF_Tensor** OutputValues;
    void* in01_buff;
    void* in02_buff;
    const char **prun_hundle;
};


struct TFkeras_Model* init_pytfmodel(const char* pathname){
    struct TFkeras_Model* model_obj = (struct TFkeras_Model*)malloc(sizeof(struct TFkeras_Model));
    memset(model_obj, 0, sizeof(struct TFkeras_Model));
    model_obj->tfver  = TF_Version();
    fprintf(stdout, "RUN_INFO: tf-version is: %s \n", model_obj->tfver);
    model_obj->model_ver = 0;
    // Get and Set shared memory
    int pyshmfd = shm_open(pytf_shmname, O_RDWR, 0660);
    fprintf(stdout, "RUN_INFO: the pyshm fd: %d\n", pyshmfd);
    char* pyshm = (char*)mmap(NULL, PYSHM_LENT, PROT_WRITE, MAP_SHARED, pyshmfd, 0);
    int8_t pyshm_stat = *(int8_t*)pyshm;
    fprintf(stdout, "RUN_INFO: the pyshm state: %d\n", (int)pyshm_stat);
    model_obj->pyshm = pyshm;
    int path_len = strlen(pathname);
    model_obj->model_path = (char*)malloc(path_len+1);
    memset(model_obj->model_path, 0, path_len+1);
    memcpy(model_obj->model_path, pathname, path_len);
    model_obj->Graph = TF_NewGraph();
    model_obj->Status = TF_NewStatus();
    model_obj->SessionOpts = TF_NewSessionOptions();
    model_obj->RunOpts = NULL;
    model_obj->Session = NULL;
    model_obj->NumInputs = 2;
    model_obj->Input = malloc(sizeof(TF_Output) * model_obj->NumInputs);
    model_obj->NumOutputs = 1;
    model_obj->Output = malloc(sizeof(TF_Output) * model_obj->NumOutputs);
    model_obj->InputValues = (TF_Tensor**)malloc(sizeof(TF_Tensor*)*model_obj->NumInputs);
    model_obj->OutputValues = (TF_Tensor**)malloc(sizeof(TF_Tensor*)*model_obj->NumOutputs);
    close(pyshmfd);
    return model_obj;
}


int trigger_retrain(struct TFkeras_Model* model_obj){
    char* pyshm = model_obj->pyshm;
    *(int8_t*)pyshm = (int8_t)1;
    fprintf(stdout, "RUN_INFO: the pyshm state set as 1, Retrain.\n");
    int8_t r = *(int8_t*)pyshm;
    while (r != 0) r = *(int8_t*)pyshm;
    return 0;
}


int reload_model(struct TFkeras_Model* model_obj){
    int ntags = 1;
    model_obj->Graph = TF_NewGraph();
    model_obj->Session = TF_LoadSessionFromSavedModel(model_obj->SessionOpts, model_obj->RunOpts, 
                                                model_obj->model_path, &model_tags, ntags, 
                                                model_obj->Graph, NULL, model_obj->Status);
    if(TF_GetCode(model_obj->Status) == TF_OK){
        // printf("RUN_INFO: TF_LoadSessionFromSavedModel OK\n");
    }
    else{
        printf("ERROR: %s\n",TF_Message(model_obj->Status));
    }
    TF_Output t0 = {TF_GraphOperationByName(model_obj->Graph, input01_name), 0};
    if(t0.oper == NULL)
        fprintf(stderr, "ERROR: Failed TF_GraphOperationByName %s\n", input01_name);
    else{
        // printf("RUN_INFO: TF_GraphOperationByName %s is OK\n", input01_name);
    }
    model_obj->Input[0] = t0;

    TF_Output t1 = {TF_GraphOperationByName(model_obj->Graph, input02_name), 0};
    if(t1.oper == NULL)
        fprintf(stderr, "ERROR: Failed TF_GraphOperationByName %s\n", input02_name);
    else{
        // printf("RUN_INFO: TF_GraphOperationByName %s is OK\n", input02_name);
    }
    model_obj->Input[1] = t1;

    TF_Output t2 = {TF_GraphOperationByName(model_obj->Graph, output_name), 0};
    if(t2.oper == NULL)
        fprintf(stderr, "ERROR: Failed TF_GraphOperationByName %s\n", output_name);
    else{
        // printf("RUN_INFO: TF_GraphOperationByName %s is OK\n", output_name);
    }
    model_obj->Output[0] = t2;

    // Up-move the tensor_alloc operations
    model_obj->InputValues[0] = TF_AllocateTensor(TF_FLOAT, C_qdims, C_qndims, C_nqdata);
    model_obj->InputValues[1] = TF_AllocateTensor(TF_INT32, C_xdims, C_xndims, C_nxdata);
    model_obj->OutputValues[0] = TF_AllocateTensor(TF_FLOAT, C_ydims, C_yndims, C_nydata);
    model_obj->in01_buff = TF_TensorData(model_obj->InputValues[0]);
    model_obj->in02_buff = TF_TensorData(model_obj->InputValues[1]);

    // Test section
    size_t pos = 0;
	TF_Operation* oper;
	
	while ((oper = TF_GraphNextOperation(model_obj->Graph, &pos)) != NULL) {
		printf(TF_OperationName(oper));
		printf("\n");
	}
    const TF_Operation* oper_tar[1] = {TF_GraphOperationByName(model_obj->Graph, "NoOp")};

    TF_SessionRun(model_obj->Session, NULL, model_obj->Input, model_obj->InputValues, model_obj->NumInputs, 
                model_obj->Output, model_obj->OutputValues, model_obj->NumOutputs, oper_tar, 1, NULL, model_obj->Status);
    if(TF_GetCode(model_obj->Status) == TF_OK)
        printf("RUN_INFO: Session is OK\n");
    else
        printf("%s\n",TF_Message(model_obj->Status));

    model_obj->prun_hundle = (const char**)malloc(sizeof(const char*)*2);
    TF_SessionPRunSetup(model_obj->Session, model_obj->Input, model_obj->NumInputs, 
                model_obj->Output, model_obj->NumOutputs, NULL, 0, model_obj->prun_hundle, model_obj->Status);
    if(TF_GetCode(model_obj->Status) == TF_OK)
        printf("RUN_INFO: Session is OK\n");
    else
        printf("%s\n",TF_Message(model_obj->Status));
    printf("\n\n The PRun Handle returned: %s \n\n", model_obj->prun_hundle[0]);


    int j;
    for (j = 0; j < 1; ++j){
        TF_SessionPRun(model_obj->Session, model_obj->prun_hundle[0], model_obj->Input, model_obj->InputValues, model_obj->NumInputs, 
                    model_obj->Output, model_obj->OutputValues, model_obj->NumOutputs, NULL, 0, model_obj->Status);
        if(TF_GetCode(model_obj->Status) == TF_OK)
            printf("RUN_INFO: Session is OK\n");
        else
            printf("%s\n",TF_Message(model_obj->Status));
    }

    return 0;
}


float inference_with(struct TFkeras_Model* model_obj, float* qdata, int xdata){
    memcpy(model_obj->in01_buff, qdata, C_nqdata);
    memcpy(model_obj->in02_buff, &xdata, C_nxdata);
    const TF_Operation* oper_tar[1] = {TF_GraphOperationByName(model_obj->Graph, "NoOp")};
    // Run the Session
    TF_SessionRun(model_obj->Session, NULL, model_obj->Input, model_obj->InputValues, model_obj->NumInputs, 
                model_obj->Output, model_obj->OutputValues, model_obj->NumOutputs, oper_tar, 1, NULL, model_obj->Status);
    
    // if(TF_GetCode(model_obj->Status) == TF_OK)
    //     printf("RUN_INFO: Session is OK\n");
    // else
    //     printf("%s",TF_Message(model_obj->Status));
    if(TF_GetCode(model_obj->Status) != TF_OK)
        printf("ERROR: %s\n",TF_Message(model_obj->Status));

    // Get the output result
    void* buff = TF_TensorData(model_obj->OutputValues[0]);
    float result = ((float*)buff)[0];
    printf("RUN_INFO: Result Tensor val is \n");
    printf("    %f\n", result);

    return result;
}


int destroy_pytfmodel(struct TFkeras_Model* model_obj){
    TF_DeleteTensor(model_obj->InputValues[0]);
    TF_DeleteTensor(model_obj->InputValues[1]);
    TF_DeleteGraph(model_obj->Graph);
    TF_DeleteSession(model_obj->Session, model_obj->Status);
    TF_DeleteSessionOptions(model_obj->SessionOpts);
    TF_DeleteStatus(model_obj->Status);
    *(int8_t*)(model_obj->pyshm) = (int8_t)3;
    fprintf(stdout, "RUN_INFO: the pyshm state set as 3, Exit.\n");
    munmap(model_obj->pyshm, PYSHM_LENT);
    return 0;
}


// Below are struct and functions for TFLite

struct TFLite_Model_Obj{
    char* lite_model_path;
    char* pyshm;
    TfLiteModel* lite_model;
    TfLiteInterpreter* interpreter;
    uint32_t model_version;
    TfLiteStatus tflite_state;
    int Input_Num;
    int Output_Num;
    TfLiteTensor* in00;
    TfLiteTensor* in01;
};


struct TFLite_Model_Obj* init_tflite_model(const char* pathname){
    struct TFLite_Model_Obj* tflite_model = (struct TFLite_Model_Obj*)malloc(sizeof(struct TFLite_Model_Obj));
    memset(tflite_model, 0, sizeof(struct TFLite_Model_Obj));

    int pathname_len = strlen(pathname);
    tflite_model->lite_model_path = (char*)malloc(pathname_len + 1);
    memcpy(tflite_model->lite_model_path, pathname, pathname_len);
    tflite_model->lite_model_path[pathname_len] = 0;

    tflite_model->model_version = 0;
    // Get and Set shared memory
    int pyshmfd = shm_open(pytf_shmname, O_RDWR, 0660);
    // fprintf(stdout, "RUN_INFO: the pyshm fd: %d\n", pyshmfd);
    char* pyshm = (char*)mmap(NULL, PYSHM_LENT, PROT_WRITE, MAP_SHARED, pyshmfd, 0);
    int8_t pyshm_stat = *(int8_t*)pyshm;
    // fprintf(stdout, "RUN_INFO: the pyshm state: %d\n", (int)pyshm_stat);
    tflite_model->pyshm = pyshm;

    tflite_model->lite_model = TfLiteModelCreateFromFile((const char*)(tflite_model->lite_model_path));
    if (!(tflite_model->lite_model)) fprintf(stderr, "ERROR: no model loaded from %s.\n", tflite_model->lite_model_path);
    TfLiteInterpreterOptions* options = TfLiteInterpreterOptionsCreate();
    tflite_model->interpreter = TfLiteInterpreterCreate(tflite_model->lite_model, options);
    if (!(tflite_model->interpreter)) fprintf(stderr, "ERROR: no interpreter created!\n");
    tflite_model->tflite_state = kTfLiteOk;

    // Allocate tensors and get the pointer to the input tensor.
    TfLiteInterpreterAllocateTensors(tflite_model->interpreter);
    tflite_model->Input_Num = TfLiteInterpreterGetInputTensorCount(tflite_model->interpreter);
    tflite_model->Output_Num = TfLiteInterpreterGetOutputTensorCount(tflite_model->interpreter);
    if (tflite_model->Input_Num != MODEL_NINPUT || tflite_model->Output_Num != MODEL_NOUTPUT)
        fprintf(stderr, "ERROR: The presettled input-output num invalid!\n");
    // fprintf(stdout, "RUN_INFO: input num is %d, output num is %d.\n", tflite_model->Input_Num, tflite_model->Output_Num);
    tflite_model->in00 = TfLiteInterpreterGetInputTensor(tflite_model->interpreter, 0);
    tflite_model->in01 = TfLiteInterpreterGetInputTensor(tflite_model->interpreter, 1);
    // fprintf(stdout, "RUN_INFO: in00 size is %d\n", TfLiteTensorByteSize(tflite_model->in00));
    // fprintf(stdout, "RUN_INFO: in01 size is %d\n", TfLiteTensorByteSize(tflite_model->in01));

    return tflite_model;
}


int free_tflite_model(struct TFLite_Model_Obj* lite_model_obj){
    TfLiteInterpreterDelete(lite_model_obj->interpreter);
    free(lite_model_obj->lite_model_path);
    TfLiteModelDelete(lite_model_obj->lite_model);
    int8_t n3 = 3;
    int8_t n4 = 4;
    *(int8_t*)(lite_model_obj->pyshm) = n3;
    fprintf(stderr, "RUN_INFO: the pyshm state set as 3, Ready to exit.\n");
    while (*(int8_t*)(lite_model_obj->pyshm) != n4){
        n4 = 4;
    }
    fprintf(stderr, "RUN_INFO: the pyshm state changed to 4, Exit.\n");
    munmap(lite_model_obj->pyshm, PYSHM_LENT);
    free(lite_model_obj);
    return 0;
}


float predict_latency(struct TFLite_Model_Obj* model_obj, float* qdata, int32_t* xdata){
    float res = 0;
    TfLiteTensorCopyFromBuffer(model_obj->in00, (const void*)qdata, C_nqdata);
    TfLiteTensorCopyFromBuffer(model_obj->in01, (const void*)xdata, C_nxdata);
    // Call the Inference
    TfLiteInterpreterInvoke(model_obj->interpreter);
    const TfLiteTensor* out = TfLiteInterpreterGetOutputTensor(model_obj->interpreter, 0);
    TfLiteTensorCopyToBuffer(out, &res, 4);
    return res;
}


// working proc actively require tf-model to retrain, actually NOT used now
int trigger_tflite_retrain(struct TFLite_Model_Obj* model_obj){
    char* pyshm = model_obj->pyshm;
    *(int8_t*)pyshm = (int8_t)1;
    // fprintf(stdout, "RUN_INFO: the pyshm state set as 1, Retrainning.\n");
    int8_t r = *(int8_t*)pyshm;
    while (r != 0) r = *(int8_t*)pyshm;
    return 0;
}


int reload_tflite_model(struct TFLite_Model_Obj* model_obj, int version_num){
    char* newest_model_path = (char*)malloc(80);
    memset(newest_model_path, 0, 80);
    sprintf(newest_model_path, "%s.%d", model_obj->lite_model_path, version_num);
    model_obj->lite_model = TfLiteModelCreateFromFile((const char*)newest_model_path);
    if (!(model_obj->lite_model)) fprintf(stderr, "ERROR: no model loaded from %s.\n", model_obj->lite_model_path);
    TfLiteInterpreterOptions* options = TfLiteInterpreterOptionsCreate();
    model_obj->interpreter = TfLiteInterpreterCreate(model_obj->lite_model, options);
    if (!(model_obj->interpreter)) fprintf(stderr, "ERROR: no interpreter created!\n");
    model_obj->tflite_state = kTfLiteOk;

    // Allocate tensors and get the pointer to the input tensor.
    TfLiteInterpreterAllocateTensors(model_obj->interpreter);
    model_obj->in00 = TfLiteInterpreterGetInputTensor(model_obj->interpreter, 0);
    model_obj->in01 = TfLiteInterpreterGetInputTensor(model_obj->interpreter, 1);
    // fprintf(stdout, "RUN_INFO: in00 size is %d\n", TfLiteTensorByteSize(model_obj->in00));
    // fprintf(stdout, "RUN_INFO: in01 size is %d\n", TfLiteTensorByteSize(model_obj->in01));

    return 0;
}


// working proc write to tf-model's buffer, actually NOT used now
int write_runtime_data(struct TFLite_Model_Obj* model_obj, float* qdata, int32_t* xdata, float* ydata, int Count){
    if (Count <= 0 || Count > PYSHM_ENTRY_NUM){
        fprintf(stderr, "ERROR: invalid entry Count: %d\n", Count);
        return Count;
    }
    char* shmstr = model_obj->pyshm;
    int64_t cur_indx = *(int64_t*)(shmstr + 8);
    // fprintf(stdout, "RUN INFO: the pyshm data cur_indx is %lld\n", cur_indx);
    int64_t new_indx = cur_indx + Count;
    *(int64_t*)(shmstr + 8) = new_indx;
    cur_indx %= PYSHM_ENTRY_NUM;
    new_indx %= PYSHM_ENTRY_NUM;
    if (new_indx > cur_indx){
        memcpy(shmstr + PYSHM_QDATA_OFF + C_nqdata*cur_indx, qdata, C_nqdata*Count);
        memcpy(shmstr + PYSHM_XDATA_OFF + 4*cur_indx, xdata, 4*Count);
        memcpy(shmstr + PYSHM_YDATA_OFF + 4*cur_indx, ydata, 4*Count);
    }
    else{
        int Count01 = PYSHM_ENTRY_NUM - cur_indx;
        memcpy(shmstr + PYSHM_QDATA_OFF + C_nqdata*cur_indx, qdata, C_nqdata*Count01);
        memcpy(shmstr + PYSHM_XDATA_OFF + 4*cur_indx, xdata, 4*Count01);
        memcpy(shmstr + PYSHM_YDATA_OFF + 4*cur_indx, ydata, 4*Count01);
        int Count02 = new_indx;
        memcpy(shmstr + PYSHM_QDATA_OFF, qdata, C_nqdata*Count02);
        memcpy(shmstr + PYSHM_XDATA_OFF, xdata, 4*Count02);
        memcpy(shmstr + PYSHM_YDATA_OFF, ydata, 4*Count02);
    }
    return Count;
}


// Note this should be proc-owned
struct conflux_aiachr{
    int pn;
    int proc_num;
    int* stats;
    struct caltime_nano* nano_timer;
    struct TFLite_Model_Obj* tflite_model;
    float* qdata_buf;
    int* xdata_buf;
    float* ydata_buf;
    char* databuf_ptr;
    int *databuf_counter;
    int *newest_flag;
    int version_flag;
    float* collectorbuf;
};


struct conflux_aiachr* init_conflux_aiachr(int pn, int const_procnum, int* stats, struct caltime_nano* nano_timer){
    struct conflux_aiachr* res = (struct conflux_aiachr*)malloc(sizeof(struct conflux_aiachr));
    res->pn = pn;
    res->proc_num = const_procnum;
    res->stats = stats;
    res->nano_timer = nano_timer;
    res->tflite_model = init_tflite_model(TFLITE_FILE);

    char *owndata_shmname = (char*)malloc(64);
    memset(owndata_shmname, 0, 64);
    sprintf(owndata_shmname, "%s%d", PROCOWN_DATA_PRE, res->pn);
    int owndata_shmfd = shm_open(owndata_shmname, O_RDWR | O_CREAT, 0666);
    ftruncate(owndata_shmfd, PYSHM_LENT);
    res->databuf_ptr = (char*)mmap(NULL, PYSHM_LENT, PROT_WRITE, MAP_SHARED, owndata_shmfd, 0);
    memset(res->databuf_ptr, 0, PYSHM_HEAD_LEN);

    res->qdata_buf = (float*)(res->databuf_ptr + PYSHM_QDATA_OFF);
    res->xdata_buf = (int*)(res->databuf_ptr + PYSHM_XDATA_OFF);
    res->ydata_buf = (float*)(res->databuf_ptr + PYSHM_YDATA_OFF);
    res->databuf_counter = (int*)(res->databuf_ptr + 8);
    res->newest_flag = (int*)(res->databuf_ptr + 16);
    res->version_flag = 0;
    res->collectorbuf = (float*)malloc(sizeof(float) * (DEVIO_CLASS * 4));
    memset(res->collectorbuf, 0, sizeof(float) * (DEVIO_CLASS * 4));
    
    close(owndata_shmfd);
    return res;
}


int free_conflux_aiachr(struct conflux_aiachr* aiagent){
    free_tflite_model(aiagent->tflite_model);

    char *owndata_shmname = (char*)malloc(64);
    memset(owndata_shmname, 0, 64);
    sprintf(owndata_shmname, "%s%d", PROCOWN_DATA_PRE, aiagent->pn);
    shm_unlink(owndata_shmname);

    free(aiagent);
    return 0;
}


uint64_t valiolevel[12] = {(1 << 9), (1 << 10), (1 << 11), (1 << 12), 
                            (1 << 13), (1 << 14), (1 << 15), (1 << 16),
                            (1 << 17), (1 << 18), (1 << 19), (1 << 20)};


// use this function when io_size is absolutely greater than 1, the io-level is 1 for 512B, 2 for 1KB...
static inline int upper_iolevel(uint64_t io_size){
    return (64 - __builtin_clzl(io_size - 1) > 9 ? 56 - __builtin_clzl(io_size - 1) : 1);
}


float* collect_qdata(struct conflux_aiachr* aiagent){
    int i;
    int level;
    float* cur_qdata = &(aiagent->qdata_buf[*(aiagent->databuf_counter)]);

    float* collector = aiagent->collectorbuf;
    memset(collector, 0, sizeof(float) * (DEVIO_CLASS*4));
    for (i = 0; i < aiagent->proc_num; i++){
        // parse nvmm read
        level = aiagent->stats[aiagent->proc_num * 2 + i];
        if (level > 0) collector[level - 1] += 1.0;
        // parse nvmm write
        level = aiagent->stats[aiagent->proc_num * 3 + i];
        if (level > 0) collector[DEVIO_CLASS + level - 1] += 1.0;
        // parse rdma read
        level = aiagent->stats[aiagent->proc_num * 4 + i];
        if (level > 0) collector[2 * DEVIO_CLASS + level - 1] += 1.0;
        // parse rdma write
        level = aiagent->stats[aiagent->proc_num * 5 + i];
        if (level > 0) collector[3 * DEVIO_CLASS + level - 1] += 1.0;
    }
    memcpy(cur_qdata, collector, sizeof(float) * (DEVIO_CLASS*4));

    return collector;
}


int pure_random_decs(float bound){
    if (!stdrand_init) stdrand_initflag = stdrand_init();
    float r = (float)rand() / (float)RAND_MAX;
    if (r >= bound) return APPLY_NVMMIO;
    else return APPLY_RDMAIO;
}


int decideby_aiagent(struct conflux_aiachr* aiagent, int iolevel){
    if (iolevel < admis_thres) return APPLY_NVMMIO;

    float* qdata = collect_qdata(aiagent);
    int32_t x1 = iolevel - 1; // query nvm io latency
    float nvmm_lat_pre = predict_latency(aiagent->tflite_model, qdata, &x1);
    int32_t x2 = DEVIO_CLASS + iolevel - 1; // query rdma io latency
    float rdma_lat_pre = predict_latency(aiagent->tflite_model, qdata, &x2);

    float pos = nvmm_lat_pre / (nvmm_lat_pre + rdma_lat_pre);
    float tar = pos;
    int i;
    for (i = 0; i < approx_stage_int; ++i) tar = split_curve(tar);
    float top_tar = split_curve(tar);
    float r = tar * (1 - approx_stage_res) + top_tar * approx_stage_res;

    return pure_random_decs(r);
}


#endif 
