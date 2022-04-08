#ifndef __CONFLUX_AIAGENT_C
#define __CONFLUX_AIAGENT_C


#include "conflux_AIagent.h"
#include "dcache_timer.c"


const char* saved_model_dir = "./PyTfcode/model/";
const char* lite_model_path = "./PyTfcode/model.tflite";


int test_origin_TFCapi(){
    float fake_qdata[] = {1,1,1,1,1,1,1,1,
                        1,1,1,1,1,1,1,1,
                        1,1,1,1,1,1,1,1,
                        1,1,1,1,1,1,1,1,
                        1,1,1,1,1,1,1,1,
                        1,1,1,1,1,1,1,1};
    int32_t fake_xdata = 9;

    struct TFkeras_Model* model = init_pytfmodel(saved_model_dir);
    reload_model(model);
    float r1 = inference_with(model, fake_qdata, fake_xdata);

    trigger_retrain(model);
    reload_model(model);
    float r2 = inference_with(model, fake_qdata, fake_xdata);

    struct caltime_nano* nano_timer = init_caltime_nano();
    start_caltime_nano(nano_timer);
    int i;
    for (int i = 0; i < 100; i++){
        float fake_qdata[] = {1,1,1,1,1,1,1,1,
                        1,1,1,1,1,1,1,1,
                        1,1,1,1,1,1,1,1,
                        1,1,1,1,1,1,1,1,
                        1,1,1,1,1,1,1,1,
                        1,1,1,1,1,1,1,1};
        int32_t fake_xdata = 3 * (i % 3) + 3;
        // reload_model(model);
        float r = inference_with(model, fake_qdata, fake_xdata);
    }
    uint64_t t_interval = stop_caltime_nano(nano_timer);
    fprintf(stdout, "\n\nuse %llu.%09llu sec\n\n", t_interval/1000000000, t_interval%1000000000);

    destroy_pytfmodel(model);
}


int main(){

    struct TFLite_Model_Obj* tflite_model = init_tflite_model(lite_model_path);
    TfLiteStatus st;
    struct caltime_nano* nano_timer = init_caltime_nano();
    int i;
    uint64_t t_interval;

    // Predict on some fake data
    start_caltime_nano(nano_timer);
    for (i = 0; i < 10; i++){
        // These are Input
        float fake_qdata[] = {12,11,10,9,8,7,6,5,4,3,2,1,
                            12,11,10,9,8,7,6,5,4,3,2,1};
        int32_t fake_xdata = 3 + (i % 3) * 3;
        float res = predict_latency(tflite_model, fake_qdata, &fake_xdata);
        fprintf(stdout, "RUN_INFO: result is %f\n", res);
    }
    t_interval = stop_caltime_nano(nano_timer);
    fprintf(stdout, "\nuse %llu.%09llu sec\n\n", t_interval/1000000000, t_interval%1000000000);

    // Generate and write fake runtime data to shm
    float* fake_qdata = (float*)malloc(sizeof(float)*(DEVIO_CLASS * 2)*9);
    int32_t* fake_xdata = (int32_t*)malloc(sizeof(int32_t)*1*9);
    float* fake_ydata = (float*)malloc(sizeof(float)*1*9);
    for (i = 0; i < (DEVIO_CLASS * 2)*9; ++i) fake_qdata[i] = (float)(12 - i % 12);
    for (i = 0; i < 9; ++i) fake_xdata[i] = (int32_t)(3 + (i % 3) * 3);
    float rand_y[] = {10,30,200};
    for (i = 0; i < 9; ++i) fake_ydata[i] = rand_y[i%3];
    write_runtime_data(tflite_model, fake_qdata, fake_xdata, fake_ydata, 9);

    // Test the retrain time overhead
    start_caltime_nano(nano_timer);
    trigger_tflite_retrain(tflite_model);
    reload_tflite_model(tflite_model, 0);
    t_interval = stop_caltime_nano(nano_timer);
    fprintf(stdout, "\nuse %llu.%09llu sec\n\n", t_interval/1000000000, t_interval%1000000000);

    // Predict on those fake data again
    start_caltime_nano(nano_timer);
    for (i = 0; i < 10; i++){
        // These are Input
        float fake_qdata[] = {12,11,10,9,8,7,6,5,4,3,2,1,
                            12,11,10,9,8,7,6,5,4,3,2,1};
        int32_t fake_xdata = 3 + (i % 3) * 3;
        float res = predict_latency(tflite_model, fake_qdata, &fake_xdata);
        fprintf(stdout, "RUN_INFO: result is %f\n", res);
    }
    t_interval = stop_caltime_nano(nano_timer);
    fprintf(stdout, "\nuse %llu.%09llu sec\n\n", t_interval/1000000000, t_interval%1000000000);

    free_tflite_model(tflite_model);
    return 0;
}


#endif 
