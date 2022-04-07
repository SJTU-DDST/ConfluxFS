
from operator import le
from time import sleep
import numpy as np
import tensorflow as tf
from tensorflow import keras
# import keras
from tensorflow.keras import layers as krsLayers
# from keras import layers as krsLayers
import mmap
import os
import struct
import time


tf.config.threading.set_inter_op_parallelism_threads(2) 
tf.config.threading.set_intra_op_parallelism_threads(4)


IO_CLASS = 12
raw_nvmm_rdma_band = [6.0,6.0,6.0,6.0,6.0,6.0,6.0,6.0,6.0,6.0,6.0,6.0,
                    5.0,5.0,5.0,5.0,5.0,5.0,5.0,5.0,5.0,5.0,5.0,5.0]
raw_nvmm_rdma_over = [0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,
                    3.8,3.8,3.8,3.8,3.8,3.8,3.8,3.8,3.8,3.8,3.8,3.8]
discrete_iokb_list = [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 
            0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0]
SHM_NAME = "/dev/shm/QZLSHM_pyscope"
SHM_DIR = "/dev/shm/"
PERPROC_SHMNAME_PRE = "QZLSHM_PROCOWN_DATA"
CURRENT_WORKDIR = "Path-to-Conflux/QDcache-User/PyTfcode/"


SHM_HEAD_LEN = 64
SHM_ENTRY_NUM = 100000
SHM_DATA_LEN = 4 * (IO_CLASS * 4 + 2)
SHM_QDATA_OFF = 64
SHM_XDATA_OFF = 64 + 4*(IO_CLASS * 4)*100000
SHM_YDATA_OFF = SHM_XDATA_OFF + 4*100000
# The shm structure is designed as below:
# First 64 B is header, where first byte indicating status flag, 4-8 bytes records the (int32)version,
# and 8-16 bytes maintain (int64)current_data_entry_index, 24-32 bytes keeps (int32)newdata_beg_index.
# Next SHM_ENTRY_NUM * SHM_DATA_LEN space store the data being added to records.


MyModel_File_Version = 0
Train_Batch_Unit = 256
InHandData_low = 1024
InHandData_high = 2048
Past_Data_High = 32768
Past_Data_Valid = 0
Past_QDATA_NpArr = None
Past_XDATA_NpArr = None
Past_YDATA_NpArr = None


class MyfcLayer(krsLayers.Layer):

    def __init__(self, out_dim, in_dim, w_val=0, b_val=0):
        super(MyfcLayer, self).__init__()
        w_init = tf.constant_initializer(w_val)
        b_init = tf.constant_initializer(b_val)
        self.w = tf.Variable(
            initial_value=w_init(shape=(in_dim, out_dim), dtype="float32"),
            trainable=True
        )
        self.b = tf.Variable(
            initial_value=b_init(shape=(out_dim, ), dtype="float32"),
            trainable=True
        )
    
    def call(self, inputs):
        # mid = tf.matmul(inputs, self.w) + self.b
        # outputs = tf.math.sigmoid(tf.matmul(inputs, self.w) + self.b)
        outputs = tf.nn.leaky_relu(tf.matmul(inputs, self.w) + self.b)
        return outputs


class MytailLayer(krsLayers.Layer):

    def __init__(self, in_dim):
        super(MytailLayer, self).__init__()
        self.upval = tf.constant(discrete_iokb_list, dtype="float32")
        scale_init = tf.constant_initializer(raw_nvmm_rdma_band)
        self.scale_rat = tf.Variable(
            initial_value=scale_init(shape=(in_dim, ), dtype="float32"),
            trainable=True
        )
        # self.scale_rat = tf.constant(raw_nvmm_rdma_band, dtype="float32")
        add_init = tf.constant_initializer(raw_nvmm_rdma_over)
        self.addval = tf.Variable(
            initial_value=add_init(shape=(in_dim, ), dtype="float32"),
            trainable=True
        )
        self.in_dim = in_dim
    
    def call(self, input_list):
        inputs, io_idx = input_list
        # bw = tf.math.multiply(self.scale_rat, inputs)
        # t1 = tf.math.divide(self.upval, tf.math.multiply(self.scale_rat, inputs))
        in_one = tf.math.sigmoid(inputs)
        t2 = tf.math.add(tf.math.divide(self.upval, tf.math.multiply(self.scale_rat, in_one)), self.addval)
        mask = tf.one_hot(io_idx, self.in_dim)
        outputs = tf.math.reduce_sum(tf.multiply(mask, t2), 1)
        return outputs


class MyTimePredictor(tf.keras.Model):

    def __init__(self):
        super(MyTimePredictor, self).__init__()
        self.fc1 = MyfcLayer(IO_CLASS*10, IO_CLASS*4, 0, 0)
        self.fc2 = MyfcLayer(IO_CLASS*2, IO_CLASS*10, 0, 0)
        self.outl = MytailLayer(IO_CLASS*2)
    
    def call(self, input_list):
        inputs, idx = input_list
        y1 = self.fc1(inputs)
        y2 = self.fc2(y1)
        y3 = self.outl([y2, idx])
        return y3


def InitModel_AndSave(shmstr, datas, epochx=10):
    In_Layer1 = tf.keras.Input(shape=(IO_CLASS*4,), dtype="float32")
    In_Layer2 = tf.keras.Input(shape=(), dtype="int32")
    netw = MyTimePredictor()([In_Layer1, In_Layer2])
    mymodel = tf.keras.Model([In_Layer1, In_Layer2], netw)
    optim = keras.optimizers.Adam(learning_rate=0.007)
    mymodel.compile(optimizer=optim, loss="MSLE")

    if datas == None:
        mymodel.save(CURRENT_WORKDIR+"model", save_format="tf")
        # convert to tf-lite model
        # converter = tf.lite.TFLiteConverter.from_saved_model("model")
        converter = tf.lite.TFLiteConverter.from_keras_model(mymodel)
        print("??? TEST ???")
        tflite_model = converter.convert()
        print("??? TEST ???")
        with open(CURRENT_WORKDIR+'model.tflite', 'wb') as f:
            f.write(tflite_model)
        shmstr[0] = 0
        zero_byte = struct.pack("i", 0)
        shmstr[4:8] = zero_byte
        curpt_byte = struct.pack("q", SHM_ENTRY_NUM + 7)
        shmstr[8:16] = curpt_byte
        begpt_byte = struct.pack("q", SHM_ENTRY_NUM + 7)
        shmstr[24:32] = begpt_byte
        return mymodel
    
    input_data, input_tar, y_true = datas
    print(mymodel([input_data, input_tar]))
    mymodel.fit([input_data, input_tar], y_true, epochs=epochx, verbose=0)
    print(mymodel([input_data, input_tar]))
    
    mymodel.save(CURRENT_WORKDIR+"model", save_format="tf")
    # convert to tf-lite model
    # converter = tf.lite.TFLiteConverter.from_saved_model("model")
    converter = tf.lite.TFLiteConverter.from_keras_model(mymodel)
    tflite_model = converter.convert()
    with open(CURRENT_WORKDIR+'model.tflite', 'wb') as f:
        f.write(tflite_model)
    shmstr[0] = 0
    zero_byte = struct.pack("i", 0)
    shmstr[4:8] = zero_byte
    curpt_byte = struct.pack("q", SHM_ENTRY_NUM + 7)
    shmstr[8:16] = curpt_byte
    begpt_byte = struct.pack("q", SHM_ENTRY_NUM + 7)
    shmstr[24:32] = begpt_byte
    return mymodel


def LoadModel_AndSave(shmstr, datas, epochx=10):
    mymodel:MyTimePredictor = keras.models.load_model("model")
    if datas == None:
        mymodel.save(CURRENT_WORKDIR+"model", save_format="tf")
        # convert to tf-lite model
        # converter = tf.lite.TFLiteConverter.from_saved_model("model")
        converter = tf.lite.TFLiteConverter.from_keras_model(mymodel)
        tflite_model = converter.convert()
        with open(CURRENT_WORKDIR+'model.tflite', 'wb') as f:
            f.write(tflite_model)
        shmstr[0] = 0
        zero_byte = struct.pack("i", 0)
        shmstr[4:8] = zero_byte
        curpt_byte = struct.pack("q", SHM_ENTRY_NUM + 7)
        shmstr[8:16] = curpt_byte
        begpt_byte = struct.pack("q", SHM_ENTRY_NUM + 7)
        shmstr[24:32] = begpt_byte
        return mymodel
    input_data, input_tar, y_true = datas
    print(mymodel([input_data, input_tar]))
    mymodel.fit([input_data, input_tar], y_true, epochs=epochx, verbose=0)
    print(mymodel([input_data, input_tar]))
    
    mymodel.save(CURRENT_WORKDIR+"model", save_format="tf")
    # convert to tf-lite model
    # converter = tf.lite.TFLiteConverter.from_saved_model("model")
    converter = tf.lite.TFLiteConverter.from_keras_model(mymodel)
    tflite_model = converter.convert()
    with open(CURRENT_WORKDIR+'model.tflite', 'wb') as f:
        f.write(tflite_model)

    shmstr[0] = 0
    zero_byte = struct.pack("i", 0)
    shmstr[4:8] = zero_byte
    curpt_byte = struct.pack("q", SHM_ENTRY_NUM + 7)
    shmstr[8:16] = curpt_byte
    begpt_byte = struct.pack("q", SHM_ENTRY_NUM + 7)
    shmstr[24:32] = begpt_byte
    return mymodel


def push_inhand_data_topool(qdata:np.ndarray, xdata:np.ndarray, ydata:np.ndarray):
    global Past_Data_Valid
    global Past_QDATA_NpArr
    global Past_XDATA_NpArr
    global Past_YDATA_NpArr
    if Past_Data_Valid == 0:
        Past_QDATA_NpArr = qdata
        Past_XDATA_NpArr = xdata
        Past_YDATA_NpArr = ydata
        Past_Data_Valid = 1
    else:
        Past_QDATA_NpArr = np.concatenate((Past_QDATA_NpArr, qdata), axis=0)
        Past_XDATA_NpArr = np.concatenate((Past_XDATA_NpArr, xdata), axis=None)
        Past_YDATA_NpArr = np.concatenate((Past_YDATA_NpArr, ydata), axis=None)
    m = Past_QDATA_NpArr.shape[0] - Past_Data_High
    if m > 0:
        Past_QDATA_NpArr = Past_QDATA_NpArr[m:,:]
        Past_XDATA_NpArr = Past_XDATA_NpArr[m:]
        Past_YDATA_NpArr = Past_YDATA_NpArr[m:]
    return 0


def balance_dataxdistri(source_data):
    _, source_x, _ = source_data
    source_L = source_x.shape[0]
    nvmm_cnt = np.count_nonzero(source_x < IO_CLASS)
    rdma_cnt = source_L - nvmm_cnt
    print("In this data batch, nvmm: {}, rdma: {}".format(nvmm_cnt, rdma_cnt))
    sample_weight_arr = np.ones(source_x.shape)
    sample_weight_plus = np.zeros(source_x.shape)
    print(sample_weight_arr.shape)
    if source_L > InHandData_high:
        sample_weight_plus[-InHandData_high:] += 0.9
    else:
        sample_weight_plus[:] += 0.9
    if (rdma_cnt == 0):
        sample_weight_arr += sample_weight_plus
        sample_weight_arr /= np.average(sample_weight_arr)
        return sample_weight_arr
    ratio = (nvmm_cnt / rdma_cnt) * 0.95
    if ratio < 1:
        ratio = 1
    sample_weight_plus[source_x >= IO_CLASS] *= ratio 
    sample_weight_arr += sample_weight_plus
    sample_weight_arr /= np.average(sample_weight_arr)
    return sample_weight_arr


# Note that there is a use-old-data method in this function, 
# which can be replaced by some more complicated selection 
def TrainModel_AndSave(shmstr, mymodel:MyTimePredictor, datas, epochx=1000):
    global MyModel_File_Version
    input_data, input_tar, y_true = datas
    # shmstr[0] = 2 # we do not use shm[0] to inform retrain anymore
    sample_out = mymodel([input_data, input_tar])
    print("   sample output before train: ", sample_out[-50:])
    print("   in that, max: {}, min: {}".format(np.max(sample_out), np.min(sample_out)))
    loss_log = []

    init_loss_val = mymodel.evaluate([input_data, input_tar], y_true, batch_size=Train_Batch_Unit, verbose=0)
    loss_log.append(init_loss_val)

    push_inhand_data_topool(input_data, input_tar, y_true)
    cur_sample_w = balance_dataxdistri([Past_QDATA_NpArr, Past_XDATA_NpArr, Past_YDATA_NpArr])
    for i in range(epochx):
        history = mymodel.fit([Past_QDATA_NpArr, Past_XDATA_NpArr], Past_YDATA_NpArr,
                            sample_weight=cur_sample_w, batch_size=Train_Batch_Unit, epochs=4, verbose=0)
        loss_log.append(np.average(history.history["loss"]))
        print("During Epochx ", i, ", train loss is ", loss_log[-1])
        if (i < 2):
            continue
        if loss_log[-1] <= 0.009:
            break
        if (loss_log[-1] / loss_log[-2] > 0.98):
            break
    
    end_loss_val = mymodel.evaluate([input_data, input_tar], y_true, batch_size=Train_Batch_Unit, verbose=0)
    loss_log.append(end_loss_val)

    print("loss history: ", loss_log)
    sample_out = mymodel([input_data, input_tar])
    print("   sample output after train: ", sample_out[-50:])
    print("   in that, max: {}, min: {}".format(np.max(sample_out), np.min(sample_out)))

    if (loss_log[-1] < loss_log[0]):
        mymodel.save(CURRENT_WORKDIR+"model", save_format="tf")
        # convert to tf-lite model
        # converter = tf.lite.TFLiteConverter.from_saved_model("model")
        converter = tf.lite.TFLiteConverter.from_keras_model(mymodel)
        tflite_model = converter.convert()
        MyModel_File_Version += 1
        new_model_name = CURRENT_WORKDIR+"model.tflite.{}".format(MyModel_File_Version)
        with open(new_model_name, 'wb') as f:
            f.write(tflite_model)

    # shmstr[0] = 0 # we do not use shm[0] to inform retrain anymore
    version = struct.unpack("i", shmstr[4:8])[0]
    version += 1
    ver_byte = struct.pack("i", version)
    shmstr[4:8] = ver_byte


def create_shm(path_name, shm_len):
    fd = os.open(path_name, flags=os.O_RDWR|os.O_CREAT, mode=0o660)
    shm_file = os.fdopen(fd, mode="w")
    shm_file.truncate(shm_len)
    shm_obj = mmap.mmap(shm_file.fileno(), length=shm_len, access=mmap.ACCESS_WRITE)
    bs = struct.pack("fiii", 1.1, 81, 90, 76)
    shm_obj[0:64] = bs * 4
    val = struct.unpack("f", shm_obj[0:4])[0]
    print(val)
    os.close(fd)
    return shm_obj


def check_allagent_shmdata(shmstr, mymodel:MyTimePredictor, 
                            qdata_list_inhand:list, xdata_list_inhand:list, ydata_list_inhand:list):
    global MyModel_File_Version
    procshm_namelist = []
    for file in os.listdir(SHM_DIR):
        try:
            if file.startswith(PERPROC_SHMNAME_PRE):
                procshm_namelist.append(file)
        except:
            continue
    zero_int32 = struct.pack("i", 0)
    select_namelist = []
    if len(procshm_namelist) > 18:
        np.random.shuffle(procshm_namelist)
        select_namelist = procshm_namelist[0:18]
    else:
        np.random.shuffle(procshm_namelist)
        select_namelist = procshm_namelist
    for name in select_namelist:
        if shmstr[0] == 3:
            break
        shm_name_cur = SHM_DIR + name
        shm_obj = None
        try:
            fd = os.open(shm_name_cur, flags=os.O_RDWR, mode=0o660)
            shm_file = os.fdopen(fd, mode="w")
            shm_obj = mmap.mmap(shm_file.fileno(), length=SHM_HEAD_LEN + SHM_DATA_LEN * SHM_ENTRY_NUM, access=mmap.ACCESS_WRITE)
        except:
            continue
        cur_inx = struct.unpack("i", shm_obj[8:12])[0]
        if (cur_inx < 100):
            continue
        qdata_list = struct.unpack("{}f".format(cur_inx * IO_CLASS*4), shm_obj[SHM_QDATA_OFF:SHM_QDATA_OFF+192*cur_inx])
        xdata_list = struct.unpack("{}i".format(cur_inx), shm_obj[SHM_XDATA_OFF:SHM_XDATA_OFF+4*cur_inx])
        ydata_list = struct.unpack("{}f".format(cur_inx), shm_obj[SHM_YDATA_OFF:SHM_YDATA_OFF+4*cur_inx])
        # print(cur_inx, qdata_list[-1], xdata_list[-1],ydata_list[-1])
        shm_obj[8:12] = zero_int32
        qdata_list_inhand.extend(qdata_list)
        xdata_list_inhand.extend(xdata_list)
        ydata_list_inhand.extend(ydata_list)
    inhand_datanum = len(xdata_list_inhand)
    if shmstr[0] == 3 or inhand_datanum < InHandData_low:
        return
    if inhand_datanum >= InHandData_low:
        willuse_datanum = inhand_datanum - (inhand_datanum % Train_Batch_Unit)
        if willuse_datanum > InHandData_high:
            willuse_datanum = InHandData_high
        q = np.asarray(qdata_list_inhand[-(willuse_datanum*IO_CLASS*4):]).reshape([willuse_datanum, IO_CLASS*4])
        x = np.asarray(xdata_list_inhand[-willuse_datanum:])
        y = np.asarray(ydata_list_inhand[-willuse_datanum:])
        data_batch = [q, x, y]
        TrainModel_AndSave(shmstr, mymodel, data_batch)
        qdata_list_inhand.clear()
        xdata_list_inhand.clear()
        ydata_list_inhand.clear()
    for name in procshm_namelist:
        if shmstr[0] == 3:
            break
        shm_name_cur = SHM_DIR + name
        shm_obj = None
        try:
            fd = os.open(shm_name_cur, flags=os.O_RDWR, mode=0o660)
            shm_file = os.fdopen(fd, mode="w")
            shm_obj = mmap.mmap(shm_file.fileno(), length=SHM_HEAD_LEN + SHM_DATA_LEN * SHM_ENTRY_NUM, access=mmap.ACCESS_WRITE)
        except:
            continue
        newest_flag = struct.unpack("i", shm_obj[16:20])[0]
        if newest_flag > MyModel_File_Version:
            print("\n\n\n  Error: retrain_and get lower model version.  \n\n\n")
        if newest_flag < MyModel_File_Version:
            ver_int32 = struct.pack("i", MyModel_File_Version)
            shm_obj[16:20] = ver_int32
        # newest_flag == MyModel_File_Version
    return 


# Value of ShmStr[0] show the model state,
# 0: Can be load
# 1: Need to re-train
# 2: Training / Saving
# 3: Ready to exit
def TrackState_TrainModel(shmstr, mymodel):
    qdata_list_inhand = []
    xdata_list_inhand = []
    ydata_list_inhand = []
    while True:
        s = shmstr[0]
        if s == 3:
            print("RUN_INFO: have got a exit signal from C-shm.")
            time.sleep(1.5)
            shmstr[0] = 4
            print("RUN_INFO: have sent exit signal ack to C-shm.")
            time.sleep(1.5)
            break
        elif s == 1:
            # conflux aiagent actively require retrain, NOT used currently  
            cur_inx = struct.unpack("q", shmstr[8:16])[0]
            beg_inx = struct.unpack("q", shmstr[24:32])[0]
            item_cnt = cur_inx - beg_inx
            cur_inx %= SHM_ENTRY_NUM
            beg_inx %= SHM_ENTRY_NUM
            if item_cnt >= SHM_ENTRY_NUM:
                beg_inx = 0
                cur_inx = SHM_ENTRY_NUM
                item_cnt = SHM_ENTRY_NUM
            qdata_list = struct.unpack("{}f".format(item_cnt * IO_CLASS*4), shmstr[SHM_QDATA_OFF+192*beg_inx:SHM_QDATA_OFF+192*cur_inx])
            xdata_list = struct.unpack("{}i".format(item_cnt), shmstr[SHM_XDATA_OFF+ 4*beg_inx:SHM_XDATA_OFF+ 4*cur_inx])
            ydata_list = struct.unpack("{}f".format(item_cnt), shmstr[SHM_YDATA_OFF+ 4*beg_inx:SHM_YDATA_OFF+ 4*cur_inx])
            q = np.asarray(qdata_list).reshape([item_cnt, IO_CLASS*4])
            x = np.asarray(xdata_list)
            y = np.asarray(ydata_list)
            print(q, x, y)
            data_batch = [q, x, y]
            TrainModel_AndSave(shmstr, mymodel, data_batch)
        elif s == 0:
            check_allagent_shmdata(shmstr, mymodel, qdata_list_inhand, xdata_list_inhand, ydata_list_inhand)
    return 0


if __name__=="__main__":
    print(SHM_DIR + PERPROC_SHMNAME_PRE)
    shmstr = create_shm(SHM_NAME, SHM_HEAD_LEN + SHM_DATA_LEN * SHM_ENTRY_NUM)

    init_method = "fromzero"
    # init_method = "onreload"

    if init_method == "fromzero":
        mymodel = InitModel_AndSave(shmstr, None)
    if init_method == "onreload":
        mymodel = LoadModel_AndSave(shmstr, None)

    TrackState_TrainModel(shmstr, mymodel)
    
    os.remove(SHM_NAME)
    os.system("rm model.tf*")
