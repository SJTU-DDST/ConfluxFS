
# Conflux: DFS that exploits both local PM and RDMA bandwidth

## Brief Introduction

Persistent Memory (PM) and Remote Direct Memory Access (RDMA) technologies have significantly improved the storage and network performance in data centers and spawned a slew of distributed file systems (DFS) designs.
Existing DFSs often consider remote storage a performance constraint, assuming it delivers lower bandwidth and higher latency than local storage.
However, the advances in RDMA technology provide an opportunity to bridge the performance gap between local and remote access, enabling DFSs to leverage both local and remote PM bandwidth and achieve higher overall throughput.


We propose Conflux, a new DFS architecture that leverages the aggregated bandwidth of PM and RDMA networks.
Conflux dynamically steers I/O requests to local and remote PM to fully utilize PM and RDMA bandwidth under heavy workloads.
To decide the I/O destination adaptively, we propose SEIOD, a learning-based policy engine predicting Conflux I/O latency and making decisions in real-time.
Furthermore, Conflux adopts a lightweight concurrency control approach to improve its scalability.

## Installation of Conflux

Here we present the installation process of Conflux.

### **Hardware and driver**

We need a cluster (>=2) of machines with real persistent memory and RDMA network interface card available.

As far as I know, the only commercially available PM device is Intel(R) Optane NVDIMM.  
We use *ipmctl* (see Documents [here](https://docs.pmem.io/ipmctl-user-guide/)) to provision Optane PM as **app direct** mode, with the command:    
`
ipmctl create -goal PersistentMemoryType=AppDirect
`   
We use *ndctl* (see Documents [here](https://docs.pmem.io/ndctl-user-guide/)) to create partitioned PM namespace.
Specifically, we need **devdax** mode for Conflux shared pool and client cache. For example:  
`
ndctl create-namespace -t pmem -m devdax -s 128G
`   
In case someone want to run Conflux without real PM devices, we recommand QEMU (documents [here](https://www.qemu.org/docs/master/system/devices/virtio-pmem.html)) for emulation.
Or for the most shabby experimental environment, a shared memory file can be used (with no guarentee of meaningful results) as memory pool.

For RDMA NIC, we use Mellanox ConnectX-6 Infiniband NIC.    
The Infiniband driver installation should follow the MLNX_OFED documents (see [here](https://docs.nvidia.com/networking/display/MLNXOFEDv543100/Installation)).
Note that if we need to test NFS over RDMA, the installation of MLNX_OFED should be coupled with *--with-nfsrdma* option.
Successful installation of RDMA NIC driver can be verified through `ibstatus` command.  
To setup ip address of infiniband NICs, use `ifconfig` with root privilege.
To check the inter-node RDMA latency and bandwidth, use `ib_write_lat` and `ib_write_bw` tools.


### **Software prerequisites**

There are three software utlilities need by Conflux:
- PMDK, for low-level PM operation
- Syscall_Intercept, for user-level system call interception
- Tensorflow lite, for lightweight neural network model API in C code

For PMDK installation, see the official documents [here](https://docs.pmem.io/persistent-memory/getting-started-guide/installing-pmdk).

For Syscall_Intercept, there is a github project within Intel Perssitent Memory Programming Group (see github link [here](https://github.com/pmem/syscall_intercept)).

For Tensorflow lite, we use the compile and installation guides from tensorflow official documents [here](https://www.tensorflow.org/lite/guide/build_cmake). 
Moreover, we recommand researchers with little experience in using tensorflow to see examples in the [homepage](https://www.tensorflow.org/lite/api_docs) and in the github [repo](https://github.com/tensorflow/tensorflow/tree/master/tensorflow/lite).


### **Build Conflux**

With all the hardware and software requirements satisfied, the build process of Conflux is straight-forward.

For Conflux server, go to *./Conflux-Center* and run:    
`
make backgr && make owner
`

For Conflux client, go to *./Conflux-User* and run:    
`
make aiagent && make conflux_call
`

Moreover, we need a background process running neural network training. See *./Conflux-User/PyTfcode* for details.


### **Installtion of other DFS** for comparison

For Octopus, see the github repo [here](https://github.com/thustorage/octopus).

For Assise, see the github repo [here](https://github.com/ut-osa/assise).

For NFS over RDMA, see Chapter 6 of this NFS white paper ([here](https://www.delltechnologies.com/asset/en-us/products/storage/industry-market/h17240_wp_isilon_onefs_nfs_design_considerations_bp.pdf)) for details.



## Experiments on Conflux

Here we introduce the experiments we have conducted on Conflux, inclusing Fio benchmarks and Filebench benchmarks information.

### **Cluster configuration**

The current implementation of Conflux need static cluster configuration, i.e., fixed number of server/client nodes and their ip address list.
See the *./Conflux-Scripts/.conflux_config* file contents for example, and corresponding changes may be needed in that file for test on other cluster.

### **Benchmarks setup**

The first benchmark we use is Fio, a flexible I/O tester.
One can see the document [page](https://fio.readthedocs.io/en/latest/fio_doc.html) for details about this benchmark, and the github [repo](https://github.com/axboe/fio) for installation guides.

Use    
`
fio --version
`   
to check installation of Fio.
The version we use for experiments is 3.28-47.

The second benchmark tool we use is Filebench, a flexible framework for file system benchmarking.
One can see the [paper](https://www.usenix.org/system/files/login/articles/login_spring16_02_tarasov.pdf) for details about Filebench, and the github [repo](https://github.com/filebench/filebench) for installation guides.

Use    
`
filebench -h
`   
to check installation of Filebench.
The version we use for experiments is 1.5-alpha3.

### **Scripts for experiments**

We have provided an example bash script for Conflux test in *./Conflux-Scripts/AlloverTest.sh*.
It acts as a contoller from outside the Conflux cluster (i.e., not in the server and client nodes). 
Note that the Username and IP address should be correctly settled in that script.   
Moreover, the CPU mask flag (for `taskset`) used in the script should be consistent with the PM and RDMA numa node. 
