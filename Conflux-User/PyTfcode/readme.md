
# Guides for Tensorflow

The machine learning model is trained by a background process in Conflux.
We implement it using a Python module.

### **Install Conda**

Conda is an open-source package management system and environment management system for Python.
We can set up a totally separate environment to run that different version of Python than the normal linux pre-enabled version.
See the official documents [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html) for installation guides.

### **Install tensorflow in Conda**

With conda successfully installed, we use following commands to install tensorflow (cpu version):   
`
conda create -n tf2 tensorflow
`   
`
conda activate tf2
`   
Note that the name of conda environment should be consistent with the test scripts in *./Conflux-Scripts*.

### **Run the training process**

Within the activated conda environment, simply using `python` to start up the *Latency_InferModel* process.
