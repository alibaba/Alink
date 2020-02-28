<font size=7>English| [简体中文](README.md)</font>

# Alink

Alink is the Machine Learning algorithm platform based on Flink, developed by the PAI team of Alibaba computing platform.
Welcome everyone to join the Alink open source user group to communicate.
 
 
<div align=center>
<img src="https://img.alicdn.com/tfs/TB1kQU0sQY2gK0jSZFgXXc5OFXa-614-554.png" height="25%" width="25%">
</div>

#### List of Algorithms

<div align=center>
<img src="https://img.alicdn.com/tfs/TB1AEOeoBr0gK0jSZFnXXbRRXXa-1320-1048.png" height="60%" width="60%">
</div>

#### PyAlink

<div align=center>
<img src="https://img.alicdn.com/tfs/TB1TmKloAL0gK0jSZFxXXXWHVXa-2070-1380.png" height="60%" width="60%">
</div>

# Quick start - PyAlink Manual

Preparation before use:
---------


About package names and versions:
  - PyAlink provides different Python packages for Flink versions that Alink supports: 
  package `pyalink` always maintains Alink Python API against the latest Flink version, which is 1.10, 
  while `pyalink-flink-***` support old-version Flink, which are `pyalink-flink-1.9` for now. 
  - The version of python packages always follows Alink Java version, like `1.1.0`.
  
Installation steps:

1. Make sure the version of python3 on your computer is 3.6 or 3.7.
2. Make sure Java 8 is installed on your computer.
3. Use pip to install:
  `pip install pyalink` or `pip install pyalink-flink-1.9` （Note: for now, `pyalink-flink-1.9` is not available，use following links instead).


Potential issues:

1. `pyalink` and/or `pyalink-flink-***` can not be installed at the same time. Multiple versions are not allowed.
If `pyalink` or `pyalink-flink-***` was/were installed, please use `pip uninstall pyalink` or `pip uninstall pyalink-flink-***` to remove them.

2. If `pip install` is slow of failed, refer to [this article](https://segmentfault.com/a/1190000006111096) to change the pip source, or use the following download links:
   - Flink 1.10：[Link 1](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.1.0/pyalink-1.1.0-py3-none-any.whl) [Link 2](https://github.com/alibaba/Alink/releases/download/v1.1.0/pyalink-1.1.0-py3-none-any.whl) (MD5: f92b6fcff0caea332f531f5d97cb00fe)
   - Flink 1.9: [Link 1](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.1.0/pyalink_flink_1.9-1.1.0-py3-none-any.whl) [Link 2](https://github.com/alibaba/Alink/releases/download/v1.1.0/pyalink_flink_1.9-1.1.0-py3-none-any.whl) (MD5: f2c8c32f0be6d9356c7f8ccdedf7238f)
3. If multiple version of Python exist, you may need to use a special version of `pip`, like `pip3`;
If Anaconda is used, the command should be run in Anaconda prompt. 

Start using: 
-------
We recommend using Jupyter Notebook to use PyAlink to provide a better experience.

Steps for usage: 

1. Start Jupyter: ```jupyter notebook``` in terminal
, and create Python 3 notebook.

2. Import the pyalink package: ```from pyalink.alink import *```.

3. Use this command to create a local runtime environment:

   ```useLocalEnv(parallism, flinkHome=None, config=None)```.

   Among them, the parameter  ```parallism```  indicates the degree of parallelism used for execution;```flinkHome``` is the full path of flink,and the default flink-1.9.0 path of PyAlink is used; ```config``` is the configuration parameter accepted by Flink. After running, the following output appears, indicating that the initialization of the running environment is successful.
```
JVM listening on ***
Python listening on ***
```
4. Start writing PyAlink code, for example:
```
source = CsvSourceBatchOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")
res = source.select(["sepal_length", "sepal_width"])
df = res.collectToDataframe()
print(df)
```

Write code: 
------
In PyAlink, the interface provided by the algorithm component is basically the same as the Java API, that is, an algorithm component is created through the default construction method, then the parameters are set through ```setXXX```, and other components are connected through ```link / linkTo / linkFrom```.

Here, Jupyter's auto-completion mechanism can be used to provide writing convenience.

For batch jobs, you can trigger execution through methods such as ```print / collectToDataframe / collectToDataframes``` of batch components or ```BatchOperator.execute ()```; for streaming jobs, start the job with ```StreamOperator.execute ()```.

More usage: 
------
 - [Interchange between DataFrame and Operator](docs/pyalink/pyalink-dataframe.md)
 - [StreamOperator data preview](docs/pyalink/pyalink-stream-operator-preview.md)
 - [UDF usage](docs/pyalink/pyalink-udf.md)
 - [Use with PyFlink](docs/pyalink/pyalink-pyflink.md)

Q&A: 
----
Q: After installing PyAlink, error occurs when using: ```AttributeError： 'NoneType' object has no attribute 'jvm```.
How to solve it?

A: This error message occurs when the Java part of PyAlink was not started correctly:
  - Please first check Java 8 is correctly installed. 
  Run ```!java --version``` in Jupyter. Version number, like 1.8.*, is shown if Java 8 is installed correctly.
  Otherwise, try to install Java 8 again, and set environment variables correctly. 
  - Run ```import pyalink; print(pyalink.__path__)``` in Jupyter.
  Use the file explorer of your system to enter this folder.
  It is normal if folders named ```alink``` and ```lib``` are in it.
  Otherwise, PyAlink is not installed correctly, please uninstall and re-install in a correct way. 
  
----

Q: Can I connect to a remote Flink cluster for computation?

A: You can connect to a Flink cluster that has been started through the command: ```useRemoteEnv(host, port, parallelism, flinkHome=None, localIp="localhost", shipAlinkAlgoJar=True, config=None)```.

- ```host``` and ```port```  represent the address of the cluster;


- ```parallelism```  indicates the degree of parallelism of executing the job;
- ```flinkHome``` is the full path of flink. By default, the flink-1.9.0 path that comes with PyAlink is used.
- ```localIp``` specifies the local IP address required to implement the print preview function of Flink ```DataStream```, which needs to be accessible by the Flink cluster. The default is ```localhost```.
- ```shipAlinkAlgoJar``` Whether transmits the Alink algorithm package provided by PyAlink to the remote cluster. If the Alink algorithm package has been placed in the remote cluster, it can be set to False to reduce data transmission.

-----

Q: How to stop long running Flink jobs?

A: When using the local execution environment, just use the Stop button provided by Notebook.

When using a remote cluster, you need to use the job stop function provided by the cluster.

---

Q: Can I run it directly using Python scripts instead of Notebook?

A: Yes. But you need to call resetEnv () at the end of the code, otherwise the script will not exit.

-----


Run Alink Algorithm with a Flink Cluster
--------

1. Prepare a Flink Cluster:
```
  wget https://archive.apache.org/dist/flink/flink-1.9.0/flink-1.9.0-bin-scala_2.11.tgz
  tar -xf flink-1.9.0-bin-scala_2.11.tgz && cd flink-1.9.0
  ./bin/start-cluster.sh
```

2. Build Alink jar from the source:
```
  git clone https://github.com/alibaba/Alink.git
  cd Alink && mvn -Dmaven.test.skip=true clean package shade:shade
```

3. Run Java examples:
```
  ./bin/flink run -p 1 -c com.alibaba.alink.ALSExample [path_to_Alink]/examples/target/alink_examples-0.1-SNAPSHOT.jar
  # ./bin/flink run -p 2 -c com.alibaba.alink.GBDTExample [path_to_Alink]/examples/target/alink_examples-0.1-SNAPSHOT.jar
  # ./bin/flink run -p 2 -c com.alibaba.alink.KMeansExample [path_to_Alink]/examples/target/alink_examples-0.1-SNAPSHOT.jar
```

