<font size=7>English| [简体中文](README.md)</font>

# Alink

Alink is the Machine Learning algorithm platform based on Flink, developed by the PAI team of Alibaba computing platform.
Welcome everyone to join the Alink open source user group to communicate.
 
 
<div align=center>
<img src="https://img.alicdn.com/tfs/TB1qeWTpAT2gK0jSZPcXXcKkpXa-884-1176.jpg" height="20%" width="20%">
</div>

#### List of Algorithms

<div align=center>
<img src="https://img.alicdn.com/tfs/TB1AEOeoBr0gK0jSZFnXXbRRXXa-1320-1048.png" height="60%" width="60%">
</div>

#### pyAlink

<div align=center>
<img src="https://img.alicdn.com/tfs/TB1TmKloAL0gK0jSZFxXXXWHVXa-2070-1380.png" height="60%" width="60%">
</div>

# Quick start - PyAlink Manual

Preparation before use:
---------

1. Make sure the version of python3 on your computer >=3.5.
2. Make sure Java 8 is installed on your computer.
2. Download the corresponding pyalink package according to the Python version:
    - Python 3.5：[Link 1](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.0.1/pyalink-1.0.1_flink_1.9.0_scala_2.11-py3.5.egg) [Link 2](https://github.com/alibaba/Alink/releases/download/v1.0.1/pyalink-1.0.1_flink_1.9.0_scala_2.11-py3.5.egg) (MD5: 9714e5e02b4681a55263970abc6dbe57)
    - Python 3.6：[Link 1](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.0.1/pyalink-1.0.1_flink_1.9.0_scala_2.11-py3.6.egg) [Link 2](https://github.com/alibaba/Alink/releases/download/v1.0.1/pyalink-1.0.1_flink_1.9.0_scala_2.11-py3.6.egg) (MD5: 112638a81c05f1372f9dac880ec527e6)
    - Python 3.7：[Link 1](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.0.1/pyalink-1.0.1_flink_1.9.0_scala_2.11-py3.7.egg) [Link 2](https://github.com/alibaba/Alink/releases/download/v1.0.1/pyalink-1.0.1_flink_1.9.0_scala_2.11-py3.7.egg) (MD5: 9b483da5176977e4f330ca7675120fed)
    - Python 3.8：[Link 1](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.0.1/pyalink-1.0.1_flink_1.9.0_scala_2.11-py3.8.egg) [Link 2](https://github.com/alibaba/Alink/releases/download/v1.0.1/pyalink-1.0.1_flink_1.9.0_scala_2.11-py3.8.egg) (MD5: d04aa5d367bc653d5e872e1eba3494cd)
3. Install using  ```easy_install [path]/pyalink-0.0.1-py3.*.egg```. have to be aware of is:
    * If you have previously installed pyalink, use pip uninstall pyalink to uninstall the previous version before install command.
    * If you have multiple versions of Python, you may need to use a specific version of easy_install, such as easy_install-3.7.
    * If Anaconda is used, you may need to install the package in Anaconda prompt.

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
res = source.select("sepal_length", "sepal_width")
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
-  [Interchange between DataFrame and Operator](docs/pyalink/pyalink-dataframe.md)
- [StreamOperator data preview](docs/pyalink/pyalink-stream-operator-preview.md)
- [UDF usage](docs/pyalink/pyalink-udf.md)

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

