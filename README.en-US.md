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

# Quick start

## PyAlink Manual

### Preparation before use:
---------


#### About package names and versions:
  - PyAlink provides different Python packages for Flink versions that Alink supports: 
  package `pyalink` always maintains Alink Python API against the latest Flink version, which is 1.12, 
  while `pyalink-flink-***` support old-version Flink, which are `pyalink-flink-1.11`, `pyalink-flink-1.10` and `pyalink-flink-1.9` for now. 
  - The version of python packages always follows Alink Java version, like `1.3.1`.
  
#### Installation steps:

1. Make sure the version of python3 on your computer is 3.6 or 3.7.
2. Make sure Java 8 is installed on your computer.
3. Use pip to install:
  `pip install pyalink`, `pip install pyalink-flink-1.11`, `pip install pyalink-flink-1.10` or `pip install pyalink-flink-1.9`.


#### Potential issues:

1. `pyalink` and/or `pyalink-flink-***` can not be installed at the same time. Multiple versions are not allowed.
If `pyalink` or `pyalink-flink-***` was/were installed, please use `pip uninstall pyalink` or `pip uninstall pyalink-flink-***` to remove them.

2. If `pip install` is slow of failed, refer to [this article](https://segmentfault.com/a/1190000006111096) to change the pip source, or use the following download links:
   - Flink 1.12：[Link](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.3.1/pyalink-1.3.1-py3-none-any.whl) (MD5: a7c793b1bb38045c5d1ef4c50285562f)
   - Flink 1.11：[Link](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.3.1/pyalink_flink_1.11-1.3.1-py3-none-any.whl) (MD5: f71779fb6d3afe99bab593d8c91f540f)
   - Flink 1.10：[Link](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.3.1/pyalink_flink_1.10-1.3.1-py3-none-any.whl) (MD5: 4950fc5cafac27d3062a047ab2b7bb34)
   - Flink 1.9: [Link](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.3.1/pyalink_flink_1.9-1.3.1-py3-none-any.whl) (MD5: f6071a4e9f6b41a3558ed97bb235346e)
3. If multiple version of Python exist, you may need to use a special version of `pip`, like `pip3`;
If Anaconda is used, the command should be run in Anaconda prompt. 


#### Download file system and Catalog dependency jar files:

After PyAlink installed, you can run ```download_pyalink_dep_jars``` to download dependency jars for file system and Hive.
(If there is an error that could not find the command, you can run the python command ```python3 -c 'from pyalink.alink.download_pyalink_dep_jars import main;main()'``` directly.)

After executed the command, you'll see a prompt asking you about the dependencies and their versions to be downloaded. 
The following dependencies and their versions of jars are supported:

- OSS：3.4.1
- Hadoop：2.8.3
- Hive：2.3.4
- MySQL: 5.1.27
- Derby: 10.6.1.0
- SQLite: 3.19.3
- S3-hadoop: 1.11.788
- S3-presto: 1.11.788
- odps: 0.36.4-public

These jars will be installed to the ```lib/plugins``` folder of PyAlink. 
Note that these command require the access for the folder.

You can also add the argument ```-d``` when executing the command, i.e.  ```download_pyalink_dep_jars -d```.
It will install all dependency jars.

### Start using: 
-------
You can start using PyAlink with Jupyter Notebook to provide a better experience.

Steps for usage: 

1. Start Jupyter: ```jupyter notebook``` in terminal
, and create Python 3 notebook.

2. Import the pyalink package: ```from pyalink.alink import *```.

3. Use this command to create a local runtime environment:

   ```useLocalEnv(parallism, flinkHome=None, config=None)```.

   Among them, the parameter  ```parallism```  indicates the degree of parallelism used for execution;```flinkHome``` is the full path of flink, and usually no need to set; ```config``` is the configuration parameter accepted by Flink. After running, the following output appears, indicating that the initialization of the running environment is successful.
```
JVM listening on ***
Python listening on ***
```
4. Start writing PyAlink code, for example:
```python
source = CsvSourceBatchOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")
res = source.select(["sepal_length", "sepal_width"])
df = res.collectToDataframe()
print(df)
```

### Write code: 
------
In PyAlink, the interface provided by the algorithm component is basically the same as the Java APIs, that is, an algorithm component is created through the default construction method, then the parameters are set through ```setXXX```, and other components are connected through ```link / linkTo / linkFrom```.

Here, Jupyter Notebook's auto-completion mechanism can be used to provide writing convenience.

For batch jobs, you can trigger execution through methods such as ```print / collectToDataframe / collectToDataframes``` of batch components or ```BatchOperator.execute ()```; for streaming jobs, start the job with ```StreamOperator.execute ()```.

### More usage: 
------
 - [Interchange between DataFrame and Operator](docs/pyalink/pyalink-dataframe.md)
 - [StreamOperator data preview](docs/pyalink/pyalink-stream-operator-preview.md)
 - [UDF/UDTF/SQL usage](docs/pyalink/pyalink-udf.md)
 - [Use with PyFlink](docs/pyalink/pyalink-pyflink.md)
 - [PyAlink Q&A](docs/pyalink/pyalink-qa.md)

## Java API Manual

### KMeans Example
```java
String URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv";
String SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

BatchOperator data = new CsvSourceBatchOp()
        .setFilePath(URL)
        .setSchemaStr(SCHEMA_STR);

VectorAssembler va = new VectorAssembler()
        .setSelectedCols(new String[]{"sepal_length", "sepal_width", "petal_length", "petal_width"})
        .setOutputCol("features");

KMeans kMeans = new KMeans().setVectorCol("features").setK(3)
        .setPredictionCol("prediction_result")
        .setPredictionDetailCol("prediction_detail")
        .setReservedCols("category")
        .setMaxIter(100);

Pipeline pipeline = new Pipeline().add(va).add(kMeans);
pipeline.fit(data).transform(data).print();
```

### With Flink-1.12
```xml
<dependency>
    <groupId>com.alibaba.alink</groupId>
    <artifactId>alink_core_flink-1.12_2.11</artifactId>
    <version>1.3.1</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_2.11</artifactId>
    <version>1.12.0</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner_2.11</artifactId>
    <version>1.12.0</version>
</dependency>
```

### With Flink-1.11
```xml
<dependency>
    <groupId>com.alibaba.alink</groupId>
    <artifactId>alink_core_flink-1.11_2.11</artifactId>
    <version>1.3.1</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_2.11</artifactId>
    <version>1.11.0</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner_2.11</artifactId>
    <version>1.11.0</version>
</dependency>
```

### With Flink-1.10
```xml
<dependency>
    <groupId>com.alibaba.alink</groupId>
    <artifactId>alink_core_flink-1.10_2.11</artifactId>
    <version>1.3.1</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
```

### With Flink-1.9

```xml
<dependency>
    <groupId>com.alibaba.alink</groupId>
    <artifactId>alink_core_flink-1.9_2.11</artifactId>
    <version>1.3.1</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_2.11</artifactId>
    <version>1.9.0</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner_2.11</artifactId>
    <version>1.9.0</version>
</dependency>
```


Get started to run Alink Algorithm with a Flink Cluster
--------

1. Prepare a Flink Cluster:
```shell
  wget https://archive.apache.org/dist/flink/flink-1.12.0/flink-1.12.0-bin-scala_2.11.tgz
  tar -xf flink-1.12.0-bin-scala_2.11.tgz && cd flink-1.12.0
  ./bin/start-cluster.sh
```

2. Build Alink jar from the source:
```shell
  git clone https://github.com/alibaba/Alink.git
  cd Alink && mvn -Dmaven.test.skip=true clean package shade:shade
```

3. Run Java examples:
```shell
  ./bin/flink run -p 1 -c com.alibaba.alink.ALSExample [path_to_Alink]/examples/target/alink_examples-1.1-SNAPSHOT.jar
  # ./bin/flink run -p 1 -c com.alibaba.alink.GBDTExample [path_to_Alink]/examples/target/alink_examples-1.1-SNAPSHOT.jar
  # ./bin/flink run -p 1 -c com.alibaba.alink.KMeansExample [path_to_Alink]/examples/target/alink_examples-1.1-SNAPSHOT.jar
```

Deployment
---------

[Cluster](docs/deploy/cluster-deploy.en-US.md)