<font size=7>[English](README.en-US.md)| 简体中文</font>

# Alink

 Alink是基于Flink的通用算法平台,由阿里巴巴计算平台PAI团队研发,欢迎大家加入Alink开源用户钉钉群进行交流。
 
 
<div align=center>
<img src="https://img.alicdn.com/tfs/TB1kQU0sQY2gK0jSZFgXXc5OFXa-614-554.png" height="25%" width="25%">
</div>

- Alink文档：https://www.yuque.com/pinshu/alink_doc
- Alink使用指南：https://www.yuque.com/pinshu/alink_guide
- Alink插件下载器：https://www.yuque.com/pinshu/alink_guide/plugin_downloader

#### 开源算法列表

<div align=center>
<img src="https://img.alicdn.com/tfs/TB1n.edorj1gK0jSZFOXXc7GpXa-1635-714.png" height="60%" width="60%">
</div>

#### PyAlink 使用截图

<div align=center>
<img src="https://img.alicdn.com/tfs/TB1TmKloAL0gK0jSZFxXXXWHVXa-2070-1380.png" height="60%" width="60%">
</div>

# 快速开始

## PyAlink 使用介绍

### 使用前准备：
---------

#### 包名和版本说明：

  - PyAlink 根据 Alink 所支持的 Flink 版本提供不同的 Python 包：
其中，`pyalink` 包对应为 Alink 所支持的最新 Flink 版本，当前为 1.13，而 `pyalink-flink-***` 为旧版本的 Flink 版本，当前提供 `pyalink-flink-1.12`, `pyalink-flink-1.11`, `pyalink-flink-1.10` 和 `pyalink-flink-1.9`。
  - Python 包的版本号与 Alink 的版本号一致，例如`1.5.1`。

####安装步骤：
1. 确保使用环境中有Python3，版本限于 3.6，3.7 和 3.8。
2. 确保使用环境中安装有 Java 8。
3. 使用 pip 命令进行安装：
  `pip install pyalink`、`pip install pyalink-flink-1.12`、`pip install pyalink-flink-1.11`、`pip install pyalink-flink-1.10` 或者 `pip install pyalink-flink-1.9`。
  
#### 安装注意事项：

1. `pyalink` 和 `pyalink-flink-***` 不能同时安装，也不能与旧版本同时安装。
如果之前安装过 `pyalink` 或者 `pyalink-flink-***`，请使用`pip uninstall pyalink` 或者 `pip uninstall pyalink-flink-***` 卸载之前的版本。
2. 出现`pip`安装缓慢或不成功的情况，可以参考[这篇文章](https://segmentfault.com/a/1190000006111096)修改pip源，或者直接使用下面的链接下载 whl 包，然后使用 `pip` 安装：
   - Flink 1.13：[链接](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.5.1/pyalink-1.5.1-py3-none-any.whl) (MD5: 870f0f2cea50238c2276ff3d6e6c776c)
   - Flink 1.12：[链接](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.5.1/pyalink_flink_1.12-1.5.1-py3-none-any.whl) (MD5: 80e13deb4027c2f6e8678bab5e6af27b)
   - Flink 1.11：[链接](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.5.1/pyalink_flink_1.11-1.5.1-py3-none-any.whl) (MD5: 31dd9a9e037bbf5a6ce6d8ad3bd4ed6c)
   - Flink 1.10：[链接](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.5.1/pyalink_flink_1.10-1.5.1-py3-none-any.whl) (MD5: e46c21699df0b298b1b6df92ccc4e5e1)
   - Flink 1.9: [链接](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.5.1/pyalink_flink_1.9-1.5.1-py3-none-any.whl) (MD5: 77cb3ddc105089ef740d800c5610f1a1)
3. 如果有多个版本的 Python，可能需要使用特定版本的 `pip`，比如 `pip3`；如果使用 Anaconda，则需要在 Anaconda 命令行中进行安装。

#### 下载安装文件系统或 Catalog 依赖 jar 包：

安装 PyAlink 之后，可以直接运行 ```download_pyalink_dep_jars``` 命令，下载支持文件系统功能所需要的 jar 包。
（如果提示找不到这个命令，可以尝试直接运行脚本： ```python3 -c 'from pyalink.alink.download_pyalink_dep_jars import main;main()'```。）

运行这个命令后，将提问是否安装某种文件系统对应的 jar 包，并选择合适的版本。 当前支持的文件系统包括：
 
- OSS：3.4.1
- Hadoop：2.8.3
- Hive：2.3.4
- MySQL: 5.1.27
- Derby: 10.6.1.0
- SQLite: 3.19.3
- S3-hadoop: 1.11.788
- S3-presto: 1.11.788
- odps: 0.36.4-public

这些 jar 包将被下载到 PyAlink 安装路径的 ```lib/plugins``` 目录下，所以要求运行命令时有 PyAlink 安装目录的权限。

运行命令时，也可以增加参数：```download_pyalink_dep_jars -d```，将自动下载所有的 jar 包。

### 开始使用：
-------
可以通过 Jupyter Notebook 来开始使用 PyAlink，能获得更好的使用体验。

使用步骤：
1. 在命令行中启动Jupyter：`jupyter notebook`，并新建 Python 3 的 Notebook 。
2. 导入 pyalink 包：`from pyalink.alink import *`。
3. 使用方法创建本地运行环境：
`useLocalEnv(parallism, flinkHome=None, config=None)`。
其中，参数 `parallism` 表示执行所使用的并行度；`flinkHome` 为 flink 的完整路径，一般情况不需要设置；`config`为Flink所接受的配置参数。运行后出现如下所示的输出，表示初始化运行环境成功：
```
JVM listening on ***
```
4. 开始编写 PyAlink 代码，例如：
```python
source = CsvSourceBatchOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")
res = source.select(["sepal_length", "sepal_width"])
df = res.collectToDataframe()
print(df)
```

### 编写代码：
------
在 PyAlink 中，算法组件提供的接口基本与 Java API 一致，即通过默认构造方法创建一个算法组件，然后通过 `setXXX` 设置参数，通过 `link/linkTo/linkFrom` 与其他组件相连。
这里利用 Jupyter Notebook 的自动补全机制可以提供书写便利。

对于批式作业，可以通过批式组件的 `print/collectToDataframe/collectToDataframes` 等方法或者 `BatchOperator.execute()` 来触发执行；对于流式作业，则通过 `StreamOperator.execute()` 来启动作业。


### 更多用法：
------
  - [DataFrame 与 Operator 互转](docs/pyalink/pyalink-dataframe.md)
  - [StreamOperator 数据预览](docs/pyalink/pyalink-stream-operator-preview.md)
  - [UDF/UDTF/SQL 使用](docs/pyalink/pyalink-udf.md)
  - [与 PyFlink 一同使用](docs/pyalink/pyalink-pyflink.md)
  - [PyAlink 常见问题](docs/pyalink/pyalink-qa.md)

## Java 接口使用介绍
----------

### 示例代码

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

### Flink-1.13 的 Maven 依赖
```xml
<dependency>
    <groupId>com.alibaba.alink</groupId>
    <artifactId>alink_core_flink-1.13_2.11</artifactId>
    <version>1.5.1</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_2.11</artifactId>
    <version>1.13.0</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner_2.11</artifactId>
    <version>1.13.0</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-clients_2.11</artifactId>
    <version>1.13.0</version>
</dependency>
```

### Flink-1.12 的 Maven 依赖
```xml
<dependency>
    <groupId>com.alibaba.alink</groupId>
    <artifactId>alink_core_flink-1.12_2.11</artifactId>
    <version>1.5.1</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_2.11</artifactId>
    <version>1.12.1</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner_2.11</artifactId>
    <version>1.12.1</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-clients_2.11</artifactId>
    <version>1.12.1</version>
</dependency>
```

### Flink-1.11 的 Maven 依赖
```xml
<dependency>
    <groupId>com.alibaba.alink</groupId>
    <artifactId>alink_core_flink-1.11_2.11</artifactId>
    <version>1.5.1</version>
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
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-clients_2.11</artifactId>
    <version>1.11.0</version>
</dependency>
```

### Flink-1.10 的 Maven 依赖
```xml
<dependency>
    <groupId>com.alibaba.alink</groupId>
    <artifactId>alink_core_flink-1.10_2.11</artifactId>
    <version>1.5.1</version>
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

### Flink-1.9 的 Maven 依赖

```xml
<dependency>
    <groupId>com.alibaba.alink</groupId>
    <artifactId>alink_core_flink-1.9_2.11</artifactId>
    <version>1.5.1</version>
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



## 快速开始在集群上运行Alink算法
--------

1. 准备Flink集群
```shell
  wget https://archive.apache.org/dist/flink/flink-1.13.0/flink-1.13.0-bin-scala_2.11.tgz
  tar -xf flink-1.13.0-bin-scala_2.11.tgz && cd flink-1.13.0
  ./bin/start-cluster.sh
```

2. 准备Alink算法包
```shell
  git clone https://github.com/alibaba/Alink.git
  # add <scope>provided</scope> in pom.xml of alink_examples.
  cd Alink && mvn -Dmaven.test.skip=true clean package shade:shade
```

3. 运行Java示例
```shell
  ./bin/flink run -p 1 -c com.alibaba.alink.ALSExample [path_to_Alink]/examples/target/alink_examples-1.5-SNAPSHOT.jar
  # ./bin/flink run -p 1 -c com.alibaba.alink.GBDTExample [path_to_Alink]/examples/target/alink_examples-1.5-SNAPSHOT.jar
  # ./bin/flink run -p 1 -c com.alibaba.alink.KMeansExample [path_to_Alink]/examples/target/alink_examples-1.5-SNAPSHOT.jar
```

## 部署
----------

[集群部署](docs/deploy/cluster-deploy.md)
