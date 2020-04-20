PyAlink 使用介绍
===============


使用前准备：
---------

包名和版本说明：

  - PyAlink 根据 Alink 所支持的 Flink 版本提供不同的 Python 包：
其中，`pyalink` 包对应为 Alink 所支持的最新 Flink 版本，而 `pyalink-flink-***` 为旧版本的 Flink 版本，当前提供 `pyalink-flink-1.9`。
  - Python 包的版本号与 Alink 的版本号一致，例如`1.1.0`。

安装步骤：
1. 确保使用环境中有Python3，版本限于 3.6 和 3.7。
2. 确保使用环境中安装有 Java 8。
3. 使用 pip 命令进行安装：
  `pip install pyalink` 或者 `pip install pyalink-flink-1.9` （注意：当前 `pyalink-flink-1.9` 还不可用，请使用下面提供的下载链接。）。
  
安装注意事项：

1. `pyalink` 和 `pyalink-flink-***` 不能同时安装，也不能与旧版本同时安装。
如果之前安装过 `pyalink` 或者 `pyalink-flink-***`，请使用`pip uninstall pyalink` 或者 `pip uninstall pyalink-flink-***` 卸载之前的版本。
2. 出现`pip`安装缓慢或不成功的情况，可以参考[这篇文章](https://segmentfault.com/a/1190000006111096)修改pip源，或者直接使用下面的链接下载 whl 包，然后使用 `pip` 安装：
   - Flink 1.10：[链接1](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.1.1/pyalink-1.1.1-py3-none-any.whl) [链接2](https://github.com/alibaba/Alink/releases/download/v1.1.1/pyalink-1.1.1-py3-none-any.whl) (MD5: b0541ea013e0ceae47d6961149d2c46f)
   - Flink 1.9: [链接1](https://alink-release.oss-cn-beijing.aliyuncs.com/v1.1.1/pyalink_flink_1.9-1.1.1-py3-none-any.whl) [链接2](https://github.com/alibaba/Alink/releases/download/v1.1.1/pyalink_flink_1.9-1.1.1-py3-none-any.whl) (MD5: fca8937ff724734dc3bcd27d12cdc997)
3. 如果有多个版本的 Python，可能需要使用特定版本的 `pip`，比如 `pip3`；如果使用 Anaconda，则需要在 Anaconda 命令行中进行安装。

开始使用：
-------
我们推荐通过 Jupyter Notebook 来使用 PyAlink，能获得更好的使用体验。

使用步骤：
1. 在命令行中启动Jupyter：```jupyter notebook```，并新建 Python 3 的 Notebook 。
2. 导入 pyalink 包：```from pyalink.alink import *```。
3. 使用方法创建本地运行环境：
```useLocalEnv(parallism, flinkHome=None, config=None)```。
其中，参数 ```parallism``` 表示执行所使用的并行度；```flinkHome``` 为 flink 的完整路径，默认使用 PyAlink 自带的 flink-1.9.0 路径；```config```为Flink所接受的配置参数。运行后出现如下所示的输出，表示初始化运行环境成功：
```
JVM listening on ***
Python listening on ***
```
4. 开始编写 PyAlink 代码，例如：
```
source = CsvSourceBatchOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")
res = source.select(["sepal_length", "sepal_width"])
df = res.collectToDataframe()
print(df)
```

编写代码：
------
在 PyAlink 中，算法组件提供的接口基本与 Java API 一致，即通过默认构造方法创建一个算法组件，然后通过 ```setXXX``` 设置参数，通过 ```link/linkTo/linkFrom``` 与其他组件相连。
这里利用 Jupyter 的自动补全机制可以提供书写便利。

对于批式作业，可以通过批式组件的 ```print/collectToDataframe/collectToDataframes``` 等方法或者 ```BatchOperator.execute()``` 来触发执行；对于流式作业，则通过 ```StreamOperator.execute()``` 来启动作业。



更多用法：
------
  - [DataFrame与Operator互转](pyalink-dataframe.md)
  - [StreamOperator数据预览](pyalink-stream-operator-preview.md)
  - [UDF使用](pyalink-udf.md)
  - [与 PyFlink 一同使用](pyalink-pyflink.md)


Q&A：
----
Q：安装 PyAlink 后，使用时报错：```AttributeError： 'NoneType' object has no attribute 'jvm```，如何解决？

A：这个报错信息是因为 PyAlink 的 Java 部分没有成功启动导致的： 
  - 请先检查是否正确安装 Java 8，可以在 Jupyter 中直接运行 ```!java --version```，如果正确显示版本号（比如 1.8.*）则正常，否则请安装 Java 8，并检查环境变量是否正确。
  - 在 Jupyter 中运行```import pyalink; print(pyalink.__path__)```，应该输出一个路径。
  请使用系统的文件管理工具定位到这个目录，如果这个目录包含有名为 ```alink``` 和 ```lib``` 目录则正常，否则 pyalink 安装有问题，请卸载重装。
----

Q：能否连接远程 Flink 集群进行计算？

A：通过方法可以连接一个已经启动的 Flink 集群：```useRemoteEnv(host, port, parallelism, flinkHome=None, localIp="localhost", shipAlinkAlgoJar=True, config=None)```。其中，参数
  - ```host``` 和 ```port``` 表示集群的地址；
  - ```parallelism``` 表示执行作业的并行度；
  - ```flinkHome``` 为 flink 的完整路径，默认使用 PyAlink 自带的 flink-1.9.0 路径；
  - ```localIp``` 指定实现 ```Flink DataStream``` 的打印预览功能时所需的本机IP地址，需要 Flink 集群能访问。默认为```localhost```。
  - ```shipAlinkAlgoJar``` 是否将 PyAlink 提供的 Alink 算法包传输给远程集群，如果远程集群已经放置了 Alink 算法包，那么这里可以设为 False，减少数据传输。
  
  Flink-1.10 及以上版本对应的 pyalink 包，还支持类似 pyflink 脚本的远程集群运行方式。

-----

Q：如何停止长时间运行的Flink作业？

A：使用本地执行环境时，使用 Notebook 提供的“停止”按钮即可。
使用远程集群时，需要使用集群提供的停止作业功能。

-----

Q：能否直接使用 Python 脚本而不是 Notebook 运行？

A：可以。但需要在代码最后调用 resetEnv()，否则脚本不会退出。

-----

