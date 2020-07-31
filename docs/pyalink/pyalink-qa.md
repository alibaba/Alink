PyAlink 常见问题
===============

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

