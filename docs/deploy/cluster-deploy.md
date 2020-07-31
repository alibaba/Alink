# 部署集群

在 Flink 集群部署 Alink，需要部署几个 Jar 包（本文会有一个部分专门讲述如何获取）。
对于不同 Flink 集群环境，方式有些区别，本文主要讨论 Standalone 集群和 Kubernetes 集群。

## 获取集群部署所需Jar包

由于 Alink 可以通过 Java 和 Python 两种方式提交，建议在集群部署的时候将相关 Jar 包一起部署上去。 

尽管 Alink Java 没有单独提供集群部署所需 Jar 包的下载，但它们与 PyAlink 所使用的一致，所以可以从 PyAlink 的```lib```目录中获取。
具体方式如下：

按照[Alink快速开始](https://github.com/alibaba/Alink/blob/master/README.md#快速开始) 安装好 PyAlink，
执行命令```python3 -c "import os; import pyalink; print(os.path.join(pyalink.__path__[0], 'lib'))"```，
在 Python 标准输出中即可拿到对应的 PyAlink ```lib```目录的路径。

在安装完成文件系统和Hive后，```lib```目录的结构如下图所示：

<div align=center>
<img src="https://img.alicdn.com/tfs/TB1yLfmcQcx_u4jSZFlXXXnUFXa-888-460.png" height="25%" width="25%">
</div>

## 在Standalone集群上部署
将前面获取到的```lib```目录中的 Jar 包拷贝到 Flink 的```lib```目录后，然后按下面步骤启动 Flink 集群即可:

1. 在 Flink 配置中增加：```classloader.resolve-order: parent-first```，通常在配置文件```conf/flink-conf.yaml```里添加。
2. 在 flink-[版本号]目录下，运行 ```bin/start-cluster.sh``` 启动集群。
3. 运行```bin/taskmanager.sh start``` 可以增加 TaskManager。

更多内容可以参考：[https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/cluster_setup.html](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/cluster_setup.html)

## 在Kubernetes集群上部署

将前面获取的目录中的 Jar 包放置进 Docker 镜像中，并将以下命令添加进 DockerFile，打包镜像即可。

```
ADD $PY_ALINK_LIB_FOLDER/* $FLINK_LIB_DIR/
```

使用Flink官方提供的部署方式，其中增加 ```classloader.resolve-order: parent-first``` 配置项。

参考：[https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/kubernetes.html](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/kubernetes.html)