# Cluster Deployment

To deploy Alink on Flink clusters, some jar files are required to be put with Flink. 
There are little differences for different Flink environments. 
This document mainly introduce the deployment of Alink on Standalone and Kubernetes Flink clusters.

## Get jars

The Alink jar files can be obtained from the ```lib``` folder of PyAlink.

Refer to [Alink Quick start](https://github.com/alibaba/Alink/blob/master/README.en-US.md#quick-start)

After installed the PyAlink, you can run ```python3 -c "import os; import pyalink; print(os.path.join(pyalink.__path__[0], 'lib'))"``` to get the ```lib``` path of PyAlink from the stdout.

The structure of ```lib``` is as follows after filesystems jars are also downloaded: 

<div align=center>
<img src="https://img.alicdn.com/tfs/TB1yLfmcQcx_u4jSZFlXXXnUFXa-888-460.png" height="25%" width="25%">
</div>

## Deploy on Standalone Clusters

1. Copy above jars in the PyAlink ```lib``` folder to the ```lib``` folder of Flink.
2. Add ```classloader.resolve-order: parent-first``` in the Flink configuration file ```flink-conf.yaml```.
3. Execute ```bin/start-cluster.sh``` in the folder of Flink to start a standalone cluster.
4. Execute ```bin/taskmanager.sh start``` to add more TaskManagers。

Refer to：[https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/cluster_setup.html](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/cluster_setup.html)

## Deploy on Kubernetes

Copy above jars in the Python ```lib``` folder to the docker image:
add the following command to the ```DockerFile``` of Flink image. 

```
ADD $PY_ALINK_LIB_FOLDER/* $FLINK_LIB_DIR/
```

Add ```classloader.resolve-order: parent-first``` in the configuration file ```flink-conf.yaml```

Then refer to：[https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/kubernetes.html](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/kubernetes.html)
to deploy the image to Kubernetes clusters.
