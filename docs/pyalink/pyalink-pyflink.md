与 PyFlink 一同使用
==================

在最新的发布中，PyAlink 与 PyFlink 进行了一定的整合。
用户在新版本的 PyAlink 中能够使用 PyFlink 的部分功能，同时 PyAlink 脚本也支持像 PyFlink 脚本一样使用 `flink run` 来提交作业了。

需要注意的是：这个版本只有 Flink-1.10 及以上对应的 Python 包 `pyalink` 才具有，`pyalink-flink-1.9` 没有以下功能。


一个简单的例子
------------

我们首先来看一个 PyAlink 与 PyFlink 结合的简单例子：
```
from pyalink.alink import *
env, btenv, senv, stenv = getMLEnv()
t = stenv.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
source = TableSourceStreamOp(t)
source.print()
StreamOperator.execute()
```

这段代码中， `getMLEnv` 设定 PyAlink 的执行环境与 PyFlink 一致，同时返回的执行环境；
接着使用 `stenv.from_elements` 来创建一个简单的 Table；
然后使用 PyAlink 的 `TableSourceStreamOp` 将 Table 转换为 Alink 所接受的 Operator，
进行输出打印。

这段代码示例既可以直接在 Notebook 中运行，也可以直接保存成`.py` 的脚本文件，使用 PyFlink 脚本的运行方式来执行：
1. `python ***.py`： 直接使用本地运行环境；
2. `flink run -py ***.py`：将脚本提交给远程集群来运行，参考 [Job Submission Examples](https://ci.apache.org/projects/flink/flink-docs-release-1.11/ops/cli.html#job-submission-examples)。


### 与 PyFlink 共用执行环境

在新版本中，PyAlink 新增了 getMLEnv 的接口，直接获取 PyFlink 的执行环境，见上文的代码示例。
这个接口返回四元组`(benv, btenv, senv, stenv)`，分别对应 PyFlink 中的四种执行环境：
ExecutionEnvironment、BatchTableEnvironment、StreamExecutionEnvironment 和 StreamTableEnvironment。
基于这四个变量，用户可以调用 PyFlink 的接口。

此外，在之前的版本中，PyAlink 提供了方便使用 Flink 不同执行环境的函数：useLocalEnv 和 useRemoteEnv。
这两个接口在新版本中将同样返回四元组 `(benv, btenv, senv, stenv)`。
用户可以通过返回的执行环境来调用 PyFlink 的接口。

`useLocalEnv/useRemoteEnv` 与 `getMLEnv` 的区别在于：
  - `useLocalEnv/useRemoteEnv` 显示指定了执行环境是本地还是远程集群，可以根据需要调用、切换，
  - 而`getMLEnv`则默认情况为本地执行，同时可以根据脚本的运行方式采用对应的执行环境。
  
这里要注意的是，为了便于调试，PyAlink 允许在调用`useLocalEnv/useRemoteEnv`后，
再调用`getMLEnv`，但此时的脚本是不支持`flink run`来提交作业的。

### 与 Table 互相转换

在文首的例子中，我们看到 PyFlink 的 `Table` 可以转换为 PyAlink 的 Operator， 然后使用 Alink 的算法组件进行后续操作。

具体来说，PyAlink 提供了 `TableSourceBatchOp` 和 `TableSourceStreamOp` 将 PyFlink 中的 `Table` 分别转换为 Alink 中的 `BatchOperator` 和 `StreamOperator`。

同时，对于 PyAlink 中的 Operator，提供了 `getOutputTable` 来获取算法组件对应的 `Table` 。
