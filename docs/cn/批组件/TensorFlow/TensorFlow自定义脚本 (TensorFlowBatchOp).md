# TensorFlow自定义脚本 (TensorFlowBatchOp)
Java 类名：com.alibaba.alink.operator.batch.tensorflow.TensorFlowBatchOp

Python 类名：TensorFlowBatchOp


## 功能介绍

该组件支持用户传入 TensorFlow 脚本，使用传入的批数据进行任意处理，并可以将数据写回 Alink 端。

用户需要提供自己编写的 TensorFlow 脚本文件。
关于脚本的编写，请先阅读下面的介绍。

### TensorFlow 自定义脚本类组件的概况

Alink TensorFlow 自定义脚本类的组件，基于的是从 Alink 进程拉起 Python 进程、执行 Python 代码的能力。
通过这个能力，Alink 可以将数据传递给 Python 进程，在 Python 进程中执行自定义代码，然后将处理的结果返回给 Alink。

下表列出了自定义脚本类组件包含的具体组件及其区别：

|                           | TF 版本  | 传入数据 | 传入参数类型           | 输出数据                 |
|---------------------------|--------|------|------------------|----------------------|
| TensorFlowBatchOp         | 1.15.2 | 批数据  | BatchTaskConfig  | 自定义                  |
| TensorFlow2BatchOp        | 2.3.1  | 批数据  | BatchTaskConfig  | 自定义                  |
| TensorFlowStreamOp        | 1.15.2 | 流数据  | StreamTaskConfig | 自定义                  |
| TensorFlow2StreamOp       | 2.3.1  | 流数据  | StreamTaskConfig | 自定义                  |
| TFTableModelTrainBatchOp  | 1.15.2 | 批数据  | TrainTaskConfig  | 要求将训练模型保存到指定目录，无其他输出 |
| TF2TableModelTrainBatchOp | 2.3.1  | 批数据  | TrainTaskConfig  | 要求将训练模型保存到指定目录，无其他输出 |

### 代码编写

用户可以提供多个 Python 文件， 其中一个为主文件，作为自定义脚本的入口。

#### 参数传递

在主文件中，必须包含一个名为 main 的函数，接受一个参数，参数的类型根据使用的组件不同而不同，具体见上表。

结合这三种 Config 的 [源码](https://github.com/alibaba/Alink/blob/master/core/src/main/python/akdl/akdl/runner/config.py) ，对这三种 Config 的字段进行说明：
  - 三者共有的字段：
    - tf_context: TFContext 类型，可以调用 flink_stream_dataset() 获取 一个TFRecordDataset，但这个数据集只能扫描一次；
    - num_workers：总的 worker 数；
    - cluster：TF_CONFIG 中的 cluster 字段；
    - task_type：``TF_CONFIG`` 中的 task.type 字段，取值有 'chief'、'worker' 或者 'ps'
    - task_index：TF_CONFIG 中的 task.index 字段；
    - work_dir：工作目录；
    - user_params：用户自定义参数，字典类型，对应为组件 setUserParams 的值。
  - BatchTaskConfig 有的字段：
      - dataset_file：将 tf_context.flink_stream_dataset() 得到的数据集写到本地文件中，从而可以读取多次；
      - dataset_length：数据条数；
      - output_writer：一个用于将数据写回 Alink 的工具，见下面说明。
  - StreamTaskConfig 有的字段：
      - dataset_fn：调用后返回的一个 DataSet；
      - output_writer：一个用于将数据写回 Alink 的工具，见下面说明。
  - TrainTaskConfig 有的字段：
      - dataset_file：将 tf_context.flink_stream_dataset() 得到的数据集写到本地文件中，从而可以读取多次；
      - dataset_length：数据条数；
      - saved_model_dir：训练完成后，必须将模型以 SavedModel 的格式导出到这个目录下。

#### 数据输入

首先需要说明一下 TensorFlow 进程与数据集之间的关系。
当 Alink 作业本身的并发度大于 1 时，会有多个 Worker 同时执行任务，数据会根据任务的配置分布在各个 Worker 上。 
在进入 TF 组件对应的任务时，各个 Worker 会启动一个 TF 进程，此时各个 Worker 会将其拥有的数据传递给 TF 进程。

这里**每个 TF 进程只能访问到它所在 Worker 的数据，而访问不了其他 Worker 的数据**。
这一点与某些 TensorFlow 分布式训练的写法不同：在一些 TensorFlow 分布式训练的写法中，数据集中存储在某些共享文件系统（例如 HDFS）上，整体作为模型训练数据，各个 TF 进程通过 shard 的形式读取部分数据。

从 Alink 传到 TensorFlow 进程的数据集为 TFRecordDataset 格式，每条数据是序列化后的 tf.train.Example 实例，可以通过 tf.parse_single_example 来进行解析。
其中，parse_single_example 的 features 参数与原本数据集的列名和类型对应，例如 tf.int64、tf.float32、tf.string。

#### 数据输出

* 通过 output_writer 可以从 TensorFlow 进程往 Alink 写回数据。
* 写出的数据需要是一个序列化后的 tf.train.Example 实例。
* Example 实例所含的 features需要与组件参数 OutputSchemaStr 中的列名和类型对应。

#### 分布式训练

在代码中可以获取环境变量 TF_CONFIG，从而可以写分布式训练的代码，包括 Estimator + PS 与 AllReduce 的模式。

### akdl 库

在 Alink 提供的 [akdl 库](https://github.com/alibaba/Alink/tree/master/core/src/main/python/akdl) 中，提供了一些便捷调用的函数，方便书写代码。具体例子可以参考 alink_dl_predictors/predictor-tf/src/test/resources/tf_dnn_batch.py。

但需要注意的是： akdl 库内的写法采用的是 TF1 或者 TF2 中 TF1 兼容模式的写法，因此可能不能满足您的需要，例如 TF2 动态图运行模式等等。（即使仅引入 akdl 包中的头文件，也可能导致运行不了一些纯 TF2 写法的代码。）这个时候就需要您另外书写代码了。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| mainScriptFile | 主脚本文件路径 | 主脚本文件路径，需要是参数 userFiles 中的一项，并且包含 main 函数 | String | ✓ |  |  |
| outputSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如 "f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| userFiles | 所有自定义脚本文件的路径 | 所有自定义脚本文件的路径 | String | ✓ |  |  |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  |  | 4 |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  |  | null |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；如果是目录，那么只能使用本地路径，即 file://。 | String |  |  | "" |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  |  | null |
| userParams | 自定义参数 | 用户自定义参数，JSON 字典格式的字符串 | String |  |  | "{}" |


### 脚本路径说明

脚本路径可以是以下形式：
  - 本地文件：file:// 加绝对路径，例如 file:///tmp/dnn.py；
  - Java 包中的资源文件：res:// 加路径，例如 res:///dnn.py；
  - http/https 文件：http:// 或 https:// 路径；
  - OSS 文件：oss:// 加路径和 Endpoint 和 access key 等信息，例如oss://bucket/xxx/xxx/xxx.py?host=xxx&access_key_id=xxx&access_key_secret=xxx；
  - HDFS 文件：hdfs:// 加路径；

### 输出数据说明

脚本中可以输出并传回 Alink 端，形成 Flink Table 的形式，进行后续的处理。

即使没有数据需要输出，参数中的输出数据 Schema 也需要填写个非空的形式，例如dummy string。

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
import json

source = RandomTableSourceBatchOp() \
    .setNumRows(100) \
    .setNumCols(10)

colNames = source.getColNames()
source = source.select("*, case when RAND() > 0.5 then 1. else 0. end as label");
label = "label"

userParams = {
    'featureCols': json.dumps(colNames),
    'labelCol': label,
    'batch_size': 16,
    'num_epochs': 1
}

tensorFlowBatchOp = TensorFlowBatchOp() \
    .setUserFiles(["https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_batch.py"]) \
    .setMainScriptFile("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_batch.py") \
    .setUserParams(json.dumps(userParams)) \
    .setOutputSchemaStr("model_id long, model_info string") \
    .linkFrom(source)
tensorFlowBatchOp.print()
```

### Java 代码
```java
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TensorFlowBatchOp;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TensorFlowBatchOpTest {

	@Test
	public void testTensorFlowBatchOp() throws Exception {
		BatchOperator <?> source = new RandomTableSourceBatchOp()
			.setNumRows(100L)
			.setNumCols(10);

		String[] colNames = source.getColNames();
		source = source.select("*, case when RAND() > 0.5 then 1. else 0. end as label");
		String label = "label";

		Map <String, Object> userParams = new HashMap <>();
		userParams.put("featureCols", JsonConverter.toJson(colNames));
		userParams.put("labelCol", label);
		userParams.put("batch_size", 16);
		userParams.put("num_epochs", 1);

		TensorFlowBatchOp tensorFlowBatchOp = new TensorFlowBatchOp()
			.setUserFiles(new String[] {"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_batch.py"})
			.setMainScriptFile("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_batch.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.setOutputSchemaStr("model_id long, model_info string")
			.linkFrom(source);
		tensorFlowBatchOp.print();
	}
}
```
