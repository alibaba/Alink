# TF2表模型训练 (TF2TableModelTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.tensorflow.TF2TableModelTrainBatchOp

Python 类名：TF2TableModelTrainBatchOp


## 功能介绍

该组件支持用户传入 TensorFlow2 脚本，进行模型训练。

用户需要提供自己编写的 TensorFlow2 脚本文件。
脚本的编写需要依赖 akdl 库，可以参考 ```alink_dl_predictors/predictor-tf/src/test/resources/tf_dnn_train.py```。

脚本中必须要将模型保存为 SavedModel 格式，并导出到指定的目录下 (```TrainTaskConfig#saved_model_dir```)。

这个组件的输出可以接入 ```TFTableModelPredictBatchOp/StreamOp``` 进行预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| mainScriptFile | 主脚本文件路径 | 主脚本文件路径，需要是参数 userFiles 中的一项，并且包含 main 函数 | String | ✓ |  |
| userFiles | 所有自定义脚本文件的路径 | 所有自定义脚本文件的路径 | String | ✓ |  |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  | 4 |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  | null |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；如果是目录，那么只能使用本地路径，即 file://。 | String |  | "" |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | null |
| userParams | 自定义参数 | 用户自定义参数，JSON 字典格式的字符串 | String |  | "{}" |


### 脚本路径说明

脚本路径可以是以下形式：
- 本地文件：```file://``` 加绝对路径，例如 ```file:///tmp/dnn.py```；
- Java 包中的资源文件：```res://``` 加路径，例如 ```res:///dnn.py```；
- http/https 文件：```http://``` 或 ```https://``` 路径；
- OSS 文件：```oss://``` 加路径和 Endpoint 和 access key 等信息，例如```oss://bucket/xxx/xxx/xxx.py?host=xxx&access_key_id=xxx&access_key_secret=xxx```；
- HDFS 文件：```hdfs://``` 加路径；

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
import json

source = RandomTableSourceBatchOp() \
    .setNumRows(100) \
    .setNumCols(10)

colNames = source.getColNames()
source = source.select("*, case when RAND() > 0.5 then 1. else 0. end as label")
label = "label"

userParams = {
    'featureCols': json.dumps(colNames),
    'labelCol': label,
    'batch_size': 16,
    'num_epochs': 1
}

tf2TableModelTrainBatchOp = TF2TableModelTrainBatchOp() \
    .setUserFiles(["https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_train.py"]) \
    .setMainScriptFile("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_train.py") \
    .setUserParams(json.dumps(userParams)) \
    .linkFrom(source)
tf2TableModelTrainBatchOp.print()
```

### Java 代码
```java
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TFTableModelTrainBatchOpTest {

	@Test
	public void testTFTableModelTrainBatchOp() throws Exception {
		BatchOperator<?> source = new RandomTableSourceBatchOp()
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

		TF2TableModelTrainBatchOp tfTableModelTrainBatchOp = new TF2TableModelTrainBatchOp()
			.setUserFiles(new String[] {"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_train.py"})
			.setMainScriptFile("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_train.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.linkFrom(source);
		tfTableModelTrainBatchOp.print();
	}
}
```
