# TF2表模型 (TF2TableModelTrainer)
Java 类名：com.alibaba.alink.pipeline.tensorflow.TF2TableModelTrainer

Python 类名：TF2TableModelTrainer


## 功能介绍

该组件支持用户传入 TensorFlow2 脚本，进行模型训练。

用户需要提供自己编写的 TensorFlow2 脚本文件。
脚本的编写需要依赖 akdl 库，可以参考 ```alink_dl_predictors/predictor-tf/src/test/resources/tf_dnn_train.py```。

脚本中必须要将模型保存为 SavedModel 格式，并导出到指定的目录下 (```TrainTaskConfig#saved_model_dir```)。

调用这个组件的 fit 方法可以得到一个 ```TFTableModelPredictor``` 进行预测。
需要注意的是：参与预测的列名一般与参与训练的列名不同（预测没有 label 列），需要通过参数 ```inferSelectedCols``` 来指定参与预测的列名。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| mainScriptFile | 主脚本文件路径 | 主脚本文件路径，需要是参数 userFiles 中的一项，并且包含 main 函数 | String | ✓ |  |  |
| outputSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如 "f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| userFiles | 所有自定义脚本文件的路径 | 所有自定义脚本文件的路径 | String | ✓ |  |  |
| graphDefTag | graph标签 | graph标签 | String |  |  | "serve" |
| inferSelectedCols | 用于推理的列名数组 | 用于推理的列名列表 | String[] |  |  | null |
| inputSignatureDefs | 输入 SignatureDef | SavedModel 模型的输入 SignatureDef 名，用逗号分隔，需要与输入列一一对应，默认与选择列相同 | String[] |  |  | null |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  |  | 4 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  |  | null |
| outputSignatureDefs | TF 输出 SignatureDef 名 | 模型的输出 SignatureDef 名，多个输出时用逗号分隔，并且与输出 Schema 一一对应，默认与输出 Schema 中的列名相同 | String[] |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；如果是目录，那么只能使用本地路径，即 file://。 | String |  |  | "" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  |  | null |
| signatureDefKey | signature标签 | signature标签 | String |  |  | "serving_default" |
| userParams | 自定义参数 | 用户自定义参数，JSON 字典格式的字符串 | String |  |  | "{}" |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |


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

trainer = TF2TableModelTrainer() \
    .setUserFiles(["https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_train.py"]) \
    .setMainScriptFile("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_train.py") \
    .setUserParams(json.dumps(userParams)) \
    .setOutputSchemaStr("logits double") \
    .setOutputSignatureDefs(["logits"]) \
    .setSignatureDefKey("predict") \
    .setInferSelectedCols(colNames)
model = trainer.fit(source)
model.transform(source).print()
```

### Java 代码
```java
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.pipeline.tensorflow.TF2TableModelTrainer;
import com.alibaba.alink.pipeline.tensorflow.TFTableModelPredictor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TF2TableModelTrainerTest {

	@Test
	public void testTF2TableModelTrainer() throws Exception {
		BatchOperator.setParallelism(3);

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

		TF2TableModelTrainer trainer = new TF2TableModelTrainer()
			.setUserFiles(new String[] {"res:///tf_dnn_train.py"})
			.setMainScriptFile("res:///tf_dnn_train.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.setNumWorkers(2)
			.setNumPSs(1)
			.setOutputSchemaStr("logits double")
			.setOutputSignatureDefs(new String[]{"logits"})
			.setSignatureDefKey("predict")
			.setInferSelectedCols(colNames);

		TFTableModelPredictor model = trainer.fit(source);
		model.transform(source).print();
	}
}
```
