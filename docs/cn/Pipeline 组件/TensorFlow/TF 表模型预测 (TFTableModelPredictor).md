# TF 表模型预测 (TFTableModelPredictor)
Java 类名：com.alibaba.alink.pipeline.tensorflow.TFTableModelPredictor

Python 类名：TFTableModelPredictor


## 功能介绍

由 `TFTableModelTrainer` 或者 `TF2TableModelTrainer` 调用 fit 方法产生的模型，可以进行预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如 "f0 string, f1 bigint, f2 double" | String | ✓ |  |
| graphDefTag | graph标签 | graph标签 | String |  | "serve" |
| inputSignatureDefs | 输入 SignatureDef | SavedModel 模型的输入 SignatureDef 名，用逗号分隔，需要与输入列一一对应，默认与选择列相同 | String[] |  | null |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  | 4 |
| outputSignatureDefs | TF 输出 SignatureDef 名 | 模型的输出 SignatureDef 名，多个输出时用逗号分隔，并且与输出 Schema 一一对应，默认与输出 Schema 中的列名相同 | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | null |
| signatureDefKey | signature标签 | signature标签 | String |  | "serving_default" |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |


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
