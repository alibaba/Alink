# TF表模型预测 (TFTableModelPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.tensorflow.TFTableModelPredictBatchOp

Python 类名：TFTableModelPredictBatchOp


## 功能介绍

使用 `TFTableModelTrainBatchOp` 或者 `TF2TableModelTrainBatchOp` 训练产生的模型进行预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如 "f0 string, f1 bigint, f2 double" | String | ✓ |  |
| graphDefTag | graph标签 | graph标签 | String |  | "serve" |
| inputSignatureDefs | 输入 SignatureDef | SavedModel 模型的输入 SignatureDef 名，用逗号分隔，需要与输入列一一对应，默认与选择列相同 | String[] |  | null |
| outputSignatureDefs | TF 输出 SignatureDef 名 | 模型的输出 SignatureDef 名，多个输出时用逗号分隔，并且与输出 Schema 一一对应，默认与输出 Schema 中的列名相同 | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | null |
| signatureDefKey | signature标签 | signature标签 | String |  | "serving_default" |

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

tfTableModelTrainBatchOp = TFTableModelTrainBatchOp() \
    .setUserFiles(["https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_train.py"]) \
    .setMainScriptFile("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_train.py") \
    .setUserParams(json.dumps(userParams)) \
    .linkFrom(source)

tfTableModelPredictBatchOp = TFTableModelPredictBatchOp() \
    .setOutputSchemaStr("logits double") \
    .setOutputSignatureDefs(["logits"]) \
    .setSignatureDefKey("predict") \
    .setSelectedCols(colNames) \
    .linkFrom(tfTableModelTrainBatchOp, source)
tfTableModelPredictBatchOp.print()
```

### Java 代码
```java
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelPredictBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TFTableModelPredictBatchOpTest {
	@Test
	public void testTFTableModelPredictBatchOp() throws Exception {
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

		TFTableModelTrainBatchOp tfTableModelTrainBatchOp = new TFTableModelTrainBatchOp()
			.setUserFiles(new String[] {"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_train.py"})
			.setMainScriptFile("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_train.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.linkFrom(source);

		TFTableModelPredictBatchOp tfTableModelPredictBatchOp = new TFTableModelPredictBatchOp()
			.setOutputSchemaStr("logits double")
			.setOutputSignatureDefs(new String[]{"logits"})
			.setSignatureDefKey("predict")
			.setSelectedCols(colNames)
			.linkFrom(tfTableModelTrainBatchOp, source);
		tfTableModelPredictBatchOp.print();
	}
}
```

### 运行结果
col0|col1|col2|col3|col4|col5|col6|col7|col8|col9|label|logits
----|----|----|----|----|----|----|----|----|----|-----|------
0.7310|0.2405|0.6374|0.5504|0.5975|0.3332|0.3852|0.9848|0.8792|0.9412|0|-0.4253
0.2750|0.1289|0.1466|0.0232|0.5467|0.9645|0.1045|0.6251|0.4108|0.7763|0|-0.4099
0.9907|0.4872|0.7462|0.7332|0.8173|0.8389|0.5267|0.8993|0.1339|0.0831|0|-0.3881
0.9786|0.7224|0.7150|0.1432|0.4630|0.0045|0.0715|0.3484|0.3388|0.8594|0|-0.3044
0.9715|0.8657|0.6126|0.1790|0.2176|0.8545|0.0097|0.6923|0.7713|0.7127|0|-0.4693
...|...|...|...|...|...|...|...|...|...|...|...
