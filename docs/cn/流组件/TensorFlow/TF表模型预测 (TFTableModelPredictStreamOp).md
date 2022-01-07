# TF表模型预测 (TFTableModelPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.tensorflow.TFTableModelPredictStreamOp

Python 类名：TFTableModelPredictStreamOp


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
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
import json

source = RandomTableSourceBatchOp() \
    .setNumRows(100) \
    .setNumCols(10)

streamSource = RandomTableSourceStreamOp() \
    .setNumCols(10) \
    .setMaxRows(100)

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

tfTableModelPredictStreamOp = TFTableModelPredictStreamOp(tfTableModelTrainBatchOp) \
    .setOutputSchemaStr("logits double") \
    .setOutputSignatureDefs(["logits"]) \
    .setSignatureDefKey("predict") \
    .setSelectedCols(colNames) \
    .linkFrom(streamSource)
tfTableModelPredictStreamOp.print()
StreamOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp;
import com.alibaba.alink.operator.stream.tensorflow.TFTableModelPredictStreamOp;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TFTableModelPredictStreamOpTest {

	@Test
	public void testTFTableModelPredictStreamOp() throws Exception {
		BatchOperator <?> source = new RandomTableSourceBatchOp()
			.setNumRows(100L)
			.setNumCols(10);

		String[] colNames = source.getColNames();
		source = source.select("*, case when RAND() > 0.5 then 1. else 0. end as label");
		String label = "label";

		StreamOperator<?> streamSource = new RandomTableSourceStreamOp()
			.setNumCols(10)
			.setMaxRows(100L);

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

		TFTableModelPredictStreamOp tfTableModelPredictStreamOp = new TFTableModelPredictStreamOp(tfTableModelTrainBatchOp)
			.setOutputSchemaStr("logits double")
			.setOutputSignatureDefs(new String[] {"logits"})
			.setSignatureDefKey("predict")
			.setSelectedCols(colNames)
			.linkFrom(streamSource);
		tfTableModelPredictStreamOp.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
num|col0|col1|col2|col3|col4|col5|col6|col7|col8|col9|logits
---|----|----|----|----|----|----|----|----|----|----|------
52|0.8289|0.0595|0.8372|0.4365|0.5137|0.3043|0.6373|0.7164|0.3754|0.2490|-0.0958
34|0.0506|0.1309|0.0579|0.4603|0.4680|0.2531|0.7893|0.7719|0.3453|0.7246|-0.1723
23|0.1034|0.4412|0.5226|0.1031|0.5974|0.7483|0.3918|0.8350|0.4634|0.4486|-0.0420
60|0.7367|0.6767|0.8048|0.0243|0.4491|0.0166|0.2471|0.0429|0.1482|0.7834|-0.0458
35|0.5111|0.4983|0.3353|0.3196|0.8428|0.0538|0.8995|0.7321|0.5583|0.2186|-0.1468
...|...|...|...|...|...|...|...|...|...|...|...|...
