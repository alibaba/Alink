# IndexToString预测 (IndexToStringPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.IndexToStringPredictBatchOp

Python 类名：IndexToStringPredictBatchOp


## 功能介绍
基于 StringIndexer 模型，将一列整数映射为字符串。

在批式预测中，IndexToStringPredictBatchOp 接收两个BatchOp的输入，
第一个输入为模型（StringIndexer的getModelData()获取，或者直接输入StringIndexerTrainBatchOp），
第二个输入为要预测的数据。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| modelName | 模型名字 | 模型名字 | String | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ["football"],
    ["football"],
    ["football"],
    ["basketball"],
    ["basketball"],
    ["tennis"],
])

train_data = BatchOperator.fromDataframe(df_data, schemaStr='f0 string')

stringIndexer = StringIndexer()\
    .setModelName("string_indexer_model")\
    .setSelectedCol("f0")\
    .setOutputCol("f0_indexed")\
    .setStringOrderType("frequency_asc").fit(train_data)

indexed = stringIndexer.transform(train_data)

indexToStrings = IndexToStringPredictBatchOp()\
    .setSelectedCol("f0_indexed")\
    .setOutputCol("f0_indxed_unindexed")

indexToStrings.linkFrom(stringIndexer.getModelData(), indexed).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.IndexToStringPredictBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.StringIndexer;
import com.alibaba.alink.pipeline.dataproc.StringIndexerModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IndexToStringPredictStreamOpTest {

	@Test
	public void testIndexToStringPredictStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("football"),
			Row.of("football"),
			Row.of("football"),
			Row.of("basketball"),
			Row.of("basketball"),
			Row.of("tennis")
		);
		BatchOperator <?> train_data = new MemSourceBatchOp(df_data, "f0 string");
		StringIndexerModel stringIndexer = new StringIndexer()
			.setModelName("string_indexer_model")
			.setSelectedCol("f0")
			.setOutputCol("f0_indexed")
			.setStringOrderType("frequency_asc").fit(train_data);
		BatchOperator indexed = stringIndexer.transform(train_data);
		BatchOperator <?> indexToStrings = new IndexToStringPredictBatchOp()
			.setSelectedCol("f0_indexed")
			.setOutputCol("f0_indxed_unindexed");
		indexToStrings.linkFrom(stringIndexer.getModelData(), indexed).print();
	}
}
```

### 运行结果


f0|f0_indexed|f0_indxed_unindexed
---|----------|-------------------
football|2|football
football|2|football
football|2|football
basketball|1|basketball
basketball|1|basketball
tennis|0|tennis
