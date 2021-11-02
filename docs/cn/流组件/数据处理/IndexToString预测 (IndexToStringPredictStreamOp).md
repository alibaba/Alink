# IndexToString预测 (IndexToStringPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.IndexToStringPredictStreamOp

Python 类名：IndexToStringPredictStreamOp


## 功能介绍
基于StringIndexer模型，将一列整数映射为字符串。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| modelName | 模型名字 | 模型名字 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |

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
data = StreamOperator.fromDataframe(df_data, schemaStr='f0 string')

stringIndexer = StringIndexer() \
    .setModelName("string_indexer_model") \
    .setSelectedCol("f0") \
    .setOutputCol("f0_indexed") \
    .setStringOrderType("frequency_asc").fit(train_data)

batch_model = stringIndexer.transform(train_data)
indexed = stringIndexer.transform(data)

indexToStrings = IndexToStringPredictStreamOp(batch_model) \
    .setSelectedCol("f0_indexed") \
    .setOutputCol("f0_indxed_unindexed")

indexToStrings.linkFrom(indexed).print()
StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.IndexToStringPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
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
		StreamOperator <?> data = new MemSourceStreamOp(df_data, "f0 string");
		StringIndexerModel stringIndexer = new StringIndexer()
			.setModelName("string_indexer_model")
			.setSelectedCol("f0")
			.setOutputCol("f0_indexed")
			.setStringOrderType("frequency_asc").fit(train_data);
		BatchOperator batch_model = stringIndexer.transform(train_data);
		StreamOperator indexed = stringIndexer.transform(data);
		StreamOperator <?> indexToStrings = new IndexToStringPredictStreamOp(batch_model)
			.setSelectedCol("f0_indexed")
			.setOutputCol("f0_indxed_unindexed");
		indexToStrings.linkFrom(indexed).print();
		StreamOperator.execute();
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
