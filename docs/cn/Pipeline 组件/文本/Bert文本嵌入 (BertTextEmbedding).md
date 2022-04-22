# Bert文本嵌入 (BertTextEmbedding)
Java 类名：com.alibaba.alink.pipeline.nlp.BertTextEmbedding

Python 类名：BertTextEmbedding


## 功能介绍

把文本输入到 BERT 模型，提取某一编码层的 pooled output 作为该句子的 embedding 结果。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| bertModelName | BERT模型名字 | BERT模型名字： Base-Chinese,Base-Multilingual-Cased,Base-Uncased,Base-Cased | String |  |  | "Base-Chinese" |
| doLowerCase | 是否将文本转换为小写 | 是否将文本转换为小写，默认根据模型自动决定 | Boolean |  |  | null |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  |  | 4 |
| layer | 输出第几层 encoder layer 的结果 | 输出第几层 encoder layer 的结果， -1 表示最后一层，-2 表示倒数第2层，以此类推 | Integer |  |  | -1 |
| maxSeqLength | 句子截断长度 | 句子截断长度 | Integer |  |  | 128 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    [1, 'An english sentence.'],
    [2, '这是一个中文句子']
])

batch_data = BatchOperator.fromDataframe(df_data, schemaStr='f1 bigint, f2 string')

BertTextEmbedding() \
    .setSelectedCol("f2") \
    .setOutputCol("embedding") \
    .setLayer(-2) \
    .transform(batch_data) \
    .print()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.pipeline.nlp.BertTextEmbedding;
import org.junit.Test;

public class BertTextEmbeddingTest {

	@Test
	public void testBertTextEmbedding() throws Exception {
		Row[] rows1 = new Row[] {
			Row.of(1L, "An english sentence."),
			Row.of(2L, "这是一个中文句子"),
		};

		BatchOperator <?> data = BatchOperator.fromTable(
			MLEnvironmentFactory.getDefault().createBatchTable(rows1, new String[] {"f1", "f2"}));

		new BertTextEmbedding()
			.setSelectedCol("f2").setOutputCol("embedding").setLayer(-2)
			.setDoLowerCase(true)
			.setIntraOpParallelism(4)
			.transform(data)
            .print();
	}
}
```

### 运行结果

|f1 |f2 |embedding|
|---|---|---------|
|1|An english sentence.|-0.4501993 0.06074004 0.121287264 -0.27875 0.3...|
|2|这是一个中文句子|-0.8317032 0.32284066 -0.12233654 -0.6955824 0...|
