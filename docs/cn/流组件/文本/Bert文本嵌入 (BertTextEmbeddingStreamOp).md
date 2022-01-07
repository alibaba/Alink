# Bert文本嵌入 (BertTextEmbeddingStreamOp)
Java 类名：com.alibaba.alink.operator.stream.nlp.BertTextEmbeddingStreamOp

Python 类名：BertTextEmbeddingStreamOp


## 功能介绍

把文本输入到 BERT 模型，提取某一编码层的 pooled output 作为该句子的 embedding 结果。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| bertModelName | BERT模型名字 | BERT模型名字： Base-Chinese,Base-Multilingual-Cased,Base-Uncased,Base-Cased | String |  | "Base-Chinese" |
| doLowerCase | 是否将文本转换为小写 | 是否将文本转换为小写，默认根据模型自动决定 | Boolean |  | null |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  | 4 |
| layer | 输出第几层 encoder layer 的结果 | 输出第几层 encoder layer 的结果， -1 表示最后一层，-2 表示倒数第2层，以此类推 | Integer |  | -1 |
| maxSeqLength | 句子截断长度 | 句子截断长度 | Integer |  | 128 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


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

stream_data = StreamOperator.fromDataframe(df_data, schemaStr='f1 bigint, f2 string')

BertTextEmbeddingStreamOp() \
    .setSelectedCol("f2") \
    .setOutputCol("embedding") \
    .setLayer(-2) \
    .linkFrom(stream_data) \
    .print()

StreamOperator.execute()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.BertTextEmbeddingStreamOp;
import org.junit.Test;

public class BertTextEmbeddingStreamOpTest {
	@Test
	public void testBertTextEmbeddingStreamOp() throws Exception {
		Row[] rows1 = new Row[] {
			Row.of(1L, "An english sentence."),
			Row.of(2L, "这是一个中文句子"),
		};

		StreamOperator <?> data = StreamOperator.fromTable(
			MLEnvironmentFactory.getDefault().createStreamTable(rows1, new String[] {"sentence_id", "sentence_text"}));

		BertTextEmbeddingStreamOp bertEmb = new BertTextEmbeddingStreamOp()
			.setSelectedCol("sentence_text").setOutputCol("embedding").setLayer(-2);
		data.link(bertEmb).print();

		StreamOperator.execute();
	}
}
```

### 运行结果

|f1 |f2 |embedding|
|---|---|---------|
|1|An english sentence.|-0.4501993 0.06074004 0.121287264 -0.27875 0.3...|
|2|这是一个中文句子|-0.8317032 0.32284066 -0.12233654 -0.6955824 0...|
