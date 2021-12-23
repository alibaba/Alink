# 文本分解 (Tokenizer)
Java 类名：com.alibaba.alink.pipeline.nlp.Tokenizer

Python 类名：Tokenizer


## 功能介绍

Tokenizer(标记器)是将文本（如句子）分解成单个词语（通常是单词）的过程。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0, 'That is an English Book!'],
    [1, 'Do you like math?'],
    [2, 'Have a good day!']
])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='id long, text string')

op = Tokenizer().setSelectedCol("text")
op.transform(inOp1).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.nlp.Tokenizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TokenizerTest {
	@Test
	public void testTokenizer() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "That is an English Book!"),
			Row.of(1, "Do you like math?"),
			Row.of(2, "Have a good day!")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "id int, text string");
		Tokenizer op = new Tokenizer().setSelectedCol("text");
		op.transform(inOp1).print();
	}
}
```

### 运行结果
id|text
---|----
0|that is an english book!
1|do you like math?
2|have a good day!
