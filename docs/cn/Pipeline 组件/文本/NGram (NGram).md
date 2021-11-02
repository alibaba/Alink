# NGram (NGram)
Java 类名：com.alibaba.alink.pipeline.nlp.NGram

Python 类名：NGram


## 功能介绍

本组件对于每行文本生成它的NGram存储。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| n | nGram长度 | nGram长度 | Integer |  | 2 |
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

op = NGram().setSelectedCol("text")
op.transform(inOp1).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.nlp.NGram;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NGramTest {
	@Test
	public void testNGram() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "That is an English Book!"),
			Row.of(1, "Do you like math?"),
			Row.of(2, "Have a good day!")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "id int, text string");
		NGram op = new NGram().setSelectedCol("text");
		op.transform(inOp1).print();
	}
}
```

### 运行结果
id|text
---|----
0|That_is is_an an_English English_Book!
1|Do_you you_like like_math?
2|Have_a a_good good_day!
