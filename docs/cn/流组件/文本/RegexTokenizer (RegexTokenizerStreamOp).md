# RegexTokenizer (RegexTokenizerStreamOp)
Java 类名：com.alibaba.alink.operator.stream.nlp.RegexTokenizerStreamOp

Python 类名：RegexTokenizerStreamOp


## 功能介绍

RegexTokenizer支持对文本的切分和匹配操作。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| pattern | 分隔符/正则匹配符 | 如果gaps为True，pattern用于切分文档；如果gaps为False，会提取出匹配pattern的词。 | String |  | "\\s+" |
| gaps | 切分/匹配 | 如果gaps为True，pattern用于切分文档；如果gaps为False，会提取出匹配pattern的词。 | Boolean |  | true |
| minTokenLength | 词语最短长度 | 词语的最短长度，小于这个值的词语会被过滤掉 | Integer |  | 1 |
| toLowerCase | 是否转换为小写 | 转换为小写 | Boolean |  | true |
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
op = RegexTokenizerBatchOp().setSelectedCol("text").setGaps(False).setToLowerCase(True).setOutputCol("token").setPattern("\\w+")

op.linkFrom(inOp1).print()

inOp2 = StreamOperator.fromDataframe(df, schemaStr='id long, text string')
op2 = RegexTokenizerStreamOp().setSelectedCol("text").setGaps(False).setToLowerCase(True).setOutputCol("token").setPattern("\\w+")
op2.linkFrom(inOp2).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.RegexTokenizerBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.RegexTokenizerStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RegexTokenizerStreamOpTest {
	@Test
	public void testRegexTokenizerStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "That is an English Book!"),
			Row.of(1, "Do you like math?"),
			Row.of(2, "Have a good day!")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "id int, text string");
		BatchOperator <?> op = new RegexTokenizerBatchOp().setSelectedCol("text").setGaps(false).setToLowerCase(true)
			.setOutputCol("token").setPattern("\\w+");
		op.linkFrom(inOp1).print();
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "id int, text string");
		StreamOperator <?> op2 =
			new RegexTokenizerStreamOp().setSelectedCol("text").setGaps(false).setToLowerCase(true)
			.setOutputCol("token").setPattern("\\w+");
		op2.linkFrom(inOp2).print();
		StreamOperator.execute();
	}
}
```

### 运行结果
#### 批运行结果
id|text|token
---|----|-----
0|That is an English Book!|that is an english book
1|Do you like math?|do you like math
2|Have a good day!|have a good day

#### 流运行结果
id|text|token
---|----|-----
0|That is an English Book!|that is an english book
2|Have a good day!|have a good day
1|Do you like math?|do you like math
