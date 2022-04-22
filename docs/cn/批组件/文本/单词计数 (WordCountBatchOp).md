# 单词计数 (WordCountBatchOp)
Java 类名：com.alibaba.alink.operator.batch.nlp.WordCountBatchOp

Python 类名：WordCountBatchOp


## 功能介绍

输出文本列所有词语和对应频数。

### 使用方式
文本内容列（SelectedCol）中的内容用于统计词频，需要是用分隔符分隔的词语。
其中，分隔符可以通过参数 wordDelimiter 来设置，默认是空格（" "），可以为正则表达式。
文本内容列可以使用分词（SegmentBatchOp）组件的输出结果列，同时也可以在之前接入停用词过滤（StopWordsRemoverBatchOp）组件去掉常见的高频词。

需要注意的是，该组件统计的是文本内容列总体的词频，而文本词频统计组件（DocWordCountBatchOp）是按行统计词频。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  |  | " " |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["doc0", "中国 的 文化"],
    ["doc1", "只要 功夫 深"],
    ["doc2", "北京 的 拆迁"],
    ["doc3", "人名 的 名义"]
])

source = BatchOperator.fromDataframe(df, "id string, content string")
wordCountBatchOp = WordCountBatchOp()\
    .setSelectedCol("content")\
    .setWordDelimiter(" ")\
    .linkFrom(source)
wordCountBatchOp.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.WordCountBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class WordCountBatchOpTest {
	@Test
	public void testWordCountBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("doc0", "中国 的 文化"),
			Row.of("doc1", "只要 功夫 深"),
			Row.of("doc2", "北京 的 拆迁"),
			Row.of("doc3", "人名 的 名义")
		);
		BatchOperator <?> source = new MemSourceBatchOp(df, "id string, content string");
		BatchOperator <?> wordCountBatchOp = new WordCountBatchOp()
			.setSelectedCol("content")
			.setWordDelimiter(" ")
			.linkFrom(source);
		wordCountBatchOp.print();
	}
}
```

### 运行结果
| word | cnt |
|------|-----|
| 深    | 1   |
| 拆迁   | 1   |
| 北京   | 1   |
| 文化   | 1   |
| 中国   | 1   |
| 的    | 3   |
| 人名   | 1   |
| 只要   | 1   |
| 名义   | 1   |
| 功夫   | 1   |
