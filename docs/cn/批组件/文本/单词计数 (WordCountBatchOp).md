# 单词计数 (WordCountBatchOp)
Java 类名：com.alibaba.alink.operator.batch.nlp.WordCountBatchOp

Python 类名：WordCountBatchOp


## 功能介绍

输出文章列所有词以及对应的词频，其中词由文章列内容根据分隔符划分产生。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  | " " |

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
word|cnt
----|---
深|1
拆迁|1
北京|1
文化|1
中国|1
的|3
人名|1
只要|1
名义|1
功夫|1
