# 文本词频统计 (DocWordCountStreamOp)
Java 类名：com.alibaba.alink.operator.stream.nlp.DocWordCountStreamOp

Python 类名：DocWordCountStreamOp


## 功能介绍

对文本列按行统计词语的出现的频数。

### 使用方式
文本内容列（ContentCol）中的内容用于统计词频，需要是用分隔符分隔的词语。
其中，分隔符可以通过参数 wordDelimiter 来设置，默认是空格（" "），可以为正则表达式。
文本内容列可以使用分词（SegmentStreamOp）组件的输出结果列，同时也可以在之前接入停用词过滤（StopWordsRemoverStreamOp）组件去掉常见的高频词。

文本 ID 列（DocIdCol）用于标识每一行数据，从而可以将输出结果中的词语、频数数据与输入行对应起来。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| contentCol | 文本列 | 文本列名 | String | ✓ | 所选列类型为 [STRING] |  |
| docIdCol | 文档ID列 | 文档ID列名 | String | ✓ |  |  |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  |  | " " |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0, u'二手旧书:医学电磁成像'],
    [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
    [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
    [3, u'二手中国糖尿病文献索引'],
    [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']
])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, text string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, text string')

segment = SegmentBatchOp().setSelectedCol("text").setOutputCol("segment").linkFrom(inOp1)
remover = StopWordsRemoverBatchOp().setSelectedCol("segment").setOutputCol("remover").linkFrom(segment)
wordCount = DocWordCountBatchOp().setContentCol("remover").setDocIdCol("id").linkFrom(remover)
wordCount.print()

segment2 = SegmentStreamOp().setSelectedCol("text").setOutputCol("segment").linkFrom(inOp2)
remover2 = StopWordsRemoverStreamOp().setSelectedCol("segment").setOutputCol("remover").linkFrom(segment2)
wordCount2 = DocWordCountStreamOp().setContentCol("remover").setDocIdCol("id").linkFrom(remover2)
wordCount2.print()
StreamOperator.execute()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.DocWordCountBatchOp;
import com.alibaba.alink.operator.batch.nlp.SegmentBatchOp;
import com.alibaba.alink.operator.batch.nlp.StopWordsRemoverBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.DocWordCountStreamOp;
import com.alibaba.alink.operator.stream.nlp.SegmentStreamOp;
import com.alibaba.alink.operator.stream.nlp.StopWordsRemoverStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DocWordCountStreamOpTest {
	@Test
	public void testDocWordCountStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "二手旧书:医学电磁成像"),
			Row.of(1, "二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969"),
			Row.of(2, "二手正版图解象棋入门/谢恩思主编/华龄出版社"),
			Row.of(3, "二手中国糖尿病文献索引"),
			Row.of(4, "二手郁达夫文集（ 国内版 ）全十二册馆藏书")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "id int, text string");
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "id int, text string");
		BatchOperator <?> segment =
			new SegmentBatchOp().setSelectedCol("text").setOutputCol("segment").linkFrom(inOp1);
		BatchOperator <?> remover = new StopWordsRemoverBatchOp().setSelectedCol("segment").setOutputCol("remover")
			.linkFrom(segment);
		BatchOperator <?> wordCount = new DocWordCountBatchOp().setContentCol("remover").setDocIdCol("id").linkFrom(
			remover);
		wordCount.print();
		StreamOperator <?> segment2 = new SegmentStreamOp().setSelectedCol("text").setOutputCol("segment").linkFrom(
			inOp2);
		StreamOperator <?> remover2 = new StopWordsRemoverStreamOp().setSelectedCol("segment").setOutputCol("remover")
			.linkFrom(segment2);
		StreamOperator <?> wordCount2 = new DocWordCountStreamOp().setContentCol("remover").setDocIdCol("id").linkFrom(
			remover2);
		wordCount2.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
#### 批运行结果
| id  | word          | cnt |
|-----|---------------|-----|
| 0   | 医学            | 1   |
| 0   | 电磁            | 1   |
| 0   | 成像            | 1   |
| 0   | 旧书            | 1   |
| 0   | 二手            | 1   |
| 1   | 美国            | 1   |
| 1   | 出版社           | 1   |
| 1   | 选读            | 1   |
| 1   | 文学            | 1   |
| 1   | 二手            | 1   |
| 1   | 下册            | 1   |
| 1   | 南开大学          | 1   |
| 1   | 9787310003969 | 1   |
| 1   | 李宜燮           | 1   |
| 2   | 出版社           | 1   |
| 2   | 主编            | 1   |
| 2   | 谢恩            | 1   |
| 2   | 二手            | 1   |
| 2   | 正版            | 1   |
| 2   | 入门            | 1   |
| 2   | 象棋            | 1   |
| 2   | 华龄            | 1   |
| 2   | 思             | 1   |
| 2   | 图解            | 1   |
| 3   | 中国            | 1   |
| 3   | 文献            | 1   |
| 3   | 索引            | 1   |
| 3   | 糖尿病           | 1   |
| 3   | 二手            | 1   |
| 4   | 国内            | 1   |
| 4   | 十二册           | 1   |
| 4   | 文集            | 1   |
| 4   | 书             | 1   |
| 4   | 二手            | 1   |
| 4   | 全             | 1   |
| 4   | 版             | 1   |
| 4   | 馆藏            | 1   |
| 4   | 郁达夫           | 1   |

#### 流运行结果
| id  | word          | cnt |
|-----|---------------|-----|
| 2   | 出版社           | 1   |
| 2   | 主编            | 1   |
| 2   | 谢恩            | 1   |
| 2   | 二手            | 1   |
| 2   | 正版            | 1   |
| 2   | 入门            | 1   |
| 2   | 象棋            | 1   |
| 2   | 华龄            | 1   |
| 2   | 思             | 1   |
| 2   | 图解            | 1   |
| 0   | 医学            | 1   |
| 0   | 电磁            | 1   |
| 0   | 成像            | 1   |
| 0   | 旧书            | 1   |
| 0   | 二手            | 1   |
| 4   | 国内            | 1   |
| 4   | 十二册           | 1   |
| 4   | 文集            | 1   |
| 4   | 书             | 1   |
| 4   | 二手            | 1   |
| 4   | 全             | 1   |
| 4   | 版             | 1   |
| 4   | 馆藏            | 1   |
| 4   | 郁达夫           | 1   |
| 3   | 中国            | 1   |
| 3   | 文献            | 1   |
| 3   | 索引            | 1   |
| 3   | 糖尿病           | 1   |
| 3   | 二手            | 1   |
| 1   | 美国            | 1   |
| 1   | 出版社           | 1   |
| 1   | 选读            | 1   |
| 1   | 文学            | 1   |
| 1   | 二手            | 1   |
| 1   | 下册            | 1   |
| 1   | 南开大学          | 1   |
| 1   | 9787310003969 | 1   |
| 1   | 李宜燮           | 1   |
