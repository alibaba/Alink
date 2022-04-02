# TF-IDF (TfidfBatchOp)
Java 类名：com.alibaba.alink.operator.batch.nlp.TfidfBatchOp

Python 类名：TfidfBatchOp


## 功能介绍

计算文本中词语的 TF-IDF 值。

### 算法原理

TF-IDF（term frequency–inverse document frequency）是一种用于资讯检索与文本挖掘的统计方法，用以评估单个词语对于一个文件集的重要程度。

词频（term frequency）$tf(t, d)$ 表示词语 $t$ 在文档 $d$ 中出现的频率：$\frac{f_{t,d}}{\Sigma_{t'\in d}f_{t', d}}$， 其中 $f_{t,d}$表示词语 $t$
在文本 $d$ 中出现的次数。

逆文档评率（inverse document frequency）$idf(t, D)$ 衡量一个词在语料库 $D$（所有文本）中提供的信息量：$\log\frac{|D|}{1 + |{d\in D:t\in D}|}$，
其中分子是所有文本的数量，分母是有词语 $t$ 出现的文本的数量。

最终，文本 $d$ 中的词语 $t$ 在该语料库 $D$ 的 TF-IDF 值就是 $tf(t, d) * idf(t, D)$。

### 使用方式

TF-IDF 加权的各种形式常被搜索引擎应用，作为文件与用户查询之间相关程度的度量或评级。

在 Alink 中使用时，输入数据不需要为原始的文本，而是文本进行词频（DocWordCountBatchOp）统计的结果，记录了在每个文本中各词出现的次数。 组件需要设置文档 ID
列（docIdCol），单词列（wordCol）和词频列（countCol）。

### 文献索引

TF-IDF: [https://en.wikipedia.org/wiki/Tf%E2%80%93idf](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| docIdCol | 文档ID列 | 文档ID列名 | String | ✓ |  |
| wordCol | 单词列 | 单词列名 | String | ✓ |  |
| countCol | 词频列 | 词频列名 | String | ✓ |  |

## 代码示例

### Python 代码

```python
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
tfidf = TfidfBatchOp().setDocIdCol("id").setWordCol("word").setCountCol("cnt").linkFrom(wordCount)
tfidf.print()
```

### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.DocWordCountBatchOp;
import com.alibaba.alink.operator.batch.nlp.SegmentBatchOp;
import com.alibaba.alink.operator.batch.nlp.StopWordsRemoverBatchOp;
import com.alibaba.alink.operator.batch.nlp.TfidfBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TfidfBatchOpTest {
	@Test
	public void testTfidfBatchOp() throws Exception {
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
		BatchOperator <?> tfidf = new TfidfBatchOp().setDocIdCol("id").setWordCol("word").setCountCol("cnt").linkFrom(
			wordCount);
		tfidf.print();
	}
}
```

### 运行结果

| id  | word          | cnt | total_word_count | doc_cnt | total_doc_count | tf     | idf     | tfidf   |
|-----|---------------|-----|------------------|---------|-----------------|--------|---------|---------|
| 0   | 医学            | 1   | 5                | 1       | 5               | 0.2000 | 0.9163  | 0.1833  |
| 0   | 电磁            | 1   | 5                | 1       | 5               | 0.2000 | 0.9163  | 0.1833  |
| 2   | 入门            | 1   | 10               | 1       | 5               | 0.1000 | 0.9163  | 0.0916  |
| 4   | 文集            | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 4   | 版             | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 4   | 十二册           | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 4   | 书             | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 4   | 馆藏            | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 4   | 郁达夫           | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 0   | 成像            | 1   | 5                | 1       | 5               | 0.2000 | 0.9163  | 0.1833  |
| 0   | 旧书            | 1   | 5                | 1       | 5               | 0.2000 | 0.9163  | 0.1833  |
| 0   | 二手            | 1   | 5                | 5       | 5               | 0.2000 | -0.1823 | -0.0365 |
| 1   | 二手            | 1   | 9                | 5       | 5               | 0.1111 | -0.1823 | -0.0203 |
| 1   | 9787310003969 | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 2   | 二手            | 1   | 10               | 5       | 5               | 0.1000 | -0.1823 | -0.0182 |
| 2   | 华龄            | 1   | 10               | 1       | 5               | 0.1000 | 0.9163  | 0.0916  |
| 2   | 思             | 1   | 10               | 1       | 5               | 0.1000 | 0.9163  | 0.0916  |
| 4   | 二手            | 1   | 9                | 5       | 5               | 0.1111 | -0.1823 | -0.0203 |
| 3   | 索引            | 1   | 5                | 1       | 5               | 0.2000 | 0.9163  | 0.1833  |
| 3   | 二手            | 1   | 5                | 5       | 5               | 0.2000 | -0.1823 | -0.0365 |
| 1   | 出版社           | 1   | 9                | 2       | 5               | 0.1111 | 0.5108  | 0.0568  |
| 2   | 出版社           | 1   | 10               | 2       | 5               | 0.1000 | 0.5108  | 0.0511  |
| 2   | 谢恩            | 1   | 10               | 1       | 5               | 0.1000 | 0.9163  | 0.0916  |
| 2   | 象棋            | 1   | 10               | 1       | 5               | 0.1000 | 0.9163  | 0.0916  |
| 3   | 糖尿病           | 1   | 5                | 1       | 5               | 0.2000 | 0.9163  | 0.1833  |
| 1   | 选读            | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 2   | 正版            | 1   | 10               | 1       | 5               | 0.1000 | 0.9163  | 0.0916  |
| 1   | 文学            | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 1   | 南开大学          | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 3   | 中国            | 1   | 5                | 1       | 5               | 0.2000 | 0.9163  | 0.1833  |
| 1   | 下册            | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 2   | 图解            | 1   | 10               | 1       | 5               | 0.1000 | 0.9163  | 0.0916  |
| 4   | 全             | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 1   | 美国            | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 1   | 李宜燮           | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 2   | 主编            | 1   | 10               | 1       | 5               | 0.1000 | 0.9163  | 0.0916  |
| 4   | 国内            | 1   | 9                | 1       | 5               | 0.1111 | 0.9163  | 0.1018  |
| 3   | 文献            | 1   | 5                | 1       | 5               | 0.2000 | 0.9163  | 0.1833  |
