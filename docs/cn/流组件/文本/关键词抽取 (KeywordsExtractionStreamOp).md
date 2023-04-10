# 关键词抽取 (KeywordsExtractionStreamOp)
Java 类名：com.alibaba.alink.operator.stream.nlp.KeywordsExtractionStreamOp

Python 类名：KeywordsExtractionStreamOp


## 功能介绍

从每行文本中抽取与文本意义最相关的若干词语。

### 算法原理

流式关键词提取基于 TextRank 算法。 
TextRank 受到网页间关系的 PageRank 算法启发，利用局部词汇之间关系（共现窗口）构建图，计算词的重要性，选取权重大的作为关键词。

在构建的图中，每个词语对应一个节点 $V_{.}$。 两个不同的词语 $i, j$ 只要在同一个窗口中共同出现过，对应节点间就存在两条有向边 $e_{ij}$ 和 $e_{ji}$， 权重分别为 1，即 $w_{ij} = w_{ji} =
1$。

每个节点初始重要性值 $WS(V_{*}) = \frac{1}{|Out(V_{*})|}$，并按照下面公式进行迭代更新直至收敛：

$$WS(V_i) = (1 - d) + d * \Sigma_{V_j\in In(V_i)} \frac{w_{ji}}{\Sigma_{V_k\in Out(V_i)}w_{jk} }WS(V_j).$$

其中，d 是阻尼系数。

### 使用方式

文本列通过参数 selectedCol 指定，需要是空格分隔的词语。
文本列可以使用分词（SegmentStreamOp）组件的输出结果列，同时也可以在之前接入停用词过滤（StopWordsRemoverStreamOp）组件去掉常见的高频词。

基于 TextRank 的方法，需要设置窗口大小 windowSize、最大迭代步数 maxIter、收敛阈值 epsilon 和阻尼稀疏 dampingFactor。

### 文献索引

TextRank：[https://web.eecs.umich.edu/~mihalcea/papers/mihalcea.emnlp04.pdf](https://web.eecs.umich.edu/~mihalcea/papers/mihalcea.emnlp04.pdf)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| dampingFactor | 阻尼系数 | 阻尼系数 | Double |  |  | 0.85 |
| epsilon | 收敛阈值 | 收敛阈值 | Double |  |  | 1.0E-6 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | x >= 1 | 100 |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| topN | 前N的数据 | 挑选最近的N个数据 | Integer |  | x >= 1 | 10 |
| windowSize | 窗口大小 | 窗口大小 | Integer |  | x >= 1 | 2 |

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

segment = SegmentBatchOp().setSelectedCol("text").linkFrom(inOp1)
remover = StopWordsRemoverBatchOp().setSelectedCol("text").linkFrom(segment)
keywords = KeywordsExtractionBatchOp().setSelectedCol("text").setMethod("TF_IDF").setTopN(3).linkFrom(remover)
keywords.print()

segment2 = SegmentStreamOp().setSelectedCol("text").linkFrom(inOp2)
remover2 = StopWordsRemoverStreamOp().setSelectedCol("text").linkFrom(segment2)
keywords2 = KeywordsExtractionStreamOp().setSelectedCol("text").setTopN(3).linkFrom(remover2)
keywords2.print()
StreamOperator.execute()
```

### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.KeywordsExtractionBatchOp;
import com.alibaba.alink.operator.batch.nlp.SegmentBatchOp;
import com.alibaba.alink.operator.batch.nlp.StopWordsRemoverBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.KeywordsExtractionStreamOp;
import com.alibaba.alink.operator.stream.nlp.SegmentStreamOp;
import com.alibaba.alink.operator.stream.nlp.StopWordsRemoverStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KeywordsExtractionStreamOpTest {
	@Test
	public void testKeywordsExtractionStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "二手旧书:医学电磁成像"),
			Row.of(1, "二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969"),
			Row.of(2, "二手正版图解象棋入门/谢恩思主编/华龄出版社"),
			Row.of(3, "二手中国糖尿病文献索引"),
			Row.of(4, "二手郁达夫文集（ 国内版 ）全十二册馆藏书")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "id int, text string");
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "id int, text string");
		BatchOperator <?> segment = new SegmentBatchOp().setSelectedCol("text").linkFrom(inOp1);
		BatchOperator <?> remover = new StopWordsRemoverBatchOp().setSelectedCol("text").linkFrom(segment);
		BatchOperator <?> keywords =
			new KeywordsExtractionBatchOp().setSelectedCol("text").setMethod("TF_IDF").setTopN(
				3).linkFrom(remover);
		keywords.print();
		StreamOperator <?> segment2 = new SegmentStreamOp().setSelectedCol("text").linkFrom(inOp2);
		StreamOperator <?> remover2 = new StopWordsRemoverStreamOp().setSelectedCol("text").linkFrom(segment2);
		StreamOperator <?> keywords2 = new KeywordsExtractionStreamOp().setSelectedCol("text").setTopN(3).linkFrom(
			remover2);
		keywords2.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

#### 批运行结果

| text     | id  |
|----------|-----|
| 中国 索引 文献 | 3   |
| 电磁 成像 旧书 | 0   |
| 下册 选读 文学 | 1   |
| 图解 正版 入门 | 2   |
| 全 文集 版   | 4   |

#### 流运行结果

| id  | text      |
|-----|-----------|
| 0   | 旧书 电磁 医学  |
| 4   | 郁达夫 馆藏 文集 |
| 1   | 美国 出版社 文学 |
| 3   | 中国 文献 糖尿病 |
| 2   | 正版 华龄 图解  |
