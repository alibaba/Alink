# 文本特征生成预测 (DocCountVectorizerPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.nlp.DocCountVectorizerPredictStreamOp

Python 类名：DocCountVectorizerPredictStreamOp


## 功能介绍

根据文本中词语的特征信息，将每条文本转化为稀疏向量。

### 使用方式

该组件是预测组件，需要配合训练组件 DocCountVectorizerTrainBatchOp 使用。

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
segment = SegmentBatchOp().setSelectedCol("text").linkFrom(inOp1)
train = DocCountVectorizerTrainBatchOp().setSelectedCol("text").linkFrom(segment)
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, text string')
segment2 = SegmentStreamOp().setSelectedCol("text").linkFrom(inOp2)
predictStream = DocCountVectorizerPredictStreamOp(train).setSelectedCol("text").linkFrom(segment2)
predictStream.print()
StreamOperator.execute()
```

### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.DocCountVectorizerPredictBatchOp;
import com.alibaba.alink.operator.batch.nlp.DocCountVectorizerTrainBatchOp;
import com.alibaba.alink.operator.batch.nlp.SegmentBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DocCountVectorizerPredictStreamOpTest {
	@Test
	public void testDocCountVectorizerPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "二手旧书:医学电磁成像"),
			Row.of(1, "二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969"),
			Row.of(2, "二手正版图解象棋入门/谢恩思主编/华龄出版社"),
			Row.of(3, "二手中国糖尿病文献索引"),
			Row.of(4, "二手郁达夫文集（ 国内版 ）全十二册馆藏书")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "id int, text string");
		BatchOperator <?> segment = new SegmentBatchOp().setSelectedCol("text").linkFrom(inOp1);
		BatchOperator <?> train = new DocCountVectorizerTrainBatchOp().setSelectedCol("text").linkFrom(segment);
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "id int, text string");
		StreamOperator <?> segment2 = new SegmentStreamOp().setSelectedCol("text").linkFrom(inOp2);
		StreamOperator <?> predictStream = new DocCountVectorizerPredictStreamOp(train).setSelectedCol("text").linkFrom(
			segment2);
		predictStream.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

| id  | text                                                                           |
|-----|--------------------------------------------------------------------------------|
| 0   | $37$10:1.0 14:1.0 18:1.0 25:1.0 29:1.0 34:1.0                                  |
| 4   | $37$0:1.0 1:1.0 2:1.0 3:1.0 11:1.0 15:1.0 21:1.0 24:1.0 27:1.0 29:1.0 30:1.0   |
| 3   | $37$8:1.0 9:1.0 16:1.0 29:1.0 32:1.0                                           |
| 2   | $37$5:1.0 6:1.0 12:1.0 19:1.0 20:1.0 23:1.0 26:1.0 28:1.0 29:1.0 31:1.0 36:2.0 |
| 1   | $37$0:1.0 1:1.0 4:1.0 7:1.0 13:1.0 17:1.0 22:1.0 26:1.0 29:1.0 33:1.0 35:1.0   |
