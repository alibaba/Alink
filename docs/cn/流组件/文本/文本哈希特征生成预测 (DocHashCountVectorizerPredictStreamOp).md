# 文本哈希特征生成预测 (DocHashCountVectorizerPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.nlp.DocHashCountVectorizerPredictStreamOp

Python 类名：DocHashCountVectorizerPredictStreamOp


## 功能介绍

根据文本中词语的特征信息，将每条文本转化为固定长度的稀疏向量。

在转换时，每个词语会通过哈希函数映射到稀疏向量的一个索引值，映射到同一个索引值的多个词语将看作同一个词语来统计特征信息。

### 使用方式

该组件是预测组件，需要配合训练组件 DocHashCountVectorizerTrainBatchOp 使用。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |

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
train = DocHashCountVectorizerTrainBatchOp().setSelectedCol("text").linkFrom(segment)
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, text string')
segment2 = SegmentStreamOp().setSelectedCol("text").linkFrom(inOp2)
predictStream = DocHashCountVectorizerPredictStreamOp(train).setSelectedCol("text").linkFrom(segment2)
predictStream.print()
StreamOperator.execute()
```

### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerPredictBatchOp;
import com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerTrainBatchOp;
import com.alibaba.alink.operator.batch.nlp.SegmentBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DocHashCountVectorizerPredictStreamOpTest {
	@Test
	public void testDocHashCountVectorizerPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "二手旧书:医学电磁成像"),
			Row.of(1, "二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969"),
			Row.of(2, "二手正版图解象棋入门/谢恩思主编/华龄出版社"),
			Row.of(3, "二手中国糖尿病文献索引"),
			Row.of(4, "二手郁达夫文集（ 国内版 ）全十二册馆藏书")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "id int, text string");
		BatchOperator <?> segment = new SegmentBatchOp().setSelectedCol("text").linkFrom(inOp1);
		BatchOperator <?> train = new DocHashCountVectorizerTrainBatchOp().setSelectedCol("text").linkFrom(segment);
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "id int, text string");
		StreamOperator <?> segment2 = new SegmentStreamOp().setSelectedCol("text").linkFrom(inOp2);
		StreamOperator <?> predictStream = new DocHashCountVectorizerPredictStreamOp(train).setSelectedCol("text")
			.linkFrom(segment2);
		predictStream.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

| id  | text                                                                                                                                  |
|-----|---------------------------------------------------------------------------------------------------------------------------------------|
| 2   | $262144$40170:1.0 70777:1.0 96509:1.0 126159:1.0 158267:1.0 181703:1.0 206232:1.0 216139:1.0 232884:1.0 250534:2.0 259932:1.0         |
| 4   | $262144$0:4.0 87711:1.0 138080:1.0 162140:1.0 180035:1.0 195777:1.0 206232:1.0 219988:1.0 241122:1.0 254628:1.0 257763:1.0 259051:1.0 |
| 3   | $262144$206232:1.0 214785:1.0 251090:1.0 255656:1.0 261064:1.0                                                                        |
| 1   | $262144$0:6.0 37505:1.0 46743:1.0 93228:1.0 119217:1.0 138080:1.0 141480:1.0 172901:1.0 206232:1.0 216139:1.0 226698:1.0 254628:1.0   |
| 0   | $262144$10121:1.0 64444:1.0 119456:1.0 206232:1.0 210357:1.0 256946:1.0                                                               |
