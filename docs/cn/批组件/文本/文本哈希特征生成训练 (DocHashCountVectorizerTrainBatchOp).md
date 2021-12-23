# 文本哈希特征生成训练 (DocHashCountVectorizerTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerTrainBatchOp

Python 类名：DocHashCountVectorizerTrainBatchOp


## 功能介绍
根据分词后的文本统计词的IDF信息，将文本转化为稀疏的向量，与 文本特征生成 的区别在于它是统计文本哈希后的词频。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| featureType | 特征类型 | 生成特征向量的类型，支持IDF/WORD_COUNT/TF_IDF/Binary/TF | String |  | "WORD_COUNT" |
| minDF | 最小文档词频 | 如果一个词出现的文档次数小于minDF, 这个词不会被包含在字典中。minTF可以是具体的词频也可以是整体词频的比例，如果minDF在[0,1)区间，会被认为是比例。 | Double |  | 1.0 |
| minTF | 最低词频 | 最低词频，如果词频小于minTF,这个词会被忽略掉。minTF可以是具体的词频也可以是整体词频的比例，如果minTF在[0,1)区间，会被认为是比例。 | Double |  | 1.0 |
| numFeatures | 向量维度 | 生成向量长度 | Integer |  | 262144 |


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

segment = SegmentBatchOp().setSelectedCol("text").linkFrom(inOp1)
train = DocHashCountVectorizerTrainBatchOp().setSelectedCol("text").linkFrom(segment)
predictBatch = DocHashCountVectorizerPredictBatchOp().setSelectedCol("text").linkFrom(train, segment)
train.lazyPrint(-1)
predictBatch.print()

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
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.DocHashCountVectorizerPredictStreamOp;
import com.alibaba.alink.operator.stream.nlp.SegmentStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DocHashCountVectorizerTrainBatchOpTest {
	@Test
	public void testDocHashCountVectorizerTrainBatchOp() throws Exception {
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
		BatchOperator <?> predictBatch = new DocHashCountVectorizerPredictBatchOp().setSelectedCol("text").linkFrom(
			train, segment);
		train.lazyPrint(-1);
		predictBatch.print();
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
#### 模型数据
model_id|model_info
--------|----------
0|{"numFeatures":"262144","minTF":"1.0","featureType":"\"WORD_COUNT\""}
1048576|{"0":-0.6061358035703156,"180035":1.0986122886681098,"37505":1.0986122886681098,"214785":1.0986122886681098,"195777":1.0986122886681098,"181703":1.0986122886681098,"216139":0.6931471805599453,"10121":1.0986122886681098,"226698":1.0986122886681098,"261064":1.0986122886681098,"126159":1.0986122886681098,"251090":1.0986122886681098,"219988":1.0986122886681098,"46743":1.0986122886681098,"206232":0.0,"162140":1.0986122886681098,"87711":1.0986122886681098,"259932":1.0986122886681098,"257763":1.0986122886681098,"241122":1.0986122886681098,"119456":1.0986122886681098,"138080":0.6931471805599453,"250534":0.6931471805599453,"254628":0.6931471805599453,"172901":1.0986122886681098,"259051":1.0986122886681098,"141480":1.0986122886681098,"40170":1.0986122886681098,"255656":1.0986122886681098,"93228":1.0986122886681098,"119217":1.0986122886681098,"256946":1.0986122886681098,"210357":1.0986122886681098,"232884":1.0986122886681098,"70777":1.0986122886681098,"158267":1.0986122886681098,"64444":1.0986122886681098,"96509":1.0986122886681098}

#### 批预测结果
id|text
--|----
0|$262144$10121:1.0 64444:1.0 119456:1.0 206232:1.0 210357:1.0 256946:1.0
1|$262144$0:6.0 37505:1.0 46743:1.0 93228:1.0 119217:1.0 138080:1.0 141480:1.0 172901:1.0 206232:1.0 216139:1.0 226698:1.0 254628:1.0
2|$262144$40170:1.0 70777:1.0 96509:1.0 126159:1.0 158267:1.0 181703:1.0 206232:1.0 216139:1.0 232884:1.0 250534:2.0 259932:1.0
3|$262144$206232:1.0 214785:1.0 251090:1.0 255656:1.0 261064:1.0
4|$262144$0:4.0 87711:1.0 138080:1.0 162140:1.0 180035:1.0 195777:1.0 206232:1.0 219988:1.0 241122:1.0 254628:1.0 257763:1.0 259051:1.0

#### 流预测结果
id|text
--|----
4|$262144$0:4.0 87711:1.0 138080:1.0 162140:1.0 180035:1.0 195777:1.0 206232:1.0 219988:1.0 241122:1.0 254628:1.0 257763:1.0 259051:1.0
3|$262144$206232:1.0 214785:1.0 251090:1.0 255656:1.0 261064:1.0
1|$262144$0:6.0 37505:1.0 46743:1.0 93228:1.0 119217:1.0 138080:1.0 141480:1.0 172901:1.0 206232:1.0 216139:1.0 226698:1.0 254628:1.0
0|$262144$10121:1.0 64444:1.0 119456:1.0 206232:1.0 210357:1.0 256946:1.0
2|$262144$40170:1.0 70777:1.0 96509:1.0 126159:1.0 158267:1.0 181703:1.0 206232:1.0 216139:1.0 232884:1.0 250534:2.0 259932:1.0
