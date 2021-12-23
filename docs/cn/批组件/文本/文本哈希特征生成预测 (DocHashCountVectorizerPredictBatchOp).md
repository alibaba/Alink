# 文本哈希特征生成预测 (DocHashCountVectorizerPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerPredictBatchOp

Python 类名：DocHashCountVectorizerPredictBatchOp


## 功能介绍
根据分词后的文本统计词的IDF信息，将文本转化为稀疏的向量，与 文本特征生成 的区别在于它是统计文本哈希后的词频。

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

public class DocHashCountVectorizerPredictBatchOpTest {
	@Test
	public void testDocHashCountVectorizerPredictBatchOp() throws Exception {
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
	}
}
```

### 运行结果
#### 模型数据
model_id|model_info
--------|----------
0|{"numFeatures":"262144","minTF":"1.0","featureType":"\"WORD_COUNT\""}
1048576|{"0":-0.6061358035703156,"37505":1.0986122886681098,"180035":1.0986122886681098,"214785":1.0986122886681098,"195777":1.0986122886681098,"181703":1.0986122886681098,"216139":0.6931471805599453,"226698":1.0986122886681098,"10121":1.0986122886681098,"261064":1.0986122886681098,"126159":1.0986122886681098,"251090":1.0986122886681098,"46743":1.0986122886681098,"219988":1.0986122886681098,"206232":0.0,"162140":1.0986122886681098,"87711":1.0986122886681098,"259932":1.0986122886681098,"257763":1.0986122886681098,"119456":1.0986122886681098,"241122":1.0986122886681098,"138080":0.6931471805599453,"250534":0.6931471805599453,"172901":1.0986122886681098,"254628":0.6931471805599453,"259051":1.0986122886681098,"141480":1.0986122886681098,"40170":1.0986122886681098,"255656":1.0986122886681098,"93228":1.0986122886681098,"119217":1.0986122886681098,"256946":1.0986122886681098,"210357":1.0986122886681098,"232884":1.0986122886681098,"70777":1.0986122886681098,"158267":1.0986122886681098,"64444":1.0986122886681098,"96509":1.0986122886681098}

#### 批预测结果
id|text
---|----
0|$262144$10121:1.0 64444:1.0 119456:1.0 206232:1.0 210357:1.0 256946:1.0
1|$262144$0:6.0 37505:1.0 46743:1.0 93228:1.0 119217:1.0 138080:1.0 141480:1.0 172901:1.0 206232:1.0 216139:1.0 226698:1.0 254628:1.0
2|$262144$40170:1.0 70777:1.0 96509:1.0 126159:1.0 158267:1.0 181703:1.0 206232:1.0 216139:1.0 232884:1.0 250534:2.0 259932:1.0
3|$262144$206232:1.0 214785:1.0 251090:1.0 255656:1.0 261064:1.0
4|$262144$0:4.0 87711:1.0 138080:1.0 162140:1.0 180035:1.0 195777:1.0 206232:1.0 219988:1.0 241122:1.0 254628:1.0 257763:1.0 259051:1.0
