# 文本特征生成训练 (DocCountVectorizerTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.nlp.DocCountVectorizerTrainBatchOp

Python 类名：DocCountVectorizerTrainBatchOp


## 功能介绍
根据分词后的文本统计词的TF/IDF信息，将文本转化为稀疏的向量。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| maxDF | 最大词频 | 如果一个词出现的文档次数大于maxDF, 这个词不会被包含在字典中。maxDF可以是具体的词频也可以是整体词频的比例，如果minDF在[0,1)区间，会被认为是比例。 | Double |  | 1.7976931348623157E308 |
| featureType | 特征类型 | 生成特征向量的类型，支持IDF/WORD_COUNT/TF_IDF/Binary/TF | String |  | "WORD_COUNT" |
| minDF | 最小文档词频 | 如果一个词出现的文档次数小于minDF, 这个词不会被包含在字典中。minTF可以是具体的词频也可以是整体词频的比例，如果minDF在[0,1)区间，会被认为是比例。 | Double |  | 1.0 |
| minTF | 最低词频 | 最低词频，如果词频小于minTF,这个词会被忽略掉。minTF可以是具体的词频也可以是整体词频的比例，如果minTF在[0,1)区间，会被认为是比例。 | Double |  | 1.0 |
| vocabSize | 字典库大小 | 字典库大小，如果总词数目大于这个值，那个文档频率低的词会被过滤掉。 | Integer |  | 262144 |



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
train = DocCountVectorizerTrainBatchOp().setSelectedCol("text").linkFrom(segment)
predictBatch = DocCountVectorizerPredictBatchOp().setSelectedCol("text").linkFrom(train, segment)
train.lazyPrint(-1)
predictBatch.print()

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

public class DocCountVectorizerTrainBatchOpTest {
	@Test
	public void testDocCountVectorizerTrainBatchOp() throws Exception {
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
		BatchOperator <?> predictBatch = new DocCountVectorizerPredictBatchOp().setSelectedCol("text").linkFrom(train,
			segment);
		train.lazyPrint(-1);
		predictBatch.print();
	}
}
```

### 运行结果
#### 模型数据
model_id|model_info
--------|----------
0|{"minTF":"1.0","featureType":"\"WORD_COUNT\""}
1048576|{"f0":"）","f1":0.6931471805599453,"f2":0}
2097152|{"f0":"（","f1":0.6931471805599453,"f2":1}
3145728|{"f0":"馆藏","f1":1.0986122886681098,"f2":2}
4194304|{"f0":"郁达夫","f1":1.0986122886681098,"f2":3}
5242880|{"f0":"选读","f1":1.0986122886681098,"f2":4}
6291456|{"f0":"象棋","f1":1.0986122886681098,"f2":5}
7340032|{"f0":"谢恩","f1":1.0986122886681098,"f2":6}
8388608|{"f0":"美国","f1":1.0986122886681098,"f2":7}
9437184|{"f0":"索引","f1":1.0986122886681098,"f2":8}
10485760|{"f0":"糖尿病","f1":1.0986122886681098,"f2":9}
11534336|{"f0":"电磁","f1":1.0986122886681098,"f2":10}
12582912|{"f0":"版","f1":1.0986122886681098,"f2":11}
13631488|{"f0":"正版","f1":1.0986122886681098,"f2":12}
14680064|{"f0":"李宜燮","f1":1.0986122886681098,"f2":13}
15728640|{"f0":"旧书","f1":1.0986122886681098,"f2":14}
16777216|{"f0":"文集","f1":1.0986122886681098,"f2":15}
17825792|{"f0":"文献","f1":1.0986122886681098,"f2":16}
18874368|{"f0":"文学","f1":1.0986122886681098,"f2":17}
19922944|{"f0":"成像","f1":1.0986122886681098,"f2":18}
20971520|{"f0":"思","f1":1.0986122886681098,"f2":19}
22020096|{"f0":"图解","f1":1.0986122886681098,"f2":20}
23068672|{"f0":"国内","f1":1.0986122886681098,"f2":21}
24117248|{"f0":"南开大学","f1":1.0986122886681098,"f2":22}
25165824|{"f0":"华龄","f1":1.0986122886681098,"f2":23}
26214400|{"f0":"十二册","f1":1.0986122886681098,"f2":24}
27262976|{"f0":"医学","f1":1.0986122886681098,"f2":25}
28311552|{"f0":"出版社","f1":0.6931471805599453,"f2":26}
29360128|{"f0":"全","f1":1.0986122886681098,"f2":27}
30408704|{"f0":"入门","f1":1.0986122886681098,"f2":28}
31457280|{"f0":"二手","f1":0.0,"f2":29}
32505856|{"f0":"书","f1":1.0986122886681098,"f2":30}
33554432|{"f0":"主编","f1":1.0986122886681098,"f2":31}
34603008|{"f0":"中国","f1":1.0986122886681098,"f2":32}
35651584|{"f0":"下册","f1":1.0986122886681098,"f2":33}
36700160|{"f0":":","f1":1.0986122886681098,"f2":34}
37748736|{"f0":"9787310003969","f1":1.0986122886681098,"f2":35}
38797312|{"f0":"/","f1":1.0986122886681098,"f2":36}

#### 批预测结果
id|text
--|----
0|$37$10:1.0 14:1.0 18:1.0 25:1.0 29:1.0 34:1.0
1|$37$0:1.0 1:1.0 4:1.0 7:1.0 13:1.0 17:1.0 22:1.0 26:1.0 29:1.0 33:1.0 35:1.0
2|$37$5:1.0 6:1.0 12:1.0 19:1.0 20:1.0 23:1.0 26:1.0 28:1.0 29:1.0 31:1.0 36:2.0
3|$37$8:1.0 9:1.0 16:1.0 29:1.0 32:1.0
4|$37$0:1.0 1:1.0 2:1.0 3:1.0 11:1.0 15:1.0 21:1.0 24:1.0 27:1.0 29:1.0 30:1.0
