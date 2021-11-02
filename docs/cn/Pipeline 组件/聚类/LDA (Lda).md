# LDA (Lda)
Java 类名：com.alibaba.alink.pipeline.clustering.Lda

Python 类名：Lda


## 功能介绍

LDA是一种文档主题生成模型。LDA是一种非监督机器学习技术，可以用来识别大规模文档集（document collection）或语料库（corpus）中潜藏的主题信息。它采用了词袋（bag of words）的方法，这种方法将每一篇文档视为一个词频向量，从而将文本信息转化为了易于建模的数字信息。但是词袋方法没有考虑词与词之间的顺序，这简化了问题的复杂性，同时也为模型的改进提供了契机。每一篇文档代表了一些主题所构成的一个概率分布，而每一个主题又代表了很多单词所构成的一个概率分布。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| topicNum | 主题个数 | 主题个数 | Integer | ✓ |  |
| numIter | 迭代次数 | 迭代次数，默认为10 | Integer |  | 10 |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  | 0 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| vocabSize | 字典库大小 | 字典库大小，如果总词数目大于这个值，那个文档频率低的词会被过滤掉。 | Integer |  | 262144 |
| alpha | 文章的超参 | 文章的超参 | Double |  | -1.0 |
| beta | 词的超参 | 词的超参 | Double |  | -1.0 |
| method | 优化方法 | 优化方法, 包含"em"和"online"两种。 | String |  | "EM" |
| onlineLearningOffset | 偏移量 | 偏移量 | Double |  | 1024.0 |
| learningDecay | 衰减率 | 衰减率 | Double |  | 0.51 |
| subsamplingRate | 采样率 | 采样率 | Double |  | 0.05 |
| optimizeDocConcentration | 是否优化alpha | 是否优化alpha | Boolean |  | true |
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
    ["a b b c c c c c c e e f f f g h k k k"], 
    ["a b b b d e e e h h k"], 
    ["a b b b b c f f f f g g g g g g g g g i j j"], 
    ["a a b d d d g g g g g i i j j j k k k k k k k k k"], 
    ["a a a b c d d d d d d d d d e e e g g j k k k"], 
    ["a a a a b b d d d e e e e f f f f f g h i j j j j"], 
    ["a a b d d d g g g g g i i j j k k k k k k k k k"], 
    ["a b c d d d d d d d d d e e f g g j k k k"], 
    ["a a a a b b b b d d d e e e e f f g h h h"], 
    ["a a b b b b b b b b c c e e e g g i i j j j j j j j k k"], 
    ["a b c d d d d d d d d d f f g g j j j k k k"], 
    ["a a a a b e e e e f f f f f g h h h j"]
])

data = BatchOperator.fromDataframe(df, schemaStr="doc string")

lda = Lda()\
    .setSelectedCol("doc")\
    .setTopicNum(6)\
    .setMethod("online")\
    .setPredictionCol("pred")

lda.fit(data).transform(data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.clustering.Lda;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LdaTest {
	@Test
	public void testLda() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a b b c c c c c c e e f f f g h k k k"),
			Row.of("a b b b d e e e h h k"),
			Row.of("a b b b b c f f f f g g g g g g g g g i j j"),
			Row.of("a a b d d d g g g g g i i j j j k k k k k k k k k"),
			Row.of("a a a b c d d d d d d d d d e e e g g j k k k"),
			Row.of("a a a a b b d d d e e e e f f f f f g h i j j j j"),
			Row.of("a a b d d d g g g g g i i j j k k k k k k k k k"),
			Row.of("a b c d d d d d d d d d e e f g g j k k k"),
			Row.of("a a a a b b b b d d d e e e e f f g h h h"),
			Row.of("a a b b b b b b b b c c e e e g g i i j j j j j j j k k"),
			Row.of("a b c d d d d d d d d d f f g g j j j k k k"),
			Row.of("a a a a b e e e e f f f f f g h h h j")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "doc string");
		Lda lda = new Lda()
			.setSelectedCol("doc")
			.setTopicNum(6)
			.setMethod("online")
			.setPredictionCol("pred");
		lda.fit(data).transform(data).print();
	}
}
```

### 运行结果
#### 模型结果

| model_id   | model_info |
| --- | --- |
| 0          | {"logPerplexity":"22.332946259667825","betaArray":"[0.2,0.2,0.2,0.2,0.2]","logLikelihood":"-915.6507966463809","method":"\"online\"","alphaArray":"[0.16926092344987234,0.17828690973899627,0.17282213771078062,0.18555794554097874,0.15898463316059516]","topicNum":"5","vocabularySize":"11"} |
| 1048576    | {"m":5,"n":11,"data":[6135.5227952852865,7454.918734235136,6569.887273287071,7647.590029783137,7459.37196542985,6689.783286316853,8396.842418256507,7771.120258275389,7497.94247894282,7983.617922597562,7975.470848777338,7114.413879475893,8420.381073064213,6747.377398176922,6959.728145538011,7368.902852508116,7635.5968635989275,6734.522904998126,6792.566021565353,6487.885790775943,8086.932892160501,8443.888239756887,7227.0417299467745,7561.023252667202,6264.97808011349,6964.080980387547,8234.247108608217,8263.190977757107,7872.088651923572,7725.669369347696,7591.453097717432,7733.627117746213,6595.2753568320295,8158.346230399092,7765.777648163369,6456.891859572009,6814.768507000475,6612.17371610521,6506.877213010642,7166.140342089344,7588.370517354863,7645.016947338933,8929.620632942893,6855.855247335312,7263.088264847597,7993.009126022237,7302.794183756114,6074.524636118613,6386.578740892538,8465.84700774072,7242.276290933901,7257.474039179472,7676.72445702261,6733.70550536632,7577.265607033211]} |
| 2097152    | {"f0":"d","f1":0.36772478012531734,"f2":0} |
| 3145728    | {"f0":"k","f1":0.36772478012531734,"f2":1} |
| 4194304    | {"f0":"g","f1":0.08004270767353636,"f2":2} |
| 5242880    | {"f0":"b","f1":0.0,"f2":3} |
| 6291456    | {"f0":"a","f1":0.0,"f2":4} |
| 7340032    | {"f0":"e","f1":0.36772478012531734,"f2":5} |
| 8388608    | {"f0":"j","f1":0.26236426446749106,"f2":6} |
| 9437184    | {"f0":"f","f1":0.4855078157817008,"f2":7} |
| 10485760   | {"f0":"c","f1":0.6190392084062235,"f2":8} |
| 11534336   | {"f0":"h","f1":0.7731898882334817,"f2":9} |
| 12582912   | {"f0":"i","f1":0.7731898882334817,"f2":10} |


#### 预测结果

| doc        | pred       |
| --- | --- |
| a b b b d e e e h h k | 1          |
| a a b d d d g g g g g i i j j j k k k k k k k k k | 3          |
| a a a a b b d d d e e e e f f f f f g h i j j j j | 3          |
| a a b d d d g g g g g i i j j k k k k k k k k k | 1          |
| a a a a b b b b d d d e e e e f f g h h h | 3          |
| a b c d d d d d d d d d f f g g j j j k k k | 3          |
| a b b c c c c c c e e f f f g h k k k | 2          |
| a b b b b c f f f f g g g g g g g g g i j j | 0          |
| a a a b c d d d d d d d d d e e e g g j k k k | 3          |
| a b c d d d d d d d d d e e f g g j k k k | 3          |
| a a b b b b b b b b c c e e e g g i i j j j j j j j k k | 3          |
| a a a a b e e e e f f f f f g h h h j | 0          |
