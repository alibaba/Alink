# 文本特征生成 (DocCountVectorizer)
Java 类名：com.alibaba.alink.pipeline.nlp.DocCountVectorizer

Python 类名：DocCountVectorizer


## 功能介绍

根据文本中词语的特征信息，将每条文本转化为稀疏向量。

### 使用方式

文本内容列（SelectedCol）中的内容用于统计词语的统计信息，需要是用空格分隔的词语。

将文本转换为稀疏向量时，每个唯一的词语将对应向量中的一个唯一的索引值。 而向量中对应索引的值表示这个词语在文本中的特征信息，可以通过参数 featureType 来选择不同的特征。

在转换时，所使用的词语集合还可以通过参数来进行筛选：

- maxDF/minDF：根据包含词语的文本次数（DF）进行筛选（当设置值在[0,1)区间时，表示占总文本数的比例）；
- minTF：仅在预测单条文本时起作用，根据词语在当前文本中的出现的次数进行筛选（当设置值在[0,1)区间时，表示占当前文本总次数的比例）；
- vocabSize：根据词语在所有文本中出现的总次数排序，只使用前 vocabSize 个词语。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| featureType | 特征类型 | 生成特征向量的类型，支持IDF/WORD_COUNT/TF_IDF/Binary/TF | String |  | "IDF", "WORD_COUNT", "TF_IDF", "BINARY", "TF" | "WORD_COUNT" |
| maxDF | 最大词频 | 如果一个词出现的文档次数大于maxDF, 这个词不会被包含在字典中。maxDF可以是具体的词频也可以是整体词频的比例，如果minDF在[0,1)区间，会被认为是比例。 | Double |  |  | 1.7976931348623157E308 |
| minDF | 最小文档词频 | 如果一个词出现的文档次数小于minDF, 这个词不会被包含在字典中。minTF可以是具体的词频也可以是整体词频的比例，如果minDF在[0,1)区间，会被认为是比例。 | Double |  |  | 1.0 |
| minTF | 最低词频 | 最低词频，如果词频小于minTF,这个词会被忽略掉。minTF可以是具体的词频也可以是整体词频的比例，如果minTF在[0,1)区间，会被认为是比例。 | Double |  |  | 1.0 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| vocabSize | 字典库大小 | 字典库大小，如果总词数目大于这个值，那个文档频率低的词会被过滤掉。 | Integer |  |  | 262144 |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

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

inOp = BatchOperator.fromDataframe(df, schemaStr='id int, text string')

pipeline = Pipeline() \
    .add(Segment().setSelectedCol("text")) \
    .add(DocCountVectorizer().setSelectedCol("text"))

pipeline.fit(inOp).transform(inOp).print()
```

### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.nlp.DocCountVectorizer;
import com.alibaba.alink.pipeline.nlp.Segment;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DocCountVectorizerTest {
	@Test
	public void testDocCountVectorizer() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "二手旧书:医学电磁成像"),
			Row.of(1, "二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969"),
			Row.of(2, "二手正版图解象棋入门/谢恩思主编/华龄出版社"),
			Row.of(3, "二手中国糖尿病文献索引"),
			Row.of(4, "二手郁达夫文集（ 国内版 ）全十二册馆藏书")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, text string");
		Pipeline pipeline = new Pipeline()
			.add(new Segment().setSelectedCol("text"))
			.add(new DocCountVectorizer().setSelectedCol("text"));
		pipeline.fit(inOp).transform(inOp).print();
	}
}
```

### 运行结果

#### 输出数据

| id  | text                                                                           |
|-----|--------------------------------------------------------------------------------|
| 0   | $37$10:1.0 14:1.0 18:1.0 25:1.0 29:1.0 34:1.0                                  |
| 1   | $37$0:1.0 1:1.0 4:1.0 7:1.0 13:1.0 17:1.0 22:1.0 26:1.0 29:1.0 33:1.0 35:1.0   |
| 2   | $37$5:1.0 6:1.0 12:1.0 19:1.0 20:1.0 23:1.0 26:1.0 28:1.0 29:1.0 31:1.0 36:2.0 |
| 3   | $37$8:1.0 9:1.0 16:1.0 29:1.0 32:1.0                                           |
| 4   | $37$0:1.0 1:1.0 2:1.0 3:1.0 11:1.0 15:1.0 21:1.0 24:1.0 27:1.0 29:1.0 30:1.0   |
