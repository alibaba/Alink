## 功能介绍
根据分词后的文本统计词的TF/IDF信息，将文本转化为稀疏的向量。

## 参数说明
<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| maxDF | 最大词频 | 如果一个词出现的文档次数大于maxDF, 这个词不会被包含在字典中。maxDF可以是具体的词频也可以是整体词频的比例，如果minDF在[0,1)区间，会被认为是比例。 | Double |  | 1.7976931348623157E308 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| minDF | 最小文档词频 | 如果一个词出现的文档次数小于minDF, 这个词不会被包含在字典中。minTF可以是具体的词频也可以是整体词频的比例，如果minDF在[0,1)区间，会被认为是比例。 | Double |  | 1.0 |
| featureType | 特征类型 | 生成特征向量的类型，支持IDF/WORD_COUNT/TF_IDF/Binary/TF | String |  | "WORD_COUNT" |
| vocabSize | 字典库大小 | 字典库大小，如果总词数目大于这个值，那个文档频率低的词会被过滤掉。 | Integer |  | 262144 |
| minTF | 最低词频 | 最低词频，如果词频小于minTF,这个词会被忽略掉。minTF可以是具体的词频也可以是整体词频的比例，如果minTF在[0,1)区间，会被认为是比例。 | Double |  | 1.0 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码
```
import numpy as np
import pandas as pd
data = np.array([
    [0, u'二手旧书:医学电磁成像'],
    [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
    [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
    [3, u'二手中国糖尿病文献索引'],
    [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']])
df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
inOp = BatchOperator.fromDataframe(df, schemaStr='id int, text string')

pipeline = (
    Pipeline()
    .add(Segment().setSelectedCol("text"))
    .add(DocCountVectorizer().setSelectedCol("text"))
)

pipeline.fit(inOp).transform(inOp).collectToDataframe()
```

#### 脚本运行结果
##### 输出数据
```
rowID   id                                               text
0   0        $37$0:1.0 6:1.0 13:1.0 20:1.0 24:1.0 28:1.0
1   1  $37$0:1.0 2:1.0 3:1.0 4:1.0 5:1.0 7:1.0 16:1.0...
2   2  $37$0:1.0 1:2.0 2:1.0 9:1.0 11:1.0 15:1.0 18:1...
3   3               $37$0:1.0 8:1.0 22:1.0 29:1.0 30:1.0
4   4  $37$0:1.0 3:1.0 4:1.0 10:1.0 12:1.0 14:1.0 17:...
```