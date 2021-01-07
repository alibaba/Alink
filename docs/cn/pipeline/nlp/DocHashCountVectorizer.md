## 功能介绍
根据分词后的文本统计词的IDF信息，将文本转化为稀疏的向量，与 文本特征生成 的区别在于它是统计文本哈希后的词频

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| numFeatures | 向量维度 | 生成向量长度 | Integer |  | 262144 |
| minDF | 最小文档词频 | 如果一个词出现的文档次数小于minDF, 这个词不会被包含在字典中。minTF可以是具体的词频也可以是整体词频的比例，如果minDF在[0,1)区间，会被认为是比例。 | Double |  | 1.0 |
| featureType | 特征类型 | 生成特征向量的类型，支持IDF/WORD_COUNT/TF_IDF/Binary/TF | String |  | "WORD_COUNT" |
| minTF | 最低词频 | 最低词频，如果词频小于minTF,这个词会被忽略掉。minTF可以是具体的词频也可以是整体词频的比例，如果minTF在[0,1)区间，会被认为是比例。 | Double |  | 1.0 |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


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
    .add(DocHashCountVectorizer().setSelectedCol("text"))
)

pipeline.fit(inOp).transform(inOp).collectToDataframe()
```

#### 脚本运行结果
##### 输出数据
```
   id                                               text
0   0  $262144$10121:1.0 64444:1.0 119456:1.0 206232:...
1   1  $262144$0:6.0 37505:1.0 46743:1.0 93228:1.0 11...
2   2  $262144$40170:1.0 70777:1.0 96509:1.0 126159:1...
3   3  $262144$206232:1.0 214785:1.0 251090:1.0 25565...
4   4  $262144$0:4.0 87711:1.0 138080:1.0 162140:1.0 ...
```
