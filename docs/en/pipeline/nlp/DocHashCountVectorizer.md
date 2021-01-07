## Description
DocHashIDFVectorizer converts a document to a sparse vector based on the inverse document frequency of every word in
 the document.
 Different from DocCountVectorizer, it used the hash value of the word as index.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| numThreads | Thread number of operator. | Integer |  | 1 |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| numFeatures | The number of features. It will be the length of the output vector. | Integer |  | 262144 |
| minDF | When the number of documents a word appears in is below minDF, the word will not be included in the dictionary. It could be an exact countor a fraction of the document number count. When minDF is within [0, 1), it's used as a fraction. | Double |  | 1.0 |
| featureType | Feature type, support IDF/WORD_COUNT/TF_IDF/Binary/TF | String |  | "WORD_COUNT" |
| minTF | When the number word in this document in is below minTF, the word will be ignored. It could be an exact count or a fraction of the document token count. When minTF is within [0, 1), it's used as a fraction. | Double |  | 1.0 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
#### Code
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

#### Results
##### Output Data
```
   id                                               text
0   0  $262144$10121:1.0 64444:1.0 119456:1.0 206232:...
1   1  $262144$0:6.0 37505:1.0 46743:1.0 93228:1.0 11...
2   2  $262144$40170:1.0 70777:1.0 96509:1.0 126159:1...
3   3  $262144$206232:1.0 214785:1.0 251090:1.0 25565...
4   4  $262144$0:4.0 87711:1.0 138080:1.0 162140:1.0 ...
```
