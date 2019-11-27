## Description
DocCountVectorizer converts a document to a sparse vector based on the document frequency, word count or inverse
 document
 frequency of every word in the document.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| maxDF | When the number of documents a word appears in is above maxDF, the word will not be included in the dictionary. It could be an exact countor a fraction of the document number count. When maxDF is within [0, 1), it's used as a fraction. | Double |  | 1.7976931348623157E308 |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| minDF | When the number of documents a word appears in is below minDF, the word will not be included in the dictionary. It could be an exact countor a fraction of the document number count. When minDF is within [0, 1), it's used as a fraction. | Double |  | 1.0 |
| featureType | Feature type, support IDF/WORD_COUNT/TF_IDF/Binary/TF | String |  | "WORD_COUNT" |
| vocabSize | The maximum word number of the dictionary. If the total numbers of words are above this value,the words with lower document frequency will be filtered | Integer |  | 262144 |
| minTF | When the number word in this document in is below minTF, the word will be ignored. It could be an exact count or a fraction of the document token count. When minTF is within [0, 1), it's used as a fraction. | Double |  | 1.0 |
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
    .add(DocCountVectorizer().setSelectedCol("text"))
)

pipeline.fit(inOp).transform(inOp).collectToDataframe()
```

#### Results
##### Output Data
```
rowID   id                                               text
0   0        $37$0:1.0 6:1.0 13:1.0 20:1.0 24:1.0 28:1.0
1   1  $37$0:1.0 2:1.0 3:1.0 4:1.0 5:1.0 7:1.0 16:1.0...
2   2  $37$0:1.0 1:2.0 2:1.0 9:1.0 11:1.0 15:1.0 18:1...
3   3               $37$0:1.0 8:1.0 22:1.0 29:1.0 30:1.0
4   4  $37$0:1.0 3:1.0 4:1.0 10:1.0 12:1.0 14:1.0 17:...
```
