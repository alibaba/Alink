## Description
Automatically identify in a text a set of terms that best describe the document based on TextRank.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| topN | top n | Integer |  | 10 |
| windowSize | window size | Integer |  | 2 |
| dampingFactor | damping factor | Double |  | 0.85 |
| maxIter | Maximum iterations, The default value is 100 | Integer |  | 100 |
| outputCol | Name of the output column | String |  | null |
| epsilon | converge threshold | Double |  | 1.0E-6 |
| method | Method to extract keywords, support TF_IDF and TEXT_RANK | String |  | "TEXT_RANK" |

## Script Example
#### Code
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, u'二手旧书:医学电磁成像'],
    [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
    [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
    [3, u'二手中国糖尿病文献索引'],
    [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']])
df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, text string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, text string')

segment = SegmentBatchOp().setSelectedCol("text").linkFrom(inOp1)
remover = StopWordsRemoverBatchOp().setSelectedCol("text").linkFrom(segment)
keywords = KeywordsExtractionBatchOp().setSelectedCol("text").setMethod("TF_IDF").setTopN(3).linkFrom(remover)
keywords.print()

segment = SegmentStreamOp().setSelectedCol("text").linkFrom(inOp2)
remover = StopWordsRemoverStreamOp().setSelectedCol("text").linkFrom(segment)
keywords = KeywordsExtractionStreamOp().setSelectedCol("text").setTopN(3).linkFrom(remover)
keywords.print()
StreamOperator.execute()
```

#### Results
```
        text  id
0   电磁 旧书 成像   0
1  李宜燮 选读 下册   1
2   谢恩 象棋 入门   2
3   索引 中国 文献   3
4    十二册 版 全   4
```






