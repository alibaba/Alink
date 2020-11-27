## Description
calculate doc word count.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| docIdCol | Name of the column indicating document ID. | String | ✓ |  |
| contentCol | Name of the column indicating document content | String | ✓ |  |
| wordDelimiter | Delimiter of words | String |  | " " |

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

segment = SegmentBatchOp().setSelectedCol("text").setOutputCol("segment").linkFrom(inOp1)
remover = StopWordsRemoverBatchOp().setSelectedCol("segment").setOutputCol("remover").linkFrom(segment)
wordCount = DocWordCountBatchOp().setContentCol("remover").setDocIdCol("id").linkFrom(remover)
wordCount.print()

segment = SegmentStreamOp().setSelectedCol("text").setOutputCol("segment").linkFrom(inOp2)
remover = StopWordsRemoverStreamOp().setSelectedCol("segment").setOutputCol("remover").linkFrom(segment)
wordCount = DocWordCountStreamOp().setContentCol("remover").setDocIdCol("id").linkFrom(remover)
wordCount.print()
StreamOperator.execute()
```

#### Results
id|word|cnt
---|---|---
0|医学|1
0|电磁|1
0|旧书|1
0|成像|1
0|二手|1
1|美国|1
1|出版社|1
1|文学|1
1|选读|1
1|二手|1
1|下册|1
1|南开大学|1
1|李宜燮|1
1|9787310003969|1
2|主编|1
2|出版社|1
2|二手|1
2|谢恩|1
2|正版|1
2|入门|1
2|象棋|1
2|华龄|1
2|思|1
2|图解|1
3|中国|1
3|文献|1
3|糖尿病|1
3|索引|1
3|二手|1
4|国内|1
4|文集|1
4|十二册|1
4|二手|1
4|书|1
4|版|1
4|全|1
4|馆藏|1
4|郁达夫|1


