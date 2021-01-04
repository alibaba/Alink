## Description
Transform a document to a sparse vector based on the inverse document frequency(idf) statistics provided by
 DocHashCountVectorizerTrainBatchOp. It uses MurmurHash 3 to get the hash value of a word as the index.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| numThreads | Thread number of operator. | Integer |  | 1 |
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
inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, text string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, text string')

segment = SegmentBatchOp().setSelectedCol("text").linkFrom(inOp1)
train = DocHashCountVectorizerTrainBatchOp().setSelectedCol("text").linkFrom(segment)
predictBatch = DocHashCountVectorizerPredictBatchOp().setSelectedCol("text").linkFrom(train, segment)
[model,predict] = collectToDataframes(train, predictBatch)
print(model)
print(predict)

segment = SegmentStreamOp().setSelectedCol("text").linkFrom(inOp2)
predictStream = DocHashCountVectorizerPredictStreamOp(train).setSelectedCol("text").linkFrom(segment)
predictStream.print(refreshInterval=-1)
StreamOperator.execute()
```

#### Results
##### Model
```
rowID  model_id                                         model_info
0         0  {"numFeatures":"262144","minTF":"1.0","feature...
1   1048576  {"0":-0.6061358035703156,"37505":1.09861228866...
```

##### Output Data
```
   id                                               text
0   0  $262144$10121:1.0 64444:1.0 119456:1.0 206232:...
1   1  $262144$0:6.0 37505:1.0 46743:1.0 93228:1.0 11...
2   2  $262144$40170:1.0 70777:1.0 96509:1.0 126159:1...
3   3  $262144$206232:1.0 214785:1.0 251090:1.0 25565...
4   4  $262144$0:4.0 87711:1.0 138080:1.0 162140:1.0 ...
```
