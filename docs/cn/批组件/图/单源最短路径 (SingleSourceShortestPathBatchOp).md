# 单源最短路径 (SingleSourceShortestPathBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.SingleSourceShortestPathBatchOp

Python 类名：SingleSourceShortestPathBatchOp


## 功能介绍

对于给定的图以及给定的单个源点，求给定点到图中所有点的最短路径。如果某点与源点不相连，输出的距离为长整型可取到的最大值。

单源最短路径的应用场景有网络设计、路径规划等。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |
| sourcePoint | 源点的值 | 源点的值 | String | ✓ |  |  |
| asUndirectedGraph | 是否为无向图 | 是否为无向图 | Boolean |  |  | true |
| edgeWeightCol | 边权重列 | 表示边权重的列 | String |  |  | null |
| maxIter | 最大迭代次数 | 最大迭代次数 | Integer |  | x >= 1 | 50 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([[1, 2],\
[2, 3],\
[3, 4],\
[4, 5],\
[5, 6],\
[6, 7],\
[7, 8],\
[8, 9],\
[9, 6]])

data = BatchOperator.fromDataframe(df, schemaStr="source double, target double")
SingleSourceShortestPathBatchOp()\
    .setEdgeSourceCol("source")\
    .setEdgeTargetCol("target")\
    .setSourcePoint("1")\
    .linkFrom(data)\
    .print()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class SingleSourceShortestPathBatchOpTest extends AlinkTestBase {

    @Test
    public void test() throws Exception {
        Row[] rows = new Row[]{
            Row.of(1, 2, 1.0),
            Row.of(2, 3, 1.0),
            Row.of(3, 4, 1.0),
            Row.of(4, 5, 1.0),
            Row.of(5, 6, 1.0),
            Row.of(6, 7, 1.0),
            Row.of(7, 8, 1.0),
            Row.of(8, 9, 1.0),
            Row.of(9, 6, 1.0)
        };
        BatchOperator inData = new MemSourceBatchOp(rows, new String[]{"source", "target", "weight"});
        SingleSourceShortestPathBatchOp op = new SingleSourceShortestPathBatchOp()
            .setEdgeSourceCol("source")
            .setEdgeTargetCol("target")
            .setSourcePoint("1");

        BatchOperator res = op.linkFrom(inData);
        res.print();
    }
}
```

### 运行结果

vertex|distance
------|--------
1|0.0000
2|1.0000
5|4.0000
9|6.0000
4|3.0000
3|2.0000
8|7.0000
6|5.0000
7|6.0000

