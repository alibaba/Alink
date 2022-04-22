# 全表统计 (SummarizerBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.SummarizerBatchOp

Python 类名：SummarizerBatchOp


## 功能介绍

全表统计用来计算整表的统计量, 包含count(个数),numValidValue(有效值个数), numMissingValue(缺失值个数), sum(求和), mean(均值), standardDeviation(标准差), variance(方差), min(最小值), max(最大值), normL1(L1范数), normL2(L2范数)。

结果可以使用collectSummary获取TableSummary, 通过TableSummary获取对应的结果, 也可以直接打印。

另外, 对所有的BatchOp, 可以直接获取Op输出表的统计量。具体使用方式如下，

### 使用方式

* 打印统计结果.
    ```python
        summary = summarizer.linkFrom(source).collectSummary()
        print(summary)
    ```
  
* 获取相应的统计值
  ```python
      summary = summarizer.linkFrom(source).collectSummary()
      print(summary.sum('f_double'))
      print(summary.mean('f_double'))
      print(summary.variance('f_double'))
      print(summary.standardDeviation('f_double'))
      print(summary.min('f_double'))
      print(summary.max('f_double'))
      print(summary.normL1('f_double'))
      print(summary.normL2('f_double'))
      print(summary.numValidValue('f_double'))
      print(summary.numMissingValue('f_double'))
  ```
* 对Op的输出表做统计
    ```python
      source.lazyPrintStatistics()
      BatchOperator.execute()
   ```
  
* 获取Op输出表的TableSummary
    ```python
      summary = source..collectStatistics()
   ```



## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["a", 1, 1,2.0, True],
    ["c", 1, 2, -3.0, True],
    ["a", 2, 2,2.0, False],
    ["c", 0, 0, 0.0, False]
])
source = BatchOperator.fromDataframe(df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')

summarizer = SummarizerBatchOp()\
    .setSelectedCols(["f_long", "f_int", "f_double"])

summary = summarizer.linkFrom(source).collectSummary()

print(summary)
```

### Java 代码
```java
package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SummarizerBatchOpTest extends AlinkTestBase {

  @Test
  public void test() {
    Row[] testArray =
            new Row[] {
                    Row.of("a", 1L, 1, 2.0, true),
                    Row.of(null, 2L, 2, -3.0, true),
                    Row.of("c", null, null, 2.0, false),
                    Row.of("a", 0L, 0, null, null),
            };

    String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};

    MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

    SummarizerBatchOp summarizer = new SummarizerBatchOp()
            .setSelectedCols("f_double", "f_int");

    summarizer.linkFrom(source);

    TableSummary srt = summarizer.collectSummary();

    System.out.println(srt.toString());

    Assert.assertEquals(srt.getColNames().length, 2);
    Assert.assertEquals(srt.count(), 4);
    Assert.assertEquals(srt.numMissingValue("f_double"), 1, 10e-4);
    Assert.assertEquals(srt.numValidValue("f_double"), 3, 10e-4);
    Assert.assertEquals(srt.max("f_double"), 2.0, 10e-4);
    Assert.assertEquals(srt.min("f_int"), 0.0, 10e-4);
    Assert.assertEquals(srt.mean("f_double"), 0.3333333333333333, 10e-4);
    Assert.assertEquals(srt.variance("f_double"), 8.333333333333334, 10e-4);
    Assert.assertEquals(srt.standardDeviation("f_double"), 2.886751345948129, 10e-4);
    Assert.assertEquals(srt.normL1("f_double"), 7.0, 10e-4);
    Assert.assertEquals(srt.normL2("f_double"), 4.123105625617661, 10e-4);

  }
}
```
### 运行结果

Summary:

| colName|count|missing|sum|mean|variance|min|max|
|--------|-----|-------|---|----|--------|---|---|
|  f_long|    4|      0|  4|   1|  0.6667|  0|  2|
|   f_int|    4|      0|  5|1.25|  0.9167|  0|  2|
|f_double|    4|      0|  1|0.25|  5.5833| -3|  2|
