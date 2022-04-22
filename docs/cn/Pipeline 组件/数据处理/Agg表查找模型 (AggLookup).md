# Agg表查找模型 (AggLookup)
Java 类名：com.alibaba.alink.pipeline.dataproc.AggLookup

Python 类名：AggLookup


## 功能介绍
需要查找多个值，并统计结果的总和、平均值、最大最小值或拼接查询结果时，可以使用聚合查找，该组件有两个输入，依次是模型数据表和原始数据表。模型数据有两列，依次是String类型和DenseVector类型，原始数据有任意行和列，每列都是String类型。原始数据默认使用空格作为单词的分隔符。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |  |
| delimiter | 分隔符 | 用来分割字符串 | String |  |  | " " |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["the quality of the word vectors increases"],
    ["amount of the training data increases"],
    ["the training speed is significantly improved"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='sentence string')

df2 = pd.DataFrame([
    ["the", "0.6343,0.8561,0.1249,0.4701"],
    ["training", "0.2753,0.2444,0.3699,0.6048"],
    ["of", "0.3160,0.3675,0.1649,0.4116"],
    ["increases", "1.0372,0.6092,0.1050,0.2630"],
    ["word", "0.9911,0.6338,0.4570,0.8451"],
    ["vectors", "0.8780,0.4500,0.5455,0.7495"],
    ["speed", "0.9504,0.3168,0.7484,0.6965"],
    ["significantly", "-0.0465,0.6597,0.0906,0.7137"],
    ["quality", "0.9745,0.7521,0.8874,0.5192"],
    ["is", "0.8221,0.0487,-0.0065,0.4088"],
    ["improved", "0.1910,0.0723,0.8216,0.4367"],
    ["data", "0.8985,0.0117,0.8083,0.9636"],
    ["amount", "0.9786,0.1470,0.7385,0.8856"]
])

modelOp = BatchOperator.fromDataframe(df2, schemaStr="id string, vec string")

AggLookup()\
    .setModelData(modelOp) \
    .setClause("CONCAT(sentence,2) as concat, AVG(sentence) as avg, SUM(sentence) as sum,MAX(sentence) as max,MIN(sentence) as min") \
    .setDelimiter(" ") \
    .setReservedCols([]) \
    .transform(inOp)\
    .print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.AggLookup;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggLookupTest { 
    @Test
    public void testAggLookup() throws Exception {
        List <Row> df = Arrays.asList(
            Row.of("the quality of the word vectors increases"),
            Row.of("amount of the training data increases"),
            Row.of("the training speed is significantly improved")
        );
        BatchOperator <?> inOp = new MemSourceBatchOp(df, "sentence string");
        List <Row> df2 = Arrays.asList(
            Row.of("the", "0.6343,0.8561,0.1249,0.4701"),
            Row.of("training", "0.2753,0.2444,0.3699,0.6048"),
            Row.of("of", "0.3160,0.3675,0.1649,0.4116"),
            Row.of("increases", "1.0372,0.6092,0.1050,0.2630"),
            Row.of("word", "0.9911,0.6338,0.4570,0.8451"),
            Row.of("vectors", "0.8780,0.4500,0.5455,0.7495"),
            Row.of("speed", "0.9504,0.3168,0.7484,0.6965"),
            Row.of("significantly", "-0.0465,0.6597,0.0906,0.7137"),
            Row.of("quality", "0.9745,0.7521,0.8874,0.5192"),
            Row.of("is", "0.8221,0.0487,-0.0065,0.4088"),
            Row.of("improved", "0.1910,0.0723,0.8216,0.4367"),
            Row.of("data", "0.8985,0.0117,0.8083,0.9636"),
            Row.of("amount", "0.9786,0.1470,0.7385,0.8856")
        );
        BatchOperator <?> modelOp = new MemSourceBatchOp(df2, "id string, vec string");
        new AggLookup()
            .setModelData(modelOp)
            .setClause("CONCAT(sentence,2) as concat, AVG(sentence) as avg, SUM(sentence) as sum,MAX(sentence) as max,MIN(sentence) as min")
            .setDelimiter(" ")
            .transform(inOp)
            .print();
    }
}
```

### 脚本输出结果
|concat|
|---|
|0.6343 0.8561 0.1249 0.4701 0.9745 0.7521 0.8874 0.5192|
|0.9786 0.147 0.7385 0.8856 0.316 0.3675 0.1649 0.4116|
|0.6343 0.8561 0.1249 0.4701 0.2753 0.2444 0.3699 0.6048|

avg|sum|max|min
---|---|---|---
0.7807 0.6464 0.3442 0.5326|5.4654 4.5248 2.4096 3.7286|1.0372 0.8561 0.8874 0.8451|0.316 0.3675 0.105 0.263
0.6899 0.3726 0.3852 0.5997|4.1399 2.2359 2.3115 3.5987|1.0372 0.8561 0.8083 0.9636|0.2753 0.0117 0.105 0.263
0.4710 0.3663 0.3581 0.5550|2.8266 2.1980 2.1489 3.3306|0.9504 0.8561 0.8216 0.7137|-0.0465 0.0487 -0.0065 0.4088
