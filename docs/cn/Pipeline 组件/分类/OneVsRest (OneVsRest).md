# OneVsRest (OneVsRest)
Java 类名：com.alibaba.alink.pipeline.classification.OneVsRest

Python 类名：OneVsRest


## 功能介绍

本组件用One VS Rest策略进行多分类。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| numClass | 类别数 | 多分类的类别数，必选 | Integer | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

lr = LogisticRegression() \
    .setFeatureCols(["sepal_length", "sepal_width", "petal_length", "petal_width"]) \
    .setLabelCol("category") \
    .setPredictionCol("pred_result") \
    .setMaxIter(100)

oneVsRest = OneVsRest().setClassifier(lr).setNumClass(3)
model = oneVsRest.fit(data)
model.setPredictionCol("pred_result").setPredictionDetailCol("pred_detail")
model.transform(data).print()
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.classification.OneVsRest;
import com.alibaba.alink.pipeline.classification.OneVsRestModel;
import org.junit.Test;

public class OneVsRestTest {
	@Test
	public void testOneVsRest() throws Exception {
		String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		String SCHEMA_STR
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		BatchOperator <?> data = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		LogisticRegression lr = new LogisticRegression()
			.setFeatureCols("sepal_length", "sepal_width", "petal_length", "petal_width")
			.setLabelCol("category")
			.setPredictionCol("pred_result")
			.setMaxIter(100);
		OneVsRest oneVsRest = new OneVsRest().setClassifier(lr).setNumClass(3);
		OneVsRestModel model = oneVsRest.fit(data);
		model.setPredictionCol("pred_result").setPredictionDetailCol("pred_detail");
		model.transform(data).print();
	}
}
```

### 运行结果

sepal_length|sepal_width|petal_length|petal_width|category|pred_result|pred_detail
------------|-----------|------------|-----------|--------|-----------|-----------
6.7000|3.1000|4.4000|1.4000|Iris-versicolor|Iris-versicolor|{"Iris-versicolor":0.9999890601537083,"Iris-virginica":1.0939842119301402E-5,"Iris-setosa":4.1724971938972156E-12}
5.4000|3.0000|4.5000|1.5000|Iris-versicolor|Iris-versicolor|{"Iris-versicolor":0.9939699721610056,"Iris-virginica":0.006030026623291463,"Iris-setosa":1.2157029667713158E-9}
5.4000|3.9000|1.7000|0.4000|Iris-setosa|Iris-setosa|{"Iris-versicolor":0.02236524089333592,"Iris-virginica":0.0,"Iris-setosa":0.9776347591066641}
5.0000|3.4000|1.6000|0.4000|Iris-setosa|Iris-setosa|{"Iris-versicolor":0.07720412400682967,"Iris-virginica":0.0,"Iris-setosa":0.9227958759931704}
5.6000|3.0000|4.5000|1.5000|Iris-versicolor|Iris-versicolor|{"Iris-versicolor":0.9961816818708689,"Iris-virginica":0.003818317908880254,"Iris-setosa":2.2025091271297693E-10}
...   | ...  | ... |   ... | ...           |...            |...           |
