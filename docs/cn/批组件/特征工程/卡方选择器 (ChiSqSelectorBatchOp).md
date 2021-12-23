# 卡方选择器 (ChiSqSelectorBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.ChiSqSelectorBatchOp

Python 类名：ChiSqSelectorBatchOp


## 功能介绍

针对table数据，进行特征筛选

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| selectorType | 筛选类型 | 筛选类型，包含"NumTopFeatures","percentile", "fpr", "fdr", "fwe"五种。 | String |  | "NumTopFeatures" |
| numTopFeatures | 最大的p-value列个数 | 最大的p-value列个数, 默认值50 | Integer |  | 50 |
| percentile | 筛选的百分比 | 筛选的百分比，默认值0.1 | Double |  | 0.1 |
| fpr | p value的阈值 | p value的阈值，默认值0.05 | Double |  | 0.05 |
| fdr | 发现阈值 | 发现阈值, 默认值0.05 | Double |  | 0.05 |
| fwe | 错误率阈值 | 错误率阈值, 默认值0.05 | Double |  | 0.05 |


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

selector = ChiSqSelectorBatchOp()\
            .setSelectedCols(["f_string", "f_long", "f_int", "f_double"])\
            .setLabelCol("f_boolean")\
            .setNumTopFeatures(2)

selector.linkFrom(source)

modelInfo: ChisqSelectorModelInfo = selector.collectModelInfo()
        
print(modelInfo.getColNames())


```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.ChiSqSelectorBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.feature.ChisqSelectorModelInfo;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ChiSqSelectorBatchOpTest {
	@Test
	public void testChiSqSelectorBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 1L, 1, 2.0, true),
			Row.of("c", 1L, 2, -3.0, true),
			Row.of("a", 2L, 2, 2.0, false),
			Row.of("c", 0L, 0, 0.0, false)
		);
		BatchOperator <?> source = new MemSourceBatchOp(df,
			"f_string string, f_long long, f_int int, f_double double, f_boolean boolean");
		ChiSqSelectorBatchOp selector = new ChiSqSelectorBatchOp()
			.setSelectedCols("f_string", "f_long", "f_int", "f_double")
			.setLabelCol("f_boolean")
			.setNumTopFeatures(2);
		selector.linkFrom(source);
		ChisqSelectorModelInfo modelInfo = selector.collectModelInfo();
		System.out.println(modelInfo.toString());
	}
}
```

### 运行结果

```
------------------------- ChisqSelectorModelInfo -------------------------
Number of Selector Features: 2
Number of Features: 4
Type of Selector: NumTopFeatures
Number of Top Features: 2
Selector Indices: 
    | ColName|ChiSquare|PValue| DF|Selected|
    |--------|---------|------|---|--------|
    |  f_long|        4|0.1353|  2|    true|
    |   f_int|        2|0.3679|  2|    true|
    |f_double|        2|0.3679|  2|   false|
    |f_string|        0|     1|  1|   false|
```



