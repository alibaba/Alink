# 卡方检验 (ChiSquareTestBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.ChiSquareTestBatchOp

Python 类名：ChiSquareTestBatchOp


## 功能介绍

卡法独立性检验是检验两个因素（各有两项或以上的分类）之间是否相互影响的问题，其零假设是两因素之间相互独立。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
            ['a1','b1','c1'],
            ['a1','b2','c1'],
            ['a1','b1','c2'],
            ['a2','b1','c1'],
            ['a2','b2','c2'],
            ['a2', 'b1','c1']
])

batchData = BatchOperator.fromDataframe(df, schemaStr='x1 string, x2 string, x3 string')

chisqTest = ChiSquareTestBatchOp()\
            .setSelectedCols(["x1","x2"])\
            .setLabelCol("x3")

batchData.link(chisqTest).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.ChiSquareTestBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ChiSquareTestBatchOpTest {
	@Test
	public void testChiSquareTestBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a1", "b1", "c1"),
			Row.of("a1", "b2", "c1"),
			Row.of("a1", "b1", "c2"),
			Row.of("a2", "b1", "c1"),
			Row.of("a2", "b2", "c2"),
			Row.of("a2", "b1", "c1")
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df, "x1 string, x2 string, x3 string");
		BatchOperator <?> chisqTest = new ChiSquareTestBatchOp()
			.setSelectedCols("x1", "x2")
			.setLabelCol("x3");
		batchData.link(chisqTest).print();
	}
}
```
### 运行结果

col|chi2_result
-----|-----------
x1|{"comment":"pearson test","df":1.0,"p":1.0,"value":0.0}
x2|{"comment":"pearson test","df":1.0,"p":0.5402913746074196,"value":0.37500000000000006}



