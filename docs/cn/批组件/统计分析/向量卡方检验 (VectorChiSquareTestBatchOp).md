# 向量卡方检验 (VectorChiSquareTestBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.VectorChiSquareTestBatchOp

Python 类名：VectorChiSquareTestBatchOp


## 功能介绍

针对vector数据，进行卡方检验

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |



## 代码示例
### Python 代码
无python接口
 
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.VectorChiSquareTestBatchOp;
import org.junit.Test;

import java.util.Arrays;

public class VectorChiSquareTestBatchOpTest {

	@Test
	public void testVectorChiSquareTestBatchOp() throws Exception {
		Row[] testArray = new Row[] {
			Row.of(7, "0.0  0.0  18.0  1.0", 1.0),
			Row.of(8, "0.0  1.0  12.0  0.0", 0.0),
			Row.of(9, "1.0  0.0  15.0  0.1", 0.0),
		};

		String[] colNames = new String[] {"id", "features", "clicked"};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		VectorChiSquareTestBatchOp test = new VectorChiSquareTestBatchOp()
			.setSelectedCol("features")
			.setLabelCol("clicked");

		test.linkFrom(source);

		test.lazyPrintChiSquareTest();

		BatchOperator.execute();
	}

}

```

### 运行结果

ChiSquareTest:

|col|     p|value| df|
|---|------|-----|---|
|  0|0.3865| 0.75|  1|
|  1|0.3865| 0.75|  1|
|  2|0.2231|    3|  2|
|  3|0.2231|    3|  2|


