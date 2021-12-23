# 决策树模型编码 (TreeModelEncoderBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.TreeModelEncoderBatchOp

Python 类名：TreeModelEncoderBatchOp


## 功能介绍
使用树模型，将输入数据编码为特征。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.0, "A", 0, 0, 0],
    [2.0, "B", 1, 1, 0],
    [3.0, "C", 2, 2, 1],
    [4.0, "D", 3, 3, 1]
])

batchSource = BatchOperator.fromDataframe(
    df, schemaStr='f0 double, f1 string, f2 int, f3 int, label int')
streamSource = StreamOperator.fromDataframe(
    df, schemaStr='f0 double, f1 string, f2 int, f3 int, label int')

gbdtTrainOp = GbdtTrainBatchOp()\
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
    .setLabelCol('label')\
    .linkFrom(batchSource)

encoderBatchOp = TreeModelEncoderBatchOp()\
    .setPredictionCol("encoded_features")
encoderStreamOp = TreeModelEncoderStreamOp(gbdtTrainOp)\
    .setPredictionCol("encoded_features")

encoderBatchOp.linkFrom(gbdtTrainOp, batchSource).print()
encoderStreamOp.linkFrom(streamSource).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.TreeModelEncoderBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.TreeModelEncoderStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;

import com.alibaba.alink.operator.batch.classification.GbdtTrainBatchOp;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TreeModelEncoderBatchOpTest {
	@Test
	public void testTreeModelEncoderBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.0, "A", 0, 0, 0),
			Row.of(2.0, "B", 1, 1, 0),
			Row.of(3.0, "C", 2, 2, 1),
			Row.of(4.0, "D", 3, 3, 1)
		);

		BatchOperator batchSource = new MemSourceBatchOp(
			df, "f0 double, f1 string, f2 int, f3 int, label int");
		StreamOperator streamSource = new MemSourceStreamOp(
			df, "f0 double, f1 string, f2 int, f3 int, label int");

		BatchOperator gbdtTrainOp = new GbdtTrainBatchOp()
			.setFeatureCols(new String[] {"f0", "f1", "f2", "f3"})
			.setLabelCol("label")
			.linkFrom(batchSource);

		BatchOperator encoderBatchOp = new TreeModelEncoderBatchOp()
			.setPredictionCol("encoded_features");
		StreamOperator encoderStreamOp =new TreeModelEncoderStreamOp(gbdtTrainOp)
			.setPredictionCol("encoded_features");

		encoderBatchOp.linkFrom(gbdtTrainOp, batchSource).print();
		encoderStreamOp.linkFrom(streamSource).print();

		StreamOperator.execute();
	}
}
```

### 运行结果
f0|f1|f2|f3|label|encoded_features
---|---|---|---|-----|----------------
1.0000|A|0|0|0|$123$0:1.0 2:1.0 4:1.0 6:1.0 8:1.0 10:1.0 12:1.0 14:1.0 16:1.0 18:1.0 20:1.0 22:1.0 24:1.0 26:1.0 28:1.0 30:1.0 32:1.0 34:1.0 36:1.0 38:1.0 40:1.0 42:1.0 44:1.0 46:1.0 47:1.0 48:1.0 49:1.0 50:1.0 51:1.0 52:1.0 53:1.0 54:1.0 55:1.0 56:1.0 57:1.0 58:1.0 59:1.0 60:1.0 61:1.0 62:1.0 63:1.0 64:1.0 65:1.0 66:1.0 67:1.0 68:1.0 69:1.0 70:1.0 71:1.0 72:1.0 73:1.0 74:1.0 75:1.0 76:1.0 77:1.0 78:1.0 79:1.0 80:1.0 81:1.0 82:1.0 83:1.0 84:1.0 85:1.0 86:1.0 87:1.0 88:1.0 89:1.0 90:1.0 91:1.0 92:1.0 93:1.0 94:1.0 95:1.0 96:1.0 97:1.0 98:1.0 99:1.0 100:1.0 101:1.0 102:1.0 103:1.0 104:1.0 105:1.0 106:1.0 107:1.0 108:1.0 109:1.0 110:1.0 111:1.0 112:1.0 113:1.0 114:1.0 115:1.0 116:1.0 117:1.0 118:1.0 119:1.0 120:1.0 121:1.0 122:1.0
2.0000|B|1|1|0|$123$0:1.0 2:1.0 4:1.0 6:1.0 8:1.0 10:1.0 12:1.0 14:1.0 16:1.0 18:1.0 20:1.0 22:1.0 24:1.0 26:1.0 28:1.0 30:1.0 32:1.0 34:1.0 36:1.0 38:1.0 40:1.0 42:1.0 44:1.0 46:1.0 47:1.0 48:1.0 49:1.0 50:1.0 51:1.0 52:1.0 53:1.0 54:1.0 55:1.0 56:1.0 57:1.0 58:1.0 59:1.0 60:1.0 61:1.0 62:1.0 63:1.0 64:1.0 65:1.0 66:1.0 67:1.0 68:1.0 69:1.0 70:1.0 71:1.0 72:1.0 73:1.0 74:1.0 75:1.0 76:1.0 77:1.0 78:1.0 79:1.0 80:1.0 81:1.0 82:1.0 83:1.0 84:1.0 85:1.0 86:1.0 87:1.0 88:1.0 89:1.0 90:1.0 91:1.0 92:1.0 93:1.0 94:1.0 95:1.0 96:1.0 97:1.0 98:1.0 99:1.0 100:1.0 101:1.0 102:1.0 103:1.0 104:1.0 105:1.0 106:1.0 107:1.0 108:1.0 109:1.0 110:1.0 111:1.0 112:1.0 113:1.0 114:1.0 115:1.0 116:1.0 117:1.0 118:1.0 119:1.0 120:1.0 121:1.0 122:1.0
3.0000|C|2|2|1|$123$1:1.0 3:1.0 5:1.0 7:1.0 9:1.0 11:1.0 13:1.0 15:1.0 17:1.0 19:1.0 21:1.0 23:1.0 25:1.0 27:1.0 29:1.0 31:1.0 33:1.0 35:1.0 37:1.0 39:1.0 41:1.0 43:1.0 45:1.0 46:1.0 47:1.0 48:1.0 49:1.0 50:1.0 51:1.0 52:1.0 53:1.0 54:1.0 55:1.0 56:1.0 57:1.0 58:1.0 59:1.0 60:1.0 61:1.0 62:1.0 63:1.0 64:1.0 65:1.0 66:1.0 67:1.0 68:1.0 69:1.0 70:1.0 71:1.0 72:1.0 73:1.0 74:1.0 75:1.0 76:1.0 77:1.0 78:1.0 79:1.0 80:1.0 81:1.0 82:1.0 83:1.0 84:1.0 85:1.0 86:1.0 87:1.0 88:1.0 89:1.0 90:1.0 91:1.0 92:1.0 93:1.0 94:1.0 95:1.0 96:1.0 97:1.0 98:1.0 99:1.0 100:1.0 101:1.0 102:1.0 103:1.0 104:1.0 105:1.0 106:1.0 107:1.0 108:1.0 109:1.0 110:1.0 111:1.0 112:1.0 113:1.0 114:1.0 115:1.0 116:1.0 117:1.0 118:1.0 119:1.0 120:1.0 121:1.0 122:1.0
4.0000|D|3|3|1|$123$1:1.0 3:1.0 5:1.0 7:1.0 9:1.0 11:1.0 13:1.0 15:1.0 17:1.0 19:1.0 21:1.0 23:1.0 25:1.0 27:1.0 29:1.0 31:1.0 33:1.0 35:1.0 37:1.0 39:1.0 41:1.0 43:1.0 45:1.0 46:1.0 47:1.0 48:1.0 49:1.0 50:1.0 51:1.0 52:1.0 53:1.0 54:1.0 55:1.0 56:1.0 57:1.0 58:1.0 59:1.0 60:1.0 61:1.0 62:1.0 63:1.0 64:1.0 65:1.0 66:1.0 67:1.0 68:1.0 69:1.0 70:1.0 71:1.0 72:1.0 73:1.0 74:1.0 75:1.0 76:1.0 77:1.0 78:1.0 79:1.0 80:1.0 81:1.0 82:1.0 83:1.0 84:1.0 85:1.0 86:1.0 87:1.0 88:1.0 89:1.0 90:1.0 91:1.0 92:1.0 93:1.0 94:1.0 95:1.0 96:1.0 97:1.0 98:1.0 99:1.0 100:1.0 101:1.0 102:1.0 103:1.0 104:1.0 105:1.0 106:1.0 107:1.0 108:1.0 109:1.0 110:1.0 111:1.0 112:1.0 113:1.0 114:1.0 115:1.0 116:1.0 117:1.0 118:1.0 119:1.0 120:1.0 121:1.0 122:1.0
