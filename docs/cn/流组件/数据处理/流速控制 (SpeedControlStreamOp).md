# 流速控制 (SpeedControlStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.SpeedControlStreamOp

Python 类名：SpeedControlStreamOp


## 功能介绍
该组件能够控制数据流的速度，在每一条数据流过后暂停一段时间（参数指定）或者每毫秒流过多少条样本（参数指定）。算法测试时经常用来构造数据流。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| timeInterval | 时间间隔 | 数据流流动过程中两条样本间的时间间隔，单位秒 | Double |  |  | 0.001 |



## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
df = pd.DataFrame([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])

streamData = StreamOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')

streamData.link(SpeedControlStreamOp().setTimeInterval(1.)).print()

StreamOperator.execute()
```

### Java 代码
```java
package benchmark.online;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.SpeedControlStreamOp;
import com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp;
import org.junit.Test;

public class SpeedControlTest {

	@Test
	public void onlineTrainAndFilter() throws Exception {
		StreamOperator<?> streamData = new RandomTableSourceStreamOp().setNumCols(2).setMaxRows(10L);
		streamData.link(new SpeedControlStreamOp().setTimeInterval(1.)).print();
		StreamOperator.execute();
	}
}
```

### 运行结果
f0 | f1 | label 
---|----|-------
 2 |  1     | 1 
   3 |  2    |  1 
   4 |  3    |  2 
   2 |  4    |  1 
   2 |  2    |  1 
   4 |  3    |  2 
   1 |  2    |  1 
   5 |  3    |  3 





