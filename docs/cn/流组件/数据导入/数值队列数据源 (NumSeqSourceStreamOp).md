# 数值队列数据源 (NumSeqSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.NumSeqSourceStreamOp

Python 类名：NumSeqSourceStreamOp


## 功能介绍
生成连续整数的表

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |



## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
无，仅在Java中使用


### Java 代码

```java
package javatest.com.alibaba.alink.stream.source;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.NumSeqSourceStreamOp;
import org.junit.Test;

public class NumSeqSourceStreamOpTest {
	@Test
	public void testMemSourceStreamOp() throws Exception {
		NumSeqSourceStreamOp streamData = new NumSeqSourceStreamOp(0,1,0.1);
		streamData.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

|num
|---
|0
|1
