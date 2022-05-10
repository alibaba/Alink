# 数值队列数据源 (NumSeqSourceBatchOp)
Java 类名：com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp

Python 类名：NumSeqSourceBatchOp


## 功能介绍
生成连续整数的表

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| from | 开始 | 开始 | Long | ✓ |  |  |
| to | 截止 | 截止 | Long | ✓ |  |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |


## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
无，仅在Java中使用

### Java 代码
```java
import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import org.junit.Test;

public class NumSeqSourceBatchOpTest {
	@Test
	public void testMemSourceBatchOp() throws Exception {
		NumSeqSourceBatchOp batchData = new NumSeqSourceBatchOp()
			.setFrom(0)
			.setTo(1);
		batchData.print();
	}
}

```

### 运行结果
|num|
|---|
|0|
|1|



