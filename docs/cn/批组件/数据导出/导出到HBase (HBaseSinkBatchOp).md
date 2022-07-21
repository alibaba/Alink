# 导出到HBase (HBaseSinkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sink.HBaseSinkBatchOp

Python 类名：HBaseSinkBatchOp


## 功能介绍
写HBase Plugin版。plugin版本为1.2.12。
写入时，指定HBase的zookeeper地址，表名称，列簇名称。指定rowkey列（可以是多列）和要写入的数据列（可以写多列）。
在使用时，需要先下载插件，详情请看https://www.yuque.com/pinshu/alink_guide/czg4cx

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| familyName | 簇值 | 簇值 | String | ✓ |  |  |
| pluginVersion | 插件版本号 | 插件版本号 | String | ✓ |  |  |
| rowKeyCols | rowkey所在列 | rowkey所在列 | String[] | ✓ |  |  |
| tableName | HBase表名称 | HBase表名称 | String | ✓ |  |  |
| zookeeperQuorum | Zookeeper quorum | Zookeeper quorum 地址 | String | ✓ |  |  |
| timeout | HBase RPC 超时时间 | HBase RPC 超时时间，单位毫秒 | Integer |  |  | 1000 |
| valueCols | 多数值列 | 多数值列 | String[] |  |  | null |


## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
df = pd.DataFrame([
    ["1", 10000, 10001.0, 10002],
    ["2", 20000, 20001.0, 20002]
])

data = BatchOperator.fromDataframe(df, schemaStr='userid string,red int,black double,green int')

HBaseSinkBatchOp()\
    .setZookeeperQuorum("localhost:2181")\
    .setTableName("user")\
    .setRowKeyCols("userid")\
    .setFamilyName("color")\
    .setPluginVersion("1.2.12")\
    .setValueCols("red", "black","green")\
    .linkFrom(data)
BatchOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.HBaseSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HBaseTest {
	@Test
	public void testWriteStream() throws Exception {
		List <Row> datas = Arrays.asList(
			Row.of("1", 10000L, 10001.0, 10002),
			Row.of("2", 20000L, 20001.0, 20002)
		);
		BatchOperator op = new MemSourceBatchOp(datas, "userid string,red long,black double,green int");
		HBaseSinkBatchOp hBaseSinkBatchOp = new HBaseSinkBatchOp()
			.setZookeeperQuorum("localhost:2181")
			.setTableName("user")
			.setRowKeyCols("userid")
			.setFamilyName("color")
			.setPluginVersion("1.2.12")
			.setValueCols("red", "black","green");
		hBaseSinkBatchOp.linkFrom(op);
		BatchOperator.execute();
	}
}
```
