# 添加HBase数据 (LookupHBaseBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.LookupHBaseBatchOp

Python 类名：LookupHBaseBatchOp


## 功能介绍
LookupHBaseBatchOp ，将HBase中的数据取出。
读HBase Plugin版。plugin版本为1.2.12。
读数据时，指定HBase的zookeeper地址，表名称，列簇名称。指定rowkey列（可以是多列）和要读取的数据列和格式（可以写多列）。
在使用时，需要先下载插件，详情请看https://www.yuque.com/pinshu/alink_guide/czg4cx

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| familyName | 簇值 | 簇值 | String | ✓ |  |  |
| outputSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| pluginVersion | 插件版本号 | 插件版本号 | String | ✓ |  |  |
| rowKeyCols | rowkey所在列 | rowkey所在列 | String[] | ✓ |  |  |
| tableName | HBase表名称 | HBase表名称 | String | ✓ |  |  |
| zookeeperQuorum | Zookeeper quorum | Zookeeper quorum 地址 | String | ✓ |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| timeout | HBase RPC 超时时间 | HBase RPC 超时时间，单位毫秒 | Integer |  |  | 1000 |


## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
df = pd.DataFrame([
    ["1"],
    ["2"]
])

data = BatchOperator.fromDataframe(df, schemaStr='userid string')

lookupHBaseBatchOp = LookupHBaseBatchOp()\
    .setZookeeperQuorum("localhost:2181")\
    .setTableName("user")\
    .setRowKeyCols("userid")\
    .setFamilyName("color")\
    .setPluginVersion("1.2.12")\
    .setOutputSchemaStr("red long,black double,green int")
lookupHBaseBatchOp.linkFrom(data).print()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.LookupHBaseBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HBaseTest {

	@Test
	public void testReadBatch() throws Exception {
		List <Row> datas = Arrays.asList(
			Row.of("1"),
			Row.of("2")
		);
		BatchOperator op = new MemSourceBatchOp(datas, "userid string");
		LookupHBaseBatchOp lookupHBaseBatchOp = new LookupHBaseBatchOp()
			.setZookeeperQuorum("localhost:2181")
			.setTableName("user")
			.setRowKeyCols("userid")
			.setFamilyName("color")
			.setPluginVersion("1.2.12")
			.setOutputSchemaStr("red long,black double,green int");
		lookupHBaseBatchOp.linkFrom(op).print();
	}
}
```
