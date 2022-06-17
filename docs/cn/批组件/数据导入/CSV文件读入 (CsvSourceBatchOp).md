# CSV文件读入 (CsvSourceBatchOp)
Java 类名：com.alibaba.alink.operator.batch.source.CsvSourceBatchOp

Python 类名：CsvSourceBatchOp


## 功能介绍
读CSV文件。支持从本地、hdfs、http读取
### 分区选择
分区目录名格式为"分区名=值"，例如： city=beijing/month=06/day=17;city=hangzhou/month=06/day=18。
Alink将遍历目录下的分区名和分区值，构造分区表：

city | month | day
---|---|---
beijing | 06 | 17
hangzhou | 06 | 18

使用SQL语句查找分区，例如：CsvSourceBatchOp.setPartitions("city = 'beijing'")，分区选择语法参考[《Flink SQL 内置函数》](https://www.yuque.com/pinshu/alink_tutorial/list_sql_function)，分区值为String类型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| fieldDelimiter | 字段分隔符 | 字段分隔符 | String |  |  | "," |
| handleInvalidMethod | 处理无效值的方法 | 处理无效值的方法，可取 error, skip | String |  | "ERROR", "SKIP" | "ERROR" |
| ignoreFirstLine | 是否忽略第一行数据 | 是否忽略第一行数据 | Boolean |  |  | false |
| lenient | 是否容错 | 若为true，当解析失败时丢弃该数据；若为false，解析失败是抛异常 | Boolean |  |  | false |
| partitions | 分区名 | 1)单级、单个分区示例：ds=20190729；2)多级分区之间用" / "分隔，例如：ds=20190729/dt=12； 3)多个分区之间用","分隔，例如：ds=20190729,ds=20190730 | String |  |  | null |
| quoteChar | 引号字符 | 引号字符 | Character |  |  | "\"" |
| rowDelimiter | 行分隔符 | 行分隔符 | String |  |  | "\n" |
| skipBlankLine | 是否忽略空行 | 是否忽略空行 | Boolean |  |  | true |

#### 支持的字段类型包括：

| 字段类型 | 描述 | 值域 | Flink类型 | Java类型 |
| --- | --- | --- | --- | --- |
| VARCHAR/STRING | 可变长度字符串 | 最大容量为4mb | Types.STRING | java.lang.String |
| BOOLEAN | 逻辑值 | 值：TRUE，FALSE，UNKNOWN | Types.BOOLEAN | java.lang.Boolean |
| TINYINT | 微整型，1字节整数 | 范围是-128到127 | Types.BYTE | java.lang.Byte |
| SMALLINT | 短整型，2字节整数 | 范围为-32768至32767 | Types.SHORT | java.lang.Short |
| INT | 整型，4字节整数 | 范围是-2147483648到2147483647 | Types.INT | java.lang.Integer |
| BIGINT/LONG | 长整型，8字节整数 | 范围是-9223372036854775808至9223372036854775807 | Types.LONG | java.lang.Long |
| FLOAT | 4字节浮点数 | 6位数字精度 | Types.FLOAT | java.lang.Float |
| DOUBLE | 8字节浮点数 | 15位十进制精度 | Types.DOUBLE | java.lang.Double |
| DECIMAL | 小数类型 | 示例：123.45是DECIMAL(5,2)值 | Types.DECIMAL | java.math.BigDecimal |
| DATE | 日期 | 示例：'1969-07-20' | Types.SQL_DATE | java.sql.Date |
| TIME | 时间 | 示例：'20:17:40' | Types.SQL_TIME | java.sql.Time |
| TIMESTAMP | 时间戳，日期和时间 | 示例：'1969-07-20 20:17:40' | Types.SQL_TIMESTAMP | java.sql.Timestamp |

#### 关于分隔符的说明：

Web前端支持用户输入如下转义字符和unicode字符作为分隔符：

| 输入分隔符 | 含义 |
| --- | --- |
| \\t | 制表符(tab键) |
| \\n | 换行符 |
| \\b | 退格符 |
| \\r | 回车 |
| \\f | 换页 |
| \\\\ | 反斜线字符 |
| ' | 单引号 |
| " | 双引号 |
| \\ddd | 1到3位八进制数所代表的任意字符，例如\\001表示'ctrl + A', \\40表示空格符 |
| \\udddd | 1到4位十六进制数所代表unicode字符，例如\\u0001表示'ctrl + A', \\u0020表示空格符 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

filePath = 'https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv'
schema = 'sepal_length double, sepal_width double, petal_length double, petal_width double, category string'
csvSource = CsvSourceBatchOp()\
    .setFilePath(filePath)\
    .setSchemaStr(schema)\
    .setFieldDelimiter(",")
BatchOperator.collectToDataframe(csvSource)
```

### Java 代码
```java
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import org.junit.Test;

public class CsvSourceBatchOpTest {

	@Test
	public void testCsvSourceBatchOp() throws Exception {
		String filePath = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		String schema
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		CsvSourceBatchOp csvSource = new CsvSourceBatchOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setFieldDelimiter(",");
		csvSource.print();
	}
}

```

### 运行结果

sepal_length|sepal_width|petal_length|petal_width|category
------------|-----------|------------|-----------|--------
5.0000|3.2000|1.2000|0.2000|Iris-setosa
6.6000|3.0000|4.4000|1.4000|Iris-versicolor
5.4000|3.9000|1.3000|0.4000|Iris-setosa
5.0000|2.3000|3.3000|1.0000|Iris-versicolor
5.5000|3.5000|1.3000|0.2000|Iris-setosa
6.2000|3.4000|5.4000|2.3000|Iris-virginica



