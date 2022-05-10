# parquet文件读入 (ParquetSourceBatchOp)
Java 类名：com.alibaba.alink.operator.batch.source.ParquetSourceBatchOp

Python 类名：ParquetSourceBatchOp


## 功能介绍
读parquet文件数据。支持从本地、hdfs、http读取

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
filePath = 'https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.parquet'
parquetSource = ParquetSourceStreamOp()\
    .setFilePath(filePath)
parquetSource.print()
StreamOperator.execute()
```
### Java 代码
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import org.junit.Test;

public class ParquetSourceStreamOpTest {
	@Test
	public void testParquetSourceStreamOp() throws Exception {
		PluginDownloader downloader = AlinkGlobalConfiguration.getPluginDownloader();
		downloader.downloadPlugin("parquet");
		String parquetName = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.parquet";
		ParquetSourceBatchOp source = new ParquetSourceBatchOp()
			.setFilePath(new FilePath(parquetName, new HttpFileReadOnlyFileSystem()));
		source.print();
	}
}
```

### 运行结果

class|f0|f1|f2|f3
-----|---|---|---|---
Iris-setosa|5.1000|3.5000|1.4000|0.2000
Iris-versicolor|5.0000|2.0000|3.5000|1.0000
Iris-setosa|5.1000|3.7000|1.5000|0.4000
Iris-virginica|6.4000|2.8000|5.6000|2.2000
Iris-versicolor|6.0000|2.9000|4.5000|1.5000
|......|
Iris-setosa|4.9000|3.0000|1.4000|0.2000
Iris-versicolor|5.7000|2.6000|3.5000|1.0000
Iris-setosa|4.6000|3.6000|1.0000|0.2000
Iris-versicolor|5.9000|3.0000|4.2000|1.5000
