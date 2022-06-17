# Text文件导出 (TextSinkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sink.TextSinkBatchOp

Python 类名：TextSinkBatchOp


## 功能介绍
按行写出到文件。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| numFiles | 文件数目 | 文件数目 | Integer |  |  | 1 |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| partitionCols | 分区列 | 创建分区使用的列名 | String[] |  |  | null |

## 代码示例

### Python 代码
** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**
```python
df_data = pd.DataFrame([
   ['changjiang', 2000, 1.5],
    ['huanghe', 2001, 1.7],
    ['zhujiang', 2002, 3.6],
    ['changjiang', 2001, 2.4],
    ['huanghe', 2002, 2.9],
    ['zhujiang', 2003, 3.2]
])
    
batch_data = BatchOperator.fromDataframe(df_data, schemaStr='f1 string, f2 bigint, f3 double')

TextSinkBatchOp().setFilePath('yourFilePath').setOverwriteSink(True).linkFrom(batch_data)
BatchOperator.execute()
```
### Java 代码
** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sink.TextSinkBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TextSinkBatchOpTest {
	@Test
	public void testTextSinkBatchOp() throws Exception {
		List <Row> inputRows = Arrays.asList(
			Row.of("changjiang", 2000, 1.5),
        	Row.of("huanghe", 2001, 1.7),
        	Row.of("zhujiang", 2002, 3.6),
       		Row.of("changjiang", 2001, 2.4),
   			Row.of("huanghe", 2002, 2.9),
  			Row.of("zhujiang", 2003, 3.2)
        );
        
        BatchOperator <?> dataOp = new MemSourceBatchOp(inputRows, "f1 string, f2 int, f3 double");

		new TextSinkBatchOp()
            .setFilePath("yourFilePath")
            .setOverwriteSink(true)
            .linkFrom(dataOp);
		dataOp.link(sink);
		BatchOperator.execute();
	}
}
```

### 运行结果
