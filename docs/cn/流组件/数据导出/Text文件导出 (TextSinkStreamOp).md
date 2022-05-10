# Text文件导出 (TextSinkStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sink.TextSinkStreamOp

Python 类名：TextSinkStreamOp


## 功能介绍
按行写出到文件。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| numFiles | 文件数目 | 文件数目 | Integer |  |  | 1 |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |

## 代码示例
### Python 代码
** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**
```python
df = pd.DataFrame([
                ["0L", "1L", 0.6],
                ["2L", "2L", 0.8],
                ["2L", "4L", 0.6],
                ["3L", "1L", 0.6],
                ["3L", "2L", 0.3],
                ["3L", "4L", 0.4]
        ])

data = StreamOperator.fromDataframe(df, schemaStr='uid string, iid string, label double')

sink = TextSinkStreamOp().setFilePath('yourFilePath').setOverwriteSink(True)
data.link(sink)
StreamOperator.execute()
```
### Java 代码
** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.TextSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import org.junit.Test;

public class TextSinkStreamOpTest {
	@Test
	public void testTextSinkStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
        			Row.of("0L", "1L", 0.6),
        			Row.of("2L", "2L", 0.8),
        			Row.of("2L", "4L", 0.6),
        			Row.of("3L", "1L", 0.6),
        			Row.of("3L", "2L", 0.3),
        			Row.of("3L", "4L", 0.4)
        		);
		StreamOperator <?> data = new MemSourceStreamOp(df, "uid string, iid string, label double");
		StreamOperator <?> sink = new TextSinkStreamOp().setFilePath("yourFilePath").setOverwriteSink(true);
		data.link(sink);
		StreamOperator.execute();
	}
}
```
