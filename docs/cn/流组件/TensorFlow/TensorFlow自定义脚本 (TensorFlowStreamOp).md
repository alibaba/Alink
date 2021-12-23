# TensorFlow自定义脚本 (TensorFlowStreamOp)
Java 类名：com.alibaba.alink.operator.stream.tensorflow.TensorFlowStreamOp

Python 类名：TensorFlowStreamOp


## 功能介绍

该组件支持用户传入 TensorFlow 脚本，使用传入的流数据进行任意处理，并可以将数据输出回 Alink 端。

用户需要提供自己编写的 TensorFlow 脚本文件。
脚本的编写需要依赖 akdl 库，可以参考 ```alink_dl_predictors/predictor-tf/src/test/resources/tf_dnn_stream.py```。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| mainScriptFile | 主脚本文件路径 | 主脚本文件路径，需要是参数 userFiles 中的一项，并且包含 main 函数 | String | ✓ |  |
| outputSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如 "f0 string, f1 bigint, f2 double" | String | ✓ |  |
| userFiles | 所有自定义脚本文件的路径 | 所有自定义脚本文件的路径 | String | ✓ |  |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  | 4 |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  | null |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。
 如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；
 如果是目录，那么只能使用本地路径，即 file://。 | String |  | "" |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | null |
| userParams | 自定义参数 | 用户自定义参数，JSON 字典格式的字符串 | String |  | "{}" |


### 脚本路径说明

脚本路径可以是以下形式：
  - 本地文件：```file://``` 加绝对路径，例如 ```file:///tmp/dnn.py```；
  - Java 包中的资源文件：```res://``` 加路径，例如 ```res:///dnn.py```；
  - http/https 文件：```http://``` 或 ```https://``` 路径；
  - OSS 文件：```oss://``` 加路径和 Endpoint 和 access key 等信息，例如```oss://bucket/xxx/xxx/xxx.py?host=xxx&access_key_id=xxx&access_key_secret=xxx```；
  - HDFS 文件：```hdfs://``` 加路径；

### 输出数据说明

脚本中可以输出并传回 Alink 端，形成 Flink Table 的形式，进行后续的处理。

即使没有数据需要输出，参数中的输出数据 Schema 也需要填写个非空的形式，例如```dummy string```。

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader()
pluginDownloader.downloadPlugin("tf115_python_env_macosx") # change according to system type

import json

source = RandomTableSourceStreamOp() \
    .setMaxRows(1000) \
    .setNumCols(10)

colNames = source.getColNames()
source = source.select("*, case when RAND() > 0.5 then 1. else 0. end as label")
source = source.link(TypeConvertStreamOp().setSelectedCols(["num"]).setTargetType("DOUBLE"))
label = "label"

userParams = {
    'featureCols': json.dumps(colNames),
    'labelCol': label,
    'batch_size': 16,
    'num_epochs': 1
}

tensorFlowStreamOp = TensorFlowStreamOp() \
    .setUserFiles(["https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_stream.py"]) \
    .setMainScriptFile("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_stream.py") \
    .setUserParams(json.dumps(userParams)) \
    .setOutputSchemaStr("model_id long, model_info string") \
    .linkFrom(source)
tensorFlowStreamOp.print()
StreamOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.TypeConvertStreamOp;
import com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp;
import com.alibaba.alink.operator.stream.tensorflow.TensorFlowStreamOp;
import com.alibaba.alink.params.dataproc.HasTargetType.TargetType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TensorFlowStreamOpTest {

	@Test
	public void testTensorFlowStreamOp() throws Exception {
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();
		pluginDownloader.downloadPlugin("tf115_python_env_macosx"); // change according to system type

		StreamOperator<?> source = new RandomTableSourceStreamOp()
			.setMaxRows(1000L)
			.setNumCols(10);

		String[] colNames = source.getColNames();
		source = source.select("*, case when RAND() > 0.5 then 1. else 0. end as label");
		source = source.link(new TypeConvertStreamOp().setSelectedCols("num").setTargetType(TargetType.DOUBLE));
		String label = "label";

		Map <String, Object> userParams = new HashMap<>();
		userParams.put("featureCols", JsonConverter.toJson(colNames));
		userParams.put("labelCol", label);
		userParams.put("batch_size", 16);
		userParams.put("num_epochs", 1);

		TensorFlowStreamOp tensorFlowStreamOp = new TensorFlowStreamOp()
			.setUserFiles(new String[] {"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_stream.py"})
			.setMainScriptFile("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/tf_dnn_stream.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.setOutputSchemaStr("model_id long, model_info string")
			.linkFrom(source);
		tensorFlowStreamOp.print();
		StreamOperator.execute();
	}
}
```
