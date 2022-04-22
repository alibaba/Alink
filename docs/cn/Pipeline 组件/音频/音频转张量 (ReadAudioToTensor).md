# 音频转张量 (ReadAudioToTensor)
Java 类名：com.alibaba.alink.pipeline.audio.ReadAudioToTensor

Python 类名：ReadAudioToTensor


## 功能介绍

读取音频文件，并转换为 Alink FloatTensor 格式。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| relativeFilePathCol | 文件路径列 | 文件路径列 | String | ✓ |  |  |
| rootFilePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| sampleRate | 采样率 | 采样率 | Integer | ✓ |  |  |
| duration | 采样持续时间 | 采样持续时间 | Double |  |  |  |
| offset | 采样开始时刻 | 采样开始时刻 | Double |  |  | 0.0 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

## 代码示例

### Python 代码

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

```python
dataDir = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/audio";
   
df = pd.DataFrame([
    ["246.wav"],
    ["247.wav"]
])

allFiles = BatchOperator.fromDataframe(df, schemaStr='wav_file_path string')
SAMPLE_RATE = 16000

readOp = ReadAudioToTensor().setRootFilePath(dataDir) \
.setSampleRate(SAMPLE_RATE) \
.setRelativeFilePathCol("wav_file_path") \
.setOutputCol("tensor") \
.transform(allFiles)

readOp.print()
```

### Java 代码

```java
package example;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.audio.ReadAudioToTensor;
import org.junit.Test;

public class ReadAudioToTensorTest {

    @Test
    public void testReadAudioToTensor() throws Exception {
        String dataDir = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/audio";
        String[] allFiles = {"246.wav", "247.wav"};
        int sampleRate = 16000;
        BatchOperator source = new MemSourceBatchOp(allFiles, "wav_file_path");
        ReadAudioToTensor pipe = new ReadAudioToTensor()
                .setRootFilePath(dataDir)
                .setSampleRate(sampleRate)
                .setRelativeFilePathCol("wav_file_path")
                .setDuration(2)
                .setOutputCol("tensor");
        pipe.transform(source).print();
    }

}
```

### 运行结果

wav_file_path|tensor
-------------|------
246.wav|FLOAT#32000,1#-7.324219E-4 -0.0010986328 -9.460449E-4 ...
247.wav|FLOAT#32000,1#-0.0057678223 -0.0051574707 -0.0036315918 ...
