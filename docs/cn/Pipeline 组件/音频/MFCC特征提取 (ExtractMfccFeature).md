# MFCC特征提取 (ExtractMfccFeature)
Java 类名：com.alibaba.alink.pipeline.audio.ExtractMfccFeature

Python 类名：ExtractMfccFeature


## 功能介绍

从 Alink Tensor 格式的音频数据中提取 MFCC 特征。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| sampleRate | 采样率 | 采样率 | Integer | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| hopTime | 相邻窗口时间间隔 | 相邻窗口时间间隔 | Double |  |  | 0.032 |
| numMfcc | mfcc参数 | mfcc参数 | Integer |  |  | 128 |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| windowTime | 一个窗口的时间 | 一个窗口的时间 | Double |  |  | 0.128 |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

## 代码示例

### Python 代码
```python
DATA_DIR = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/audio"
SAMPLE_RATE = 16000

df = pd.DataFrame([
    "246.wav",
    "247.wav"
])

source = BatchOperator.fromDataframe(df, "wav_file_path string") \
        .link(ReadAudioToTensorBatchOp()
                .setRootFilePath(DATA_DIR)
                .setSampleRate(SAMPLE_RATE)
                .setRelativeFilePathCol("wav_file_path")
                .setDuration(2.)
                .setOutputCol("tensor")
        )

mfcc = ExtractMfccFeature() \
        .setSelectedCol("tensor") \
        .setSampleRate(SAMPLE_RATE) \
        .setWindowTime(0.128) \
        .setHopTime(0.032) \
        .setNumMfcc(26) \
        .setOutputCol("mfcc")

mfcc.transform(source).print()
```

### Java 代码
```java
public class ExtractMfccFeatureTest {
    @Test
    public void testExtractMfccFeature() throws Exception {
        String DATA_DIR = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/audio";
        String[] allFiles = {"246.wav", "247.wav"};
        int SAMPLE_RATE = 16000;
        BatchOperator source = new MemSourceBatchOp(allFiles, "wav_file_path")
                .link(new ReadAudioToTensorBatchOp()
                        .setRootFilePath(DATA_DIR)
                        .setSampleRate(SAMPLE_RATE)
                        .setRelativeFilePathCol("wav_file_path")
                        .setDuration(2)
                        .setOutputCol("tensor")
                );

        ExtractMfccFeature mfcc = new ExtractMfccFeature()
                .setSelectedCol("tensor")
                .setSampleRate(SAMPLE_RATE)
                .setWindowTime(0.128)
                .setHopTime(0.032)
                .setNumMfcc(26)
                .setOutputCol("mfcc");

        mfcc.transform(source).print();
    }
}
```

