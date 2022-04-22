# MFCC特征提取 (ExtractMfccFeatureBatchOp)
Java 类名：com.alibaba.alink.operator.batch.audio.ExtractMfccFeatureBatchOp

Python 类名：ExtractMfccFeatureBatchOp


## 功能介绍

* 从数据中提取 MFCC 特征。
* 支持Alink Vector、一维或两维Alink FloatTensor格式的数据

### 使用方式

用于声学特征提取，通常与ReadAudioToTensor组件一起使用，连接在其后

### 文献索引

[1] Davis S, Mermelstein P. Comparison of parametric representations for monosyllabic word recognition in continuously spoken sentences[J]. IEEE transactions on acoustics, speech, and signal processing, 1980, 28(4): 357-366.

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

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

```python
dataDir = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/audio";
   
df = pd.DataFrame([
    ["246.wav"],
    ["247.wav"]
])

allFiles = BatchOperator.fromDataframe(df, schemaStr='wav_file_path string')
SAMPLE_RATE = 16000

readOp = ReadAudioToTensorBatchOp().setRootFilePath(dataDir) \
.setSampleRate(SAMPLE_RATE) \
.setRelativeFilePathCol("wav_file_path") \
.setOutputCol("tensor") \
.linkFrom(allFiles)

mfccOp = ExtractMfccFeatureBatchOp() \
.setSampleRate(SAMPLE_RATE) \
.setSelectedCol("tensor") \
.linkFrom(readOp)

mfccOp.print()
```

### Java 代码

```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class ExtractMfccFeatureBatchOpTest extends AlinkTestBase {
	@Test
	public void testExtractMfccFeatureBatchOp() throws Exception {
		String dataDir = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/audio";
		String[] allFiles = {"246.wav", "247.wav"};
		int sampleRate = 16000;
		String tensorName = "tensor";
		String mfccName = "mfcc";
		String wavFile = "wav_file_path";
		BatchOperator source = new MemSourceBatchOp(allFiles, wavFile)
				.link(new ReadAudioToTensorBatchOp()
						.setRootFilePath(dataDir)
						.setSampleRate(sampleRate)
						.setRelativeFilePathCol(wavFile)
						.setDuration(2)
						.setOutputCol(tensorName)
				)
				.link(new ExtractMfccFeatureBatchOp()
						.setSelectedCol(tensorName)
						.setSampleRate(sampleRate)
						.setWindowTime(0.128)
						.setHopTime(0.032)
						.setNumMfcc(26)
						.setOutputCol(mfccName))
				.select(new String[]{wavFile, mfccName})
				.print();
	}
}

```

### 运行结果

wav_file_path|mfcc
-------------|----
246.wav|FLOAT#59,26,1#48.78127 -32.02646 12.432438 ...
247.wav|FLOAT#59,26,1#-50.62911 -13.844937 24.176699 ...
