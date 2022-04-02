package com.alibaba.alink.params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.FilePath;

/**
 * Params for ModelStream4Predict.
 */
public interface ModelStreamScanParams<T> extends WithParams <T> {

	@NameCn("模型流的文件路径")
	@DescCn("模型流的文件路径")
	ParamInfo <String> MODEL_STREAM_FILE_PATH = ParamInfoFactory
		.createParamInfo("modelStreamFilePath", String.class)
		.setDescription("File path with file system.")
		.setHasDefaultValue(null)
		.build();

	default FilePath getModelStreamFilePath() {
		return FilePath.deserialize(get(MODEL_STREAM_FILE_PATH));
	}

	default T setModelStreamFilePath(String pathString) {
		return set(MODEL_STREAM_FILE_PATH, new FilePath(pathString).serialize());
	}

	default T setModelStreamFilePath(FilePath filePath) {
		return set(MODEL_STREAM_FILE_PATH, filePath.serialize());
	}

	@NameCn("扫描模型路径的时间间隔")
	@DescCn("描模型路径的时间间隔，单位秒")
	ParamInfo <Integer> MODEL_STREAM_SCAN_INTERVAL = ParamInfoFactory
		.createParamInfo("modelStreamScanInterval", Integer.class)
		.setDescription("scan time interval")
		.setHasDefaultValue(10)
		.build();

	default Integer getModelStreamScanInterval() {return get(MODEL_STREAM_SCAN_INTERVAL);}

	default T setModelStreamScanInterval(Integer scanInterval) {return set(MODEL_STREAM_SCAN_INTERVAL, scanInterval);}

	@NameCn("模型流的起始时间")
	@DescCn("模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s)")
	ParamInfo <String> MODEL_STREAM_START_TIME = ParamInfoFactory
		.createParamInfo("modelStreamStartTime", String.class)
		.setDescription("start time of model stream")
		.setHasDefaultValue(null)
		.build();

	default String getModelStreamStartTime() {return get(MODEL_STREAM_START_TIME);}

	default T setModelStreamStartTime(String startTime) {return set(MODEL_STREAM_START_TIME, startTime);}
}
