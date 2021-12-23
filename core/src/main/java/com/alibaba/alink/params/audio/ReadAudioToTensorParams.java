package com.alibaba.alink.params.audio;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.image.HasRelativeFilePathCol;
import com.alibaba.alink.params.io.HasRootFilePath;
import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface ReadAudioToTensorParams<T> extends
	MapperParams <T>,
	HasSampleRate <T>,
	HasRelativeFilePathCol <T>,
	HasRootFilePath <T>,
	HasOutputCol <T>,
	HasReservedColsDefaultAsNull <T> {
	/**
	 * @cn-name 采样持续时间
	 * @cn 采样持续时间
	 */
	ParamInfo <Double> DURATION = ParamInfoFactory
		.createParamInfo("durationTime", Double.class)
		.setDescription("only load up to this much audio (in seconds)")
		.build();
	/**
	 * @cn-name 采样开始时刻
	 * @cn 采样开始时刻
	 */
	ParamInfo <Double> OFFSET = ParamInfoFactory
		.createParamInfo("startTime", Double.class)
		.setDescription("start reading after this time (in seconds)")
		.setHasDefaultValue(0.0)
		.build();

	default double getDuration() {
		return get(DURATION);
	}

	default T setDuration(double value) {
		return set(DURATION, value);
	}

	default double getOffset() {
		return get(OFFSET);
	}

	default T setOffset(double value) {
		return set(OFFSET, value);
	}

}
