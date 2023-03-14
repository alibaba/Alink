package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.outlier.OutlierDetectorParams;
import com.alibaba.alink.params.shared.colname.HasSelectedColDefaultAsNull;
import com.alibaba.alink.params.timeseries.HasFrequency;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalType;

import java.io.Serializable;

public interface TimeSeriesDecomposeParams<T> extends
	OutlierDetectorParams <T>,
	//HasTimeCol<T>,
	HasSelectedColDefaultAsNull <T>,
	HasSeasonalType <T>,
	HasFrequency <T> {

	@NameCn("时序分解方法")
	@DescCn("时序分解方法")
	ParamInfo <DecomposeMethod> DECOMPOSE_METHOD = ParamInfoFactory
		.createParamInfo("decomposeMethod", DecomposeMethod.class)
		.setDescription("Method to decompose the time series.")
		.setHasDefaultValue(DecomposeMethod.STL)
		.build();

	default DecomposeMethod getDecomposeMethod() {
		return get(DECOMPOSE_METHOD);
	}

	default T setDecomposeMethod(DecomposeMethod value) {
		return set(DECOMPOSE_METHOD, value);
	}

	default T setDecomposeMethod(String value) {
		return set(DECOMPOSE_METHOD, ParamUtil.searchEnum(DECOMPOSE_METHOD, value));
	}

	enum DecomposeMethod implements Serializable {
		STL,
		CONVOLUTION
	}

	@NameCn("时序分解结果的检测方法")
	@DescCn("时序分解结果的检测方法")
	ParamInfo <DetectMethod> DETECT_METHOD = ParamInfoFactory
		.createParamInfo("detectMethod", DetectMethod.class)
		.setDescription("Detect method for the decomposition of time series.")
		.setHasDefaultValue(DetectMethod.KSigma)
		.build();

	default DetectMethod getDetectMethod() {
		return get(DETECT_METHOD);
	}

	default T setDetectMethod(DetectMethod value) {
		return set(DETECT_METHOD, value);
	}

	default T setDetectMethod(String value) {
		return set(DETECT_METHOD, ParamUtil.searchEnum(DETECT_METHOD, value));
	}

	enum DetectMethod implements Serializable {
		KSigma,
		SHESD,
		BoxPlot
	}
}
