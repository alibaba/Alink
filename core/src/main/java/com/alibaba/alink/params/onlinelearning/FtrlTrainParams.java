package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;

public interface FtrlTrainParams<T> extends WithParams <T>,
	OnlineTrainParams <T>,
	HasL1 <T>,
	HasL2 <T>,
	HasAlpha <T>,
	HasBeta <T> {
}
