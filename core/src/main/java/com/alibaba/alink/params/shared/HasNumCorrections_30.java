package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
@Internal
public interface HasNumCorrections_30<T> extends WithParams <T> {
	@NameCn("LBFGS内部参数")
	@DescCn("LBFGS内部参数")
	ParamInfo <Integer> NUM_CORRECTIONS = ParamInfoFactory
		.createParamInfo("numCorrections", Integer.class)
		.setDescription("num corrections, only used by LBFGS and owlqn.")
		.setHasDefaultValue(30)
		.build();

	default int getNumCorrections() {
		return get(NUM_CORRECTIONS);
	}

	default T setNumCorrections(int value) {
		return set(NUM_CORRECTIONS, value);
	}
}
