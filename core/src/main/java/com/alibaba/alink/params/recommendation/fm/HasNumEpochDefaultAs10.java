package com.alibaba.alink.params.recommendation.fm;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * number of epoch.
 */
public interface HasNumEpochDefaultAs10<T> extends WithParams <T> {
	@NameCn("epoch数")
	@DescCn("epoch数")
	ParamInfo <Integer> NUM_EPOCHS = ParamInfoFactory
		.createParamInfo("numEpochs", Integer.class)
		.setDescription("num epochs")
		.setHasDefaultValue(10)
		.setAlias(new String[] {"numIter"})
		.build();

	default Integer getNumEpochs() {
		return get(NUM_EPOCHS);
	}

	default T setNumEpochs(Integer value) {
		return set(NUM_EPOCHS, value);
	}

}
