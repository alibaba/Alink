package com.alibaba.alink.params.dataproc.tensor;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasChannelFirst<T> extends WithParams <T> {

	@NameCn("Channel维度是否放在Shape的第一位")
	@DescCn("默认为false，参数为false时，放在Shape的最右侧，为true时，放在Shape的最左侧。")
	ParamInfo <Boolean> CHANNEL_FIRST = ParamInfoFactory
		.createParamInfo("channelFirst", Boolean.class)
		.setOptional()
		.setHasDefaultValue(false)
		.build();

	default Boolean getChannelFirst() {
		return get(CHANNEL_FIRST);
	}

	default T setChannelFirst(Boolean val) {
		return set(CHANNEL_FIRST, val);
	}

}
