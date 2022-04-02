package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxOutlierNumPerGroup<T> extends WithParams <T> {
	@NameCn("每组最大异常点数目")
	@DescCn("每组最大异常点数目")
	ParamInfo <Integer> MAX_OUTLIER_NUM_PER_GROUP = ParamInfoFactory
		.createParamInfo("maxOutlierNumPerGroup", Integer.class)
		.setDescription("the maximum number of outliers per group.")
		.build();

	default Integer getMaxOutlierNumPerGroup() {
		return get(MAX_OUTLIER_NUM_PER_GROUP);
	}

	default T setMaxOutlierNumPerGroup(Integer value) {
		return set(MAX_OUTLIER_NUM_PER_GROUP, value);
	}
}
