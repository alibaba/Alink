package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMinSampleRatioPerChild<T> extends WithParams <T> {
	@NameCn("子节点占父节点的最小样本比例")
	@DescCn("子节点占父节点的最小样本比例")
	ParamInfo <Double> MIN_SAMPLE_RATIO_PERCHILD = ParamInfoFactory
		.createParamInfo("minSampleRatioPerChild", Double.class)
		.setDescription("Minimal value of: (num of samples in child)/(num of samples in its parent).")
		.setHasDefaultValue(0.0)
		.setAlias(new String[] {"minPercent"})
		.build();

	default Double getMinSampleRatioPerChild() {
		return get(MIN_SAMPLE_RATIO_PERCHILD);
	}

	default T setMinSampleRatioPerChild(Double value) {
		return set(MIN_SAMPLE_RATIO_PERCHILD, value);
	}
}
