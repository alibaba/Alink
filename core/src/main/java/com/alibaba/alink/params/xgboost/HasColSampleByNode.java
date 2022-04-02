package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasColSampleByNode<T> extends WithParams <T> {

	@NameCn("每个结点列采样")
	@DescCn("每个结点采样")
	ParamInfo <Double> COL_SAMPLE_BY_NODE = ParamInfoFactory
		.createParamInfo("colSampleByNode", Double.class)
		.setDescription("The subsample ratio of columns for each node (split)")
		.setHasDefaultValue(1.0)
		.build();

	default Double getColSampleByNode() {
		return get(COL_SAMPLE_BY_NODE);
	}

	default T setColSampleByNode(Double colSampleByTree) {
		return set(COL_SAMPLE_BY_NODE, colSampleByTree);
	}
}
