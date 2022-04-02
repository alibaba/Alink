package com.alibaba.alink.params.classification;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface KnnPredictParams<T> extends
	RichModelMapperParams <T>,
	HasVectorColDefaultAsNull <T> {
	@NameCn("topK")
	@DescCn("topK")
	ParamInfo <Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("k")
		.setHasDefaultValue(10)
		.build();

	default Integer getK() {return get(K);}

	default T setK(Integer value) {return set(K, value);}
}
