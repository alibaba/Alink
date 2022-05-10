package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.feature.featuregenerator.HasHopTime;
import com.alibaba.alink.params.feature.featuregenerator.HasWindowTime;
import com.alibaba.alink.params.nlp.HasTopNDefaultAs10;
import com.alibaba.alink.params.shared.colname.HasTimeCol;

public interface HotProductParams<T> extends
	HasTimeCol <T>,
	HasTopNDefaultAs10 <T>,
	HasWindowTime <T>,
	HasHopTime <T> {

	@NameCn("产品列")
	@DescCn("产品列")
	ParamInfo <String> PRODUCT_COL = ParamInfoFactory
		.createParamInfo("productCol", String.class)
		.setDescription("product col name")
		.setRequired()
		.build();

	default String getProductCol() {
		return get(PRODUCT_COL);
	}

	default T setProductCol(String value) {
		return set(PRODUCT_COL, value);
	}
}
