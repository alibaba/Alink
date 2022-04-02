package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasPredMethod<T> extends WithParams <T> {
	@NameCn("向量组合方法")
	@DescCn("预测文档向量时，需要用到的方法。支持三种方法：平均（avg），最小（min）和最大（max），默认值为平均")
	ParamInfo <PredMethod> PRED_METHOD = ParamInfoFactory
		.createParamInfo("predMethod", PredMethod.class)
		.setDescription("Method to predict doc vector, support 3 method: avg, min and max, default value is avg.")
		.setHasDefaultValue(PredMethod.AVG)
		.setAlias(new String[] {"generationType", "algorithmType"})
		.build();

	default PredMethod getPredMethod() {
		return get(PRED_METHOD);
	}

	default T setPredMethod(PredMethod value) {
		return set(PRED_METHOD, value);
	}

	default T setPredMethod(String value) {
		return set(PRED_METHOD, ParamUtil.searchEnum(PRED_METHOD, value));
	}

	enum PredMethod {
		/**
		 * AVG Method
		 */
		AVG,

		/**
		 * SUM Method
		 */
		SUM,

		/**
		 * MIN Method
		 */
		MIN,

		/**
		 * MAX Method
		 */
		MAX;


	}
}
