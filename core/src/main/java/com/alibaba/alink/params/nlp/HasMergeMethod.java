package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.ParamUtil;

import java.io.Serializable;

public interface HasMergeMethod<T> extends WithParams <T> {
	/**
	 * @cn-name 向量组合方法
	 * @cn 预测文档向量时，需要用到的方法。支持三种方法：平均（avg），最小（min）和最大（max），默认值为平均
	 */
	ParamInfo <MergeMethod> MERGE_METHOD = ParamInfoFactory
		.createParamInfo("mergeMethod", MergeMethod.class)
		.setDescription("Method to predict doc vector, support 3 method: avg, min and max, default value is avg.")
		.setHasDefaultValue(MergeMethod.AVG)
		.build();

	default MergeMethod getMergeMethod() {
		return get(MERGE_METHOD);
	}

	default T setMergeMethod(MergeMethod value) {
		return set(MERGE_METHOD, value);
	}

	default T setMergeMethod(String value) {
		return set(MERGE_METHOD, ParamUtil.searchEnum(MERGE_METHOD, value));
	}

	enum MergeMethod implements Serializable {

		/**
		 * AVG Method
		 */
		AVG,

		/**
		 * SUM Method
		 */
		SUM,

		/**
		 * SEQUENCE Method
		 */
		SEQUENCE,

		/**
		 * MIN Method
		 */
		MIN,

		/**
		 * MAX Method
		 */
		MAX
	}
}
