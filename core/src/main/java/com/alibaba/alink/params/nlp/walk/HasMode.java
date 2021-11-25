package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasMode<T> extends WithParams <T> {
	/**
	 * @cn metapath的模式
	 * @cn-name metapath中word2vec的模式，分别为metapath2vec和metapath2vecpp
	 */
	ParamInfo <Mode> MODE = ParamInfoFactory
		.createParamInfo("mode", Mode.class)
		.setDescription("mode for metapath")
		.setHasDefaultValue(Mode.METAPATH2VEC)
		.build();

	default Mode getMode() {
		return get(MODE);
	}

	default T setMode(Mode value) {
		return set(MODE, value);
	}

	default T setMode(String value) {
		return set(MODE, ParamUtil.searchEnum(MODE, value));
	}

	enum Mode {
		METAPATH2VEC,
		/**
		 * break change. the old key is 'metapath2vec++'
		 */
		METAPATH2VECPP
	}
}
