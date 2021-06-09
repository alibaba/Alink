package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasMode<T> extends WithParams <T> {
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
