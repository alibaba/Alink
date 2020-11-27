package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Params: Flag of excluding the known objects in recommended top objects.
 */
public interface HasExcludeKnownDefaultAsFalse<T> extends WithParams <T> {
	ParamInfo <Boolean> EXCLUDE_KNOWN = ParamInfoFactory
		.createParamInfo("excludeKnown", Boolean.class)
		.setDescription("Flag of excluding the known objects in recommended top objects.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getExcludeKnown() {return get(EXCLUDE_KNOWN);}

	default T setExcludeKnown(Boolean value) {return set(EXCLUDE_KNOWN, value);}
}
