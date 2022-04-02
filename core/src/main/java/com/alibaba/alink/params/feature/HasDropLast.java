package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDropLast<T> extends WithParams <T> {
	@NameCn("是否删除最后一个元素")
	@DescCn("删除最后一个元素是为了保证线性无关性。默认true")
	ParamInfo <Boolean> DROP_LAST = ParamInfoFactory
		.createParamInfo("dropLast", Boolean.class)
		.setDescription("drop last")
		.setHasDefaultValue(Boolean.TRUE)
		.build();

	default Boolean getDropLast() {
		return get(DROP_LAST);
	}

	default T setDropLast(Boolean value) {
		return set(DROP_LAST, value);
	}
}
