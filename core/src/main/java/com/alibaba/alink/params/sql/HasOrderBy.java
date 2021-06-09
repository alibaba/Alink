package com.alibaba.alink.params.sql;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasOrderBy<T> extends WithParams<T> {
	ParamInfo<String> ORDER_BY = ParamInfoFactory
		.createParamInfo("orderBy", String.class)
		.setDescription("order by col")
		.setRequired()
		.build();

	default String getOrderBy() {return get(ORDER_BY);}

	default T setOrderBy(String value) {return set(ORDER_BY, value);}
}
