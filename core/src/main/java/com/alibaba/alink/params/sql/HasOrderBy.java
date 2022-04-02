package com.alibaba.alink.params.sql;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasOrderBy<T> extends WithParams <T> {

	@NameCn("排序列")
	@DescCn("排序列")
	ParamInfo <String> ORDER_BY = ParamInfoFactory
		.createParamInfo("orderBy", String.class)
		.setDescription("order by col")
		.setRequired()
		.build();

	default String getOrderBy() {return get(ORDER_BY);}

	default T setOrderBy(String value) {return set(ORDER_BY, value);}
}
