package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasClause<T> extends WithParams <T> {
	@NameCn("运算语句")
	@DescCn("运算语句")
	ParamInfo <String> CLAUSE = ParamInfoFactory
		.createParamInfo("clause", String.class)
		.setDescription("Operation clause.")
		.setRequired()
		.setAlias(new String[] {"param", "filter"})
		.build();

	default String getClause() {
		return get(CLAUSE);
	}

	default T setClause(String value) {
		return set(CLAUSE, value);
	}
}
