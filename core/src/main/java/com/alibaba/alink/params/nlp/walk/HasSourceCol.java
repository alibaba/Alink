package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSourceCol<T> extends WithParams <T> {

	@NameCn("起始点列名")
	@DescCn("用来指定起始点列")
	ParamInfo <String> SOURCE_COL = ParamInfoFactory
		.createParamInfo("sourceCol", String.class)
		.setAlias(new String[] {"sourceColName", "node0"})
		.setDescription("source col name")
		.setRequired()
		.build();

	default String getSourceCol() {return get(SOURCE_COL);}

	default T setSourceCol(String value) {return set(SOURCE_COL, value);}
}
