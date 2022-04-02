package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTargetCol<T> extends WithParams <T> {
	@NameCn("中止点点列名")
	@DescCn("用来指定中止点列")
	ParamInfo <String> TARGET_COL = ParamInfoFactory
		.createParamInfo("targetCol", String.class)
		.setDescription("target col name")
		.setAlias(new String[] {"targetColName", "node1"})
		.setRequired()
		.build();

	default String getTargetCol() {return get(TARGET_COL);}

	default T setTargetCol(String value) {return set(TARGET_COL, value);}
}
