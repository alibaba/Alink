package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * @author dota.zk
 * @date 25/06/2019
 */
public interface HasResultTypes<T> extends WithParams<T> {
	@NameCn("结果的类型列表")
	@DescCn("结果的类型列表, 类型包括'BOOLEAN', 'INTEGER', 'LONG', 'FLOAT', 'DOUBLE', 'STRING'")
	ParamInfo <String[]> RESULT_TYPES = ParamInfoFactory
		.createParamInfo("resultTypes", String[].class)
		.setDescription(
			"the type list of result, each should be one of {'BOOLEAN', 'INTEGER', 'LONG', 'FLOAT', 'DOUBLE', 'STRING'}")
		.setRequired()
		.build();

    default T setResultTypes(String... resultType) {
		return set(RESULT_TYPES, resultType);
    }

    default String[] getResultTypes() {
		return get(RESULT_TYPES);
    }
}
