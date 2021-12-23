package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasWalkNum<T> extends WithParams <T> {
	/**
	 * @cn-name 路径数目
	 * @cn 每一个起始点游走出多少条路径
	 */
	ParamInfo <Integer> WALK_NUM = ParamInfoFactory
		.createParamInfo("walkNum", Integer.class)
		.setDescription("walk num")
		.setRequired()
		.build();

	default Integer getWalkNum() {return get(WALK_NUM);}

	default T setWalkNum(Integer value) {return set(WALK_NUM, value);}
}
