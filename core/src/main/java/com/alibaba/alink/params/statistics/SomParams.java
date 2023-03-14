package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface SomParams<T> extends WithParams <T> {

	@NameCn("vector列名")
	@DescCn("vector列名")
	ParamInfo <String> VECTOR_COL = ParamInfoFactory
		.createParamInfo("vectorCol", String.class)
		.setDescription("tensor col name")
		.setAlias(new String[] {"tensorColName"})
		.setRequired()
		.build();
	@NameCn("向量长度")
	@DescCn("向量长度")
	ParamInfo <Integer> VDIM = ParamInfoFactory
		.createParamInfo("vdim", Integer.class)
		.setDescription("vdim")
		.setRequired()
		.build();
	@NameCn("x方向网格数")
	@DescCn("x方向网格数")
	ParamInfo <Integer> XDIM = ParamInfoFactory
		.createParamInfo("xdim", Integer.class)
		.setDescription("xdim")
		.setRequired()
		.build();
	@NameCn("y方向网格数")
	@DescCn("y方向网格数")
	ParamInfo <Integer> YDIM = ParamInfoFactory
		.createParamInfo("ydim", Integer.class)
		.setDescription("ydim")
		.setRequired()
		.build();
	@NameCn("迭代轮数")
	@DescCn("迭代轮数")
	ParamInfo <Integer> NUM_ITERS = ParamInfoFactory
		.createParamInfo("numIters", Integer.class)
		.setDescription("num iters")
		.setHasDefaultValue(100)
		.build();
	@NameCn("是否打开调试")
	@DescCn("是否打开调试")
	ParamInfo <Boolean> DEBUG = ParamInfoFactory
		.createParamInfo("debug", Boolean.class)
		.setDescription("debug")
		.setHasDefaultValue(false)
		.build();
	@NameCn("是否每轮评估迭代结果")
	@DescCn("是否每轮评估迭代结果")
	ParamInfo <Boolean> EVALUATION = ParamInfoFactory
		.createParamInfo("evaluation", Boolean.class)
		.setDescription("evaluation")
		.setHasDefaultValue(false)
		.build();
	@NameCn("学习率")
	@DescCn("学习率")
	ParamInfo <Double> LEARN_RATE = ParamInfoFactory
		.createParamInfo("learnRate", Double.class)
		.setDescription("learn rate")
		.setHasDefaultValue(0.5)
		.build();
	@NameCn("neighborhood函数方差")
	@DescCn("neighborhood函数方差")
	ParamInfo <Double> SIGMA = ParamInfoFactory
		.createParamInfo("sigma", Double.class)
		.setDescription("sigma")
		.setHasDefaultValue(1.0)
		.build();

	default String getVectorCol() {return get(VECTOR_COL);}

	default T setVectorCol(String value) {return set(VECTOR_COL, value);}

	default Integer getVdim() {return get(VDIM);}

	default T setVdim(Integer value) {return set(VDIM, value);}

	default Integer getXdim() {return get(XDIM);}

	default T setXdim(Integer value) {return set(XDIM, value);}

	default Integer getYdim() {return get(YDIM);}

	default T setYdim(Integer value) {return set(YDIM, value);}

	default Integer getNumIters() {return get(NUM_ITERS);}

	default T setNumIters(Integer value) {return set(NUM_ITERS, value);}

	default Boolean getDebug() {return get(DEBUG);}

	default T setDebug(Boolean value) {return set(DEBUG, value);}

	default Boolean getEvaluation() {return get(EVALUATION);}

	default T setEvaluation(Boolean value) {return set(EVALUATION, value);}

	default Double getLearnRate() {return get(LEARN_RATE);}

	default T setLearnRate(Double value) {return set(LEARN_RATE, value);}

	default Double getSigma() {return get(SIGMA);}

	default T setSigma(Double value) {return set(SIGMA, value);}

}
