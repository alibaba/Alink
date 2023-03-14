package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.HasDegreeDefaultAs2;

public interface OcsvmModelTrainParams<T> extends WithParams <T>,
	WithMultiVarParams <T>,
	HasDegreeDefaultAs2 <T>,
	HaskernelType <T> {

	@NameCn("Kernel函数的相关参数coef0")
	@DescCn("Kernel函数的相关参数，只有在POLY和SIGMOID时起作用。")
	ParamInfo <Double> COEF0 = ParamInfoFactory
		.createParamInfo("coef0", Double.class)
		.setDescription("coef0")
		.setHasDefaultValue(0.0)
		.build();
	@NameCn("收敛阈值")
	@DescCn("迭代算法的收敛阈值")
	ParamInfo <Double> EPS = ParamInfoFactory
		.createParamInfo("eps", Double.class)
		.setDescription("eps")
		.setHasDefaultValue(1e-3)
		.build();
	@NameCn("Kernel函数的相关参数gamma")
	@DescCn("Kernel函数的相关参数，只在 RBF, POLY 和 SIGMOID 时起作用. 如果不设置默认取 1/d，d为特征维度.")
	ParamInfo <Double> GAMMA = ParamInfoFactory
		.createParamInfo("gamma", Double.class)
		.setDescription("gamma")
		.setHasDefaultValue(-1.0)
		.build();
	@NameCn("异常点比例上界参数nu")
	@DescCn("该参数取值范围是(0,1)，该值与支持向量的数目正向相关。")
	ParamInfo <Double> NU = ParamInfoFactory
		.createParamInfo("nu", Double.class)
		.setDescription("nu")
		.setHasDefaultValue(0.001)
		.build();

	default Double getCoef0() {return get(COEF0);}

	default T setCoef0(Double value) {return set(COEF0, value);}

	default Double getEps() {return get(EPS);}

	default T setEps(Double value) {return set(EPS, value);}

	default Double getGamma() {return get(GAMMA);}

	default T setGamma(Double value) {return set(GAMMA, value);}

	default Double getNu() {return get(NU);}

	default T setNu(Double value) {return set(NU, value);}
}
