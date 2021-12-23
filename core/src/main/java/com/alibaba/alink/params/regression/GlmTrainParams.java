package com.alibaba.alink.params.regression;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs10;

/**
 * Parameter of  glm train.
 */
public interface GlmTrainParams<T> extends
	RegressorTrainParams <T>,
	HasWeightColDefaultAsNull <T>,
	HasMaxIterDefaultAs10 <T> {

	/**
	 * @cn-name 分布族
	 * @cn 分布族，包含gaussian, Binomial, Poisson, Gamma and Tweedie，默认值gaussian。
	 */
	ParamInfo <Family> FAMILY = ParamInfoFactory
		.createParamInfo("family", Family.class)
		.setDescription("the name of family which is a description of the error distribution. " +
			"Supported options: Gaussian, Binomial, Poisson, Gamma and Tweedie")
		.setHasDefaultValue(Family.Gaussian)
		.build();
	/**
	 * @cn-name 分布族的超参
	 * @cn 分布族的超参，默认值是0.0
	 */
	ParamInfo <Double> VARIANCE_POWER = ParamInfoFactory
		.createParamInfo("variancePower", Double.class)
		.setDescription("The power in the variance function of the Tweedie distribution. " +
			"It describe the relationship between the variance and mean of the distribution")
		.setHasDefaultValue(0.0)
		.build();
	/**
	 * @cn-name 连接函数
	 * @cn 连接函数，包含cloglog, Identity, Inverse, log, logit, power, probit和sqrt，默认值是指数分布族对应的连接函数。
	 */
	ParamInfo <Link> LINK = ParamInfoFactory
		.createParamInfo("link", Link.class)
		.setDescription("The name of link function" +
			"Supported options: CLogLog, Identity, Inverse, log, logit, power, probit and sqrt")
		.setHasDefaultValue(null)
		.build();
	/**
	 * @cn-name 连接函数的超参
	 * @cn 连接函数的超参
	 */
	ParamInfo <Double> LINK_POWER = ParamInfoFactory
		.createParamInfo("linkPower", Double.class)
		.setDescription("Param for the index in the power link function. ")
		.setHasDefaultValue(1.0)
		.build();
	/**
	 * @cn-name 偏移列
	 * @cn 偏移列
	 */
	ParamInfo <String> OFFSET_COL = ParamInfoFactory
		.createParamInfo("offsetCol", String.class)
		.setDescription("The col name of offset")
		.setAlias(new String[] {"offsetColName"})
		.setHasDefaultValue(null)
		.build();
	/**
	 * @cn-name 是否拟合常数项
	 * @cn 是否拟合常数项，默认是拟合
	 */
	ParamInfo <Boolean> FIT_INTERCEPT = ParamInfoFactory
		.createParamInfo("fitIntercept", Boolean.class)
		.setDescription("Sets if we should fit the intercept")
		.setHasDefaultValue(true)
		.build();
	/**
	 * @cn-name l2正则系数
	 * @cn l2正则系数
	 */
	ParamInfo <Double> REG_PARAM = ParamInfoFactory
		.createParamInfo("regParam", Double.class)
		.setDescription("Sets the regularization parameter for L2 regularization")
		.setHasDefaultValue(0.0)
		.build();
	/**
	 * @cn-name 收敛精度
	 * @cn 收敛精度
	 */
	ParamInfo <Double> EPSILON = ParamInfoFactory
		.createParamInfo("epsilon", Double.class)
		.setDescription("epsilon")
		.setHasDefaultValue(1.0e-5)
		.build();

	default Family getFamily() {
		return get(FAMILY);
	}

	default T setFamily(Family value) {
		return set(FAMILY, value);
	}

	default T setFamily(String value) {
		return set(FAMILY, ParamUtil.searchEnum(FAMILY, value));
	}

	default Double getVariancePower() {
		return get(VARIANCE_POWER);
	}

	default T setVariancePower(Double value) {
		return set(VARIANCE_POWER, value);
	}

	default Link getLink() {
		return get(LINK);
	}

	default T setLink(Link value) {
		return set(LINK, value);
	}

	default T setLink(String value) {
		return set(LINK, ParamUtil.searchEnum(LINK, value));
	}

	default Double getLinkPower() {
		return get(LINK_POWER);
	}

	default T setLinkPower(Double value) {
		return set(LINK_POWER, value);
	}

	default String getOffsetCol() {
		return get(OFFSET_COL);
	}

	default T setOffsetCol(String value) {
		return set(OFFSET_COL, value);
	}

	default Boolean getFitIntercept() {
		return get(FIT_INTERCEPT);
	}

	default T setFitIntercept(Boolean value) {
		return set(FIT_INTERCEPT, value);
	}

	default Double getRegParam() {
		return get(REG_PARAM);
	}

	default T setRegParam(Double value) {
		return set(REG_PARAM, value);
	}

	default Double getEpsilon() {
		return get(EPSILON);
	}

	default Double setEpsilon() {
		return get(EPSILON);
	}

	enum Family {
		Gamma,
		Binomial,
		Gaussian,
		Poisson,
		Tweedie
	}

	enum Link {
		CLogLog,
		Identity,
		Inverse,
		Log,
		Logit,
		Power,
		Probit,
		Sqrt
	}
}
