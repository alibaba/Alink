package com.alibaba.alink.common.model;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.linear.LinearModelType;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Params for the meta data of some models.
 */
public class ModelParamName {

	public static ParamInfo <String> MODEL_NAME = ParamInfoFactory
		.createParamInfo("modelName", String.class)
		.setDescription("modelName")
		.setRequired()
		.build();

	public static ParamInfo <String> BIN_CLS_PARAMS = ParamInfoFactory
		.createParamInfo("binClsParams", String.class)
		.setDescription("binClsParams")
		.setRequired()
		.build();

	public static ParamInfo <String> BIN_CLS_CLASS_NAME = ParamInfoFactory
		.createParamInfo("binClsClassName", String.class)
		.setDescription("binClsClassName")
		.setRequired()
		.build();

	public static ParamInfo <String> LABELS = ParamInfoFactory
		.createParamInfo("labels", String.class)
		.setDescription("labels")
		.setRequired()
		.build();

	public static ParamInfo <String> VECTOR_COL_NAME = ParamInfoFactory
		.createParamInfo("vectorColName", String.class)
		.setDescription("vectorColName")
		.setRequired()
		.build();

	public static ParamInfo <String> LABEL_TYPE_NAME = ParamInfoFactory
		.createParamInfo("labelTypeName", String.class)
		.setDescription("labelTypeName")
		.setAlias(new String[]{"labelType"})
		.setRequired()
		.build();

	public static ParamInfo <Integer[]> MODEL_COL_TYPES = ParamInfoFactory
		.createParamInfo("modelColTypes", Integer[].class)
		.setDescription("modelColTypes")
		.setRequired()
		.build();

	public static ParamInfo <String[]> MODEL_COL_NAMES = ParamInfoFactory
		.createParamInfo("modelColNames", String[].class)
		.setDescription("modelColNames")
		.setRequired()
		.build();

	public static ParamInfo <String[]> FEATURE_TYPES = ParamInfoFactory
		.createParamInfo("featureTypes", String[].class)
		.setDescription("feature types")
		.setRequired()
		.build();

	public static ParamInfo <Integer> VECTOR_SIZE = ParamInfoFactory
		.createParamInfo("vectorSize", Integer.class)
		.setDescription("vectorSize")
		.setRequired()
		.build();

	public static ParamInfo <Integer> NUM_CLASSES = ParamInfoFactory
		.createParamInfo("numClasses", Integer.class)
		.setDescription("numClasses")
		.setRequired()
		.build();

	public static ParamInfo <DenseVector> COEF = ParamInfoFactory
		.createParamInfo("coef", DenseVector.class)
		.setDescription("coef")
		.setRequired()
		.build();

	public static ParamInfo <String> LABEL_TYPE = ParamInfoFactory
		.createParamInfo("labelType", String.class)
		.setDescription("labelType")
		.setRequired()
		.build();

	public static ParamInfo <LinearModelType> LINEAR_MODEL_TYPE = ParamInfoFactory
		.createParamInfo("linearModelType", LinearModelType.class)
		.setDescription("linear model type")
		.setRequired()
		.build();

	public static ParamInfo <Boolean> IS_OLD_FORMAT = ParamInfoFactory
		.createParamInfo("isOldFormat", Boolean.class)
		.setDescription("isOldFormat")
		.setRequired()
		.build();

	public static ParamInfo <Boolean> IS_VECTOR_INPUT = ParamInfoFactory
		.createParamInfo("isVectorInput", Boolean.class)
		.setDescription("isVectorInput")
		.setRequired()
		.build();

	public static ParamInfo <Boolean> HAS_INTERCEPT_ITEM = ParamInfoFactory
		.createParamInfo("hasInterceptItem", boolean.class)
		.setDescription("has intercept item")
		.setRequired()
		.build();

	public static ParamInfo <Object[]> LABEL_VALUES = ParamInfoFactory
		.createParamInfo("labelValues", Object[].class)
		.setDescription("label values")
		.setRequired()
		.build();

	public static ParamInfo <Double> L2 = ParamInfoFactory
		.createParamInfo("l2", Double.class)
		.setDescription("label values")
		.setRequired()
		.build();

	public static ParamInfo <Double> L1 = ParamInfoFactory
		.createParamInfo("l1", Double.class)
		.setDescription("label values")
		.setRequired()
		.build();

	public static ParamInfo <double[]> LOSS_CURVE = ParamInfoFactory
		.createParamInfo("lossCurve", double[].class)
		.setDescription("lossCurve")
		.setRequired()
		.build();
}
