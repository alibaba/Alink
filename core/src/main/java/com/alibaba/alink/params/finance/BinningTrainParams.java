package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.feature.HasDiscreteThresholds;
import com.alibaba.alink.params.feature.HasDiscreteThresholdsArray;
import com.alibaba.alink.params.feature.HasLeftOpen;
import com.alibaba.alink.params.feature.HasNumBuckets;
import com.alibaba.alink.params.feature.HasNumBucketsArray;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueStringDefaultAs1;

public interface BinningTrainParams<T> extends
	HasSelectedCols <T>,
	HasNumBuckets <T>,
	HasNumBucketsArray <T>,
	HasLeftOpen <T>,
	HasDiscreteThresholds <T>,
	HasDiscreteThresholdsArray <T>,
	HasPositiveLabelValueStringDefaultAs1 <T>,
	HasBinningMethod <T> {

	@NameCn("是否读取用户自定义JSON")
	@DescCn("是否读取用户自定义JSON, true则为用户自定义分箱，false则按参数配置分箱。")
	ParamInfo <Boolean> FROM_USER_DEFINED = ParamInfoFactory
		.createParamInfo("fromUserDefined", Boolean.class)
		.setDescription("From user defined")
		.setHasDefaultValue(false)
		.build();

	default Boolean getFromUserDefined() {
		return get(FROM_USER_DEFINED);
	}

	default T setFromUserDefined(Boolean value) {
		return set(FROM_USER_DEFINED, value);
	}

	@NameCn("标签列名")
	@DescCn("输入表中的标签列名")
	ParamInfo <String> LABEL_COL = ParamInfoFactory
		.createParamInfo("labelCol", String.class)
		.setDescription("Name of the label column in the input table")
		.setAlias(new String[] {"labelColName"})
		.setHasDefaultValue(null)
		.build();

	default String getLabelCol() {
		return get(LABEL_COL);
	}

	default T setLabelCol(String value) {
		return set(LABEL_COL, value);
	}

	@NameCn("用户定义的bucket个数，形式如col0:3, col1:4")
	@DescCn("用户定义的bucket个数，形式如col0:3, col1:4")
	ParamInfo <String> NUM_BUCKETS_MAP = ParamInfoFactory
		.createParamInfo("numBucketsMap", String.class)
		.setDescription("numbero bucket string")
		.setHasDefaultValue(null)
		.build();

	default String getNumBucketsMap() {
		return get(NUM_BUCKETS_MAP);
	}

	default T setNumBucketsMap(String value) {
		return set(NUM_BUCKETS_MAP, value);
	}

	@NameCn("用户定义的bin的json")
	@DescCn("用户定义的bin的json")
	ParamInfo <String> USER_DEFINED_BIN = ParamInfoFactory
		.createParamInfo("userDefinedBin", String.class)
		.setDescription("User Defined Bin")
		.build();

	default String getUserDefinedBin() {
		return get(USER_DEFINED_BIN);
	}

	default T setUserDefinedBin(String value) {
		return set(USER_DEFINED_BIN, value);
	}

	@NameCn("离散分箱离散为ELSE的最小阈值, 形式如col0:3, col1:4")
	@DescCn("离散分箱离散为ELSE的最小阈值，形式如col0:3, col1:4。")
	ParamInfo <String> DISCRETE_THRESHOLDS_MAP = ParamInfoFactory
		.createParamInfo("discreteThresholdsMap", String.class)
		.setDescription("discreate threshold str")
		.setHasDefaultValue(null)
		.build();

	default String getDiscreteThresholdsMap() {
		return get(DISCRETE_THRESHOLDS_MAP);
	}

	default T setDiscreteThresholdsMap(String value) {
		return set(DISCRETE_THRESHOLDS_MAP, value);
	}


}
