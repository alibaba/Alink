package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.clustering.HasClusteringDistanceType;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;

/**
 * Params for EvalCluster.
 */
public interface EvalClusterParams<T> extends
	HasPredictionCol <T>,
	HasClusteringDistanceType <T> {

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

	@NameCn("向量列名")
	@DescCn("输入表中的向量列名")
	ParamInfo <String> VECTOR_COL = ParamInfoFactory
		.createParamInfo("vectorCol", String.class)
		.setDescription("Name of a vector column")
		.setHasDefaultValue(null)
		.setAlias(new String[] {"vectorColName", "tensorColName", "vecColName"})
		.build();

	default String getVectorCol() {
		return get(VECTOR_COL);
	}

	default T setVectorCol(String value) {
		return set(VECTOR_COL, value);
	}
}
