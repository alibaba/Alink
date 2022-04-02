package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * An interface for classes with a parameter specifying the column name of the recommend result.
 */
public interface HasRecommCol<T> extends WithParams <T> {

	@NameCn("推荐结果列名")
	@DescCn("推荐结果列名")
	ParamInfo <String> RECOMM_COL = ParamInfoFactory
		.createParamInfo("recommCol", String.class)
		.setDescription("Column name of recommend result.")
		.setAlias(new String[] {"predResultColName", "predictionCol"})
		.setRequired()
		.build();

	default String getRecommCol() {
		return get(RECOMM_COL);
	}

	default T setRecommCol(String colName) {
		return set(RECOMM_COL, colName);
	}
}
