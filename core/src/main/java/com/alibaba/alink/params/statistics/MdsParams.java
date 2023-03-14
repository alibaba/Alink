package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

public interface MdsParams<T>
	extends WithParams <T>, HasSelectedColsDefaultAsNull <T>, HasReservedColsDefaultAsNull <T> {

	/**
	 * Param "dim"
	 */
	@NameCn("维度数目")
	@DescCn("降维后的维度数目")
	ParamInfo <Integer> DIM = ParamInfoFactory
		.createParamInfo("dim", Integer.class)
		.setDescription("number of dimensions")
		.setHasDefaultValue(2)
		.setAlias(new String[] {"numDimensions"})
		.build();

	default Integer getDim() {
		return get(DIM);
	}

	default T setDim(Integer value) {
		return set(DIM, value);
	}

	/**
	 * Param "outputColPrefix"
	 */
	@NameCn("输出列的前缀")
	@DescCn("输出列的前缀")
	ParamInfo <String> OUTPUT_COL_PREFIX = ParamInfoFactory
		.createParamInfo("outputColPrefix", String.class)
		.setDescription("output column prefix")
		.setHasDefaultValue("coord")
		.setAlias(new String[] {"coordsColNamePrefix"})
		.build();

	default String getOutputColPrefix() {
		return get(OUTPUT_COL_PREFIX);
	}

	default T setOutputColPrefix(String value) {
		return set(OUTPUT_COL_PREFIX, value);
	}
}
