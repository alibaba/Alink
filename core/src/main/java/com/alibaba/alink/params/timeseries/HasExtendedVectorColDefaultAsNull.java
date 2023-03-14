package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasExtendedVectorColDefaultAsNull<T> extends WithParams <T> {
	@NameCn("扩展Vector列")
	@DescCn("扩展Vector列")
	ParamInfo <String> EXTENDED_VECTOR_COL = ParamInfoFactory
		.createParamInfo("extendedVectorCol", String.class)
		.setDescription("Names of the columns used for processing")
		.setHasDefaultValue(null)
		.build();

	default String getExtendedVectorCol() {
		return get(EXTENDED_VECTOR_COL);
	}

	default T setExtendedVectorCol(String colName) {
		return set(EXTENDED_VECTOR_COL, colName);
	}
}
