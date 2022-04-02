package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasPartitionColsDefaultAsNull<T> extends WithParams <T> {
	@NameCn("分组列名数组")
	@DescCn("分组列名，多列，可选，默认不选")
	ParamInfo <String[]> PARTITION_COLS = ParamInfoFactory
		.createParamInfo("partitionCols", String[].class)
		.setDescription("partition col names")
		.setAlias(new String[] {"partitionColNames"})
		.setHasDefaultValue(null)
		.build();

	default String[] getPartitionCols() {return get(PARTITION_COLS);}

	default T setPartitionCols(String... colNames) {return set(PARTITION_COLS, colNames);}
}
