package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface AkSinkBatchParams<T> extends AkSinkParams <T> {

	@NameCn("分区列")
	@DescCn("创建分区使用的列名")
	ParamInfo <String[]> PARTITION_COLS = ParamInfoFactory
		.createParamInfo("partitionCols", String[].class)
		.setDescription("Columns for creating partition")
		.setHasDefaultValue(null)
		.build();

	default T setPartitionCols(String... partitionCols) {
		return set(PARTITION_COLS, partitionCols);
	}

	default String[] getPartitionCols() {
		return get(PARTITION_COLS);
	}

}
