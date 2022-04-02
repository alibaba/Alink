package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

public class VectorToTensorMapper extends ToTensorMapper {

	public VectorToTensorMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}
}
