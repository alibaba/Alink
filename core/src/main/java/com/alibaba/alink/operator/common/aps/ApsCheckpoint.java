package com.alibaba.alink.operator.common.aps;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

public abstract class ApsCheckpoint {
	public abstract void write(BatchOperator <?> operator, String identity, Long mlEnvId, Params params);

	public abstract BatchOperator <?> read(String identity, Long mlEnvId, Params params);
}
