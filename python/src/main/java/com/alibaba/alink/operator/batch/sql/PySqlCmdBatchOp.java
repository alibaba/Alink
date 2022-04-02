package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.sql.PySqlCmdParams;

@SuppressWarnings("unused")
public final class PySqlCmdBatchOp extends BatchOperator <PySqlCmdBatchOp> implements PySqlCmdParams <PySqlCmdBatchOp> {

	public PySqlCmdBatchOp() {
		this(new Params());
	}

	public PySqlCmdBatchOp(Params params) {
		super(params);
	}

	@Override
	public PySqlCmdBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkMinOpSize(1, inputs);
		long sessionId = getMLEnvironmentId();
		final String cmd = getCommand();
		for (BatchOperator <?> input : inputs) {
			if (sessionId != input.getMLEnvironmentId()) {
				sessionId = input.getMLEnvironmentId();
			}
		}
		if (getMLEnvironmentId() != sessionId) {
			setMLEnvironmentId(sessionId);
		}
		final MLEnvironment env = MLEnvironmentFactory.get(sessionId);
		this.setOutputTable(env.batchSQL(cmd).setMLEnvironmentId(sessionId).getOutputTable());
		return this;
	}
}
