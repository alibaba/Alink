package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.sql.PySqlCmdParams;

@SuppressWarnings("unused")
public final class PySqlCmdStreamOp extends StreamOperator <PySqlCmdStreamOp>
	implements PySqlCmdParams <PySqlCmdStreamOp> {

	public PySqlCmdStreamOp() {
		this(new Params());
	}

	public PySqlCmdStreamOp(Params params) {
		super(params);
	}

	@Override
	public PySqlCmdStreamOp linkFrom(StreamOperator <?>... inputs) {
		checkMinOpSize(1, inputs);
		long sessionId = getMLEnvironmentId();
		final String cmd = getCommand();
		for (StreamOperator <?> input : inputs) {
			if (sessionId != input.getMLEnvironmentId()) {
				sessionId = input.getMLEnvironmentId();
			}
		}
		if (getMLEnvironmentId() != sessionId) {
			setMLEnvironmentId(sessionId);
		}
		final MLEnvironment env = MLEnvironmentFactory.get(sessionId);
		this.setOutputTable(env.streamSQL(cmd).setMLEnvironmentId(sessionId).getOutputTable());
		return this;
	}
}
