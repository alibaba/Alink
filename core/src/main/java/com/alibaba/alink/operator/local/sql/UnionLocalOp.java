package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.local.LocalOperator;

/**
 * Union with other <code>BatchOperator</code>s.
 */

@InputPorts(values = @PortSpec(value = PortType.DATA, isRepeated = true))
@OutputPorts(values = @PortSpec(value = PortType.DATA))
@NameCn("SQL操作：Union")
public final class UnionLocalOp extends LocalOperator <UnionLocalOp> {

	private static final long serialVersionUID = 6141413513148024360L;

	public UnionLocalOp() {
		this(new Params());
	}

	public UnionLocalOp(Params params) {
		super(params);
	}

	@Override
	public UnionLocalOp linkFrom(LocalOperator <?>... inputs) {
		checkMinOpSize(1, inputs);
		LocalOperator <?> output = inputs[0];
		for (int i = 1; i < inputs.length; i += 1) {
			output = LocalMLEnvironment.getInstance().getSqlExecutor().union(output, inputs[i]);
		}
		setOutputTable(output.getOutputTable());
		return this;
	}
}
