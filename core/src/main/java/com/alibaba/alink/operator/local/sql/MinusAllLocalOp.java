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
 * Minus another <code>BatchOperator</code>. The duplicated records are kept.
 */
@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：MinusAll")
public final class MinusAllLocalOp extends LocalOperator <MinusAllLocalOp> {

	private static final long serialVersionUID = -7582100858266866075L;

	public MinusAllLocalOp() {
		this(new Params());
	}

	public MinusAllLocalOp(Params params) {
		super(params);
	}

	@Override
	public MinusAllLocalOp linkFrom(LocalOperator <?>... inputs) {
		checkMinOpSize(2, inputs);
		this.setOutputTable(LocalMLEnvironment.getInstance().getSqlExecutor()
			.minusAll(inputs[0], inputs[1])
			.getOutputTable());
		return this;
	}
}
