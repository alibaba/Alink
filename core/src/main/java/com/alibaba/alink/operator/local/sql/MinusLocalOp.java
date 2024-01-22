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
 * Minus another <code>BatchOperator</code>.
 */
@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：Minus")
public final class MinusLocalOp extends LocalOperator <MinusLocalOp> {

	private static final long serialVersionUID = 5643333177043157438L;

	public MinusLocalOp() {
		this(new Params());
	}

	public MinusLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		checkOpSize(2, inputs);
		this.setOutputTable(LocalMLEnvironment.getInstance().getSqlExecutor()
			.minus(inputs[0], inputs[1])
			.getOutputTable());
	}
}
