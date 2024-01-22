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
 * Union with another <code>BatchOperator</code>. The duplicated records are kept.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, isRepeated = true))
@OutputPorts(values = @PortSpec(value = PortType.DATA))
@NameCn("SQL操作：UnionAll")
public final class UnionAllLocalOp extends LocalOperator <UnionAllLocalOp> {

	private static final long serialVersionUID = 2468662188701775196L;

	public UnionAllLocalOp() {
		this(new Params());
	}

	public UnionAllLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		checkMinOpSize(1, inputs);
		LocalOperator <?> output = inputs[0];
		for (int i = 1; i < inputs.length; i += 1) {
			output = LocalMLEnvironment.getInstance().getSqlExecutor().unionAll(output, inputs[i]);
		}
		setOutputTable(output.getOutputTable());
	}
}
