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
 * Intersect two batch  operators. It returns records that exist in both batch operators. Duplicated records are
 * removed.
 */
@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：Intersect")
public final class IntersectLocalOp extends LocalOperator <IntersectLocalOp> {

	private static final long serialVersionUID = -2981473236917210647L;

	public IntersectLocalOp() {
		this(new Params());
	}

	public IntersectLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		checkOpSize(2, inputs);
		this.setOutputTable(LocalMLEnvironment.getInstance().getSqlExecutor()
			.intersect(inputs[0], inputs[1])
			.getOutputTable());
	}
}
