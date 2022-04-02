package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Minus another <code>BatchOperator</code>.
 */
@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：Minus")
public final class MinusBatchOp extends BatchOperator <MinusBatchOp> {

	private static final long serialVersionUID = 5643333177043157438L;

	public MinusBatchOp() {
		this(new Params());
	}

	public MinusBatchOp(Params params) {
		super(params);
	}

	@Override
	public MinusBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);
		this.setOutputTable(inputs[0].getOutputTable().minus(inputs[1].getOutputTable()));
		return this;
	}
}
