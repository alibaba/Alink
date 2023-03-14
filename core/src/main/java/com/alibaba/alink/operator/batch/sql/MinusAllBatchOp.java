package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Minus another <code>BatchOperator</code>. The duplicated records are kept.
 */
@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：MinusAll")
@NameEn("SQL MinusAll Operation")
public final class MinusAllBatchOp extends BatchOperator <MinusAllBatchOp> {

	private static final long serialVersionUID = -7582100858266866075L;

	public MinusAllBatchOp() {
		this(new Params());
	}

	public MinusAllBatchOp(Params params) {
		super(params);
	}

	@Override
	public MinusAllBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkMinOpSize(2, inputs);
		this.setOutputTable(inputs[0].getOutputTable().minusAll(inputs[1].getOutputTable()));
		return this;
	}
}
