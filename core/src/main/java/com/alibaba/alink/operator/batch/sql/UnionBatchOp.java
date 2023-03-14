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
 * Union with other <code>BatchOperator</code>s.
 */

@InputPorts(values = @PortSpec(value = PortType.DATA, isRepeated = true))
@OutputPorts(values = @PortSpec(value = PortType.DATA))
@NameCn("SQL操作：Union")
@NameEn("SQL Union Operation")
public final class UnionBatchOp extends BatchOperator <UnionBatchOp> {

	private static final long serialVersionUID = 6141413513148024360L;

	public UnionBatchOp() {
		this(new Params());
	}

	public UnionBatchOp(Params params) {
		super(params);
	}

	@Override
	public UnionBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkMinOpSize(1, inputs);
		this.setOutputTable(inputs[0].getOutputTable());
		for (int i = 1; i < inputs.length; i++) {
			this.setOutputTable(this.getOutputTable().union(inputs[i].getOutputTable()));
		}
		return this;
	}
}
