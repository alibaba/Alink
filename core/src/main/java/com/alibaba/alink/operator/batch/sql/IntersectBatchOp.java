package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Intersect two batch  operators. It returns records that exist in both batch
 * operators. Duplicated records are removed.
 */
@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：Intersect")
public final class IntersectBatchOp extends BatchOperator <IntersectBatchOp> {

	private static final long serialVersionUID = -2981473236917210647L;

	public IntersectBatchOp() {
		this(new Params());
	}

	public IntersectBatchOp(Params params) {
		super(params);
	}

	@Override
	public IntersectBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);
		this.setOutputTable(inputs[0].getOutputTable().intersect(inputs[1].getOutputTable()));
		return this;
	}
}
