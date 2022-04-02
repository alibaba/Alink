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
 * operators. Duplicated records are kept.
 */
@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：IntersectAll")
public final class IntersectAllBatchOp extends BatchOperator <IntersectAllBatchOp> {
	private static final long serialVersionUID = -8644196260740789294L;

	public IntersectAllBatchOp() {
		this(new Params());
	}

	public IntersectAllBatchOp(Params params) {
		super(params);
	}

	@Override
	public IntersectAllBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		this.setOutputTable(inputs[0].getOutputTable().intersectAll(inputs[1].getOutputTable()));
		return this;
	}
}
