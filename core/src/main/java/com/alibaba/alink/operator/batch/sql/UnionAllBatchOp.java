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
 * Union with another <code>BatchOperator</code>. The duplicated records are kept.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, isRepeated = true))
@OutputPorts(values = @PortSpec(value = PortType.DATA))
@NameCn("SQL操作：UnionAll")
@NameEn("SQL UnionAll Operation")
public final class UnionAllBatchOp extends BatchOperator <UnionAllBatchOp> {

	private static final long serialVersionUID = 2468662188701775196L;

	public UnionAllBatchOp() {
		this(new Params());
	}

	public UnionAllBatchOp(Params params) {
		super(params);
	}

	@Override
	public UnionAllBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkMinOpSize(1, inputs);

		this.setOutputTable(inputs[0].getOutputTable());
		for (int i = 1; i < inputs.length; i++) {
			this.setOutputTable(this.getOutputTable().unionAll(inputs[i].getOutputTable()));
		}

		return this;
	}
}
