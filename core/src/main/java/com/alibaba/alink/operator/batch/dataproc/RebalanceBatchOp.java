package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Rebalance data.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@NameCn("数据Rebalance")
public final class RebalanceBatchOp extends BatchOperator <RebalanceBatchOp> {
	private static final long serialVersionUID = -4236329417415800780L;

	public RebalanceBatchOp() {
		this(new Params());
	}

	public RebalanceBatchOp(Params params) {
		super(params);
	}

	@Override
	public RebalanceBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> input = inputs[0];
		setMLEnvironmentId(input.getMLEnvironmentId());
		DataSet <Row> rows = input.getDataSet().rebalance();
		setOutput(rows, input.getSchema());
		return this;
	}
}
