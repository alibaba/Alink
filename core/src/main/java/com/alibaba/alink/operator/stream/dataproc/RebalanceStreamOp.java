package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.stream.StreamOperator;

/**
 * Rebalance data.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@NameCn("数据Rebalance")
@NameEn("Rebalance")
public final class RebalanceStreamOp extends StreamOperator <RebalanceStreamOp> {
	private static final long serialVersionUID = -4236329417415800780L;

	public RebalanceStreamOp() {
		this(new Params());
	}

	public RebalanceStreamOp(Params params) {
		super(params);
	}

	@Override
	public RebalanceStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> input = inputs[0];
		setMLEnvironmentId(input.getMLEnvironmentId());
		DataStream <Row> rows = input.getDataStream().rebalance();
		setOutput(rows, input.getSchema());
		return this;
	}
}
