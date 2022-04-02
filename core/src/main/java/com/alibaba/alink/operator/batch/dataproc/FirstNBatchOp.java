package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.FirstReducer;
import com.alibaba.alink.params.dataproc.FirstNParams;

/**
 * BatchOperator to select first n records.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("前N个数")
public class FirstNBatchOp extends BatchOperator <FirstNBatchOp>
	implements FirstNParams <FirstNBatchOp> {

	private static final long serialVersionUID = -25837624812528880L;

	public FirstNBatchOp() {
		this(new Params());
	}

	public FirstNBatchOp(Params params) {
		super(params);
	}

	@Override
	public FirstNBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		int n = getSize();
		DataSet <Row> rows = in.getDataSet().reduceGroup(new FirstReducer <>(n));
		setOutput(rows, in.getSchema());
		return this;
	}
}
