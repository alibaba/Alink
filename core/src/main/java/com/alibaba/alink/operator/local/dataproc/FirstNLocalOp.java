package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.dataproc.FirstNParams;

/**
 * LocalOperator to select first n records.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("前N个数")
public class FirstNLocalOp extends LocalOperator <FirstNLocalOp>
	implements FirstNParams <FirstNLocalOp> {

	private static final long serialVersionUID = -25837624812528880L;

	public FirstNLocalOp() {
		this(new Params());
	}

	public FirstNLocalOp(Params params) {
		super(params);
	}

	@Override
	public FirstNLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		int n = getSize();
		MTable output = in.getOutputTable().subTable(0, Math.min(n, in.getOutputTable().getNumRow()));
		setOutputTable(output);
		return this;
	}
}
