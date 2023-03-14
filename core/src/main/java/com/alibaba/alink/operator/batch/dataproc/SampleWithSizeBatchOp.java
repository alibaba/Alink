package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.SampleWithSizeParams;

/**
 * Sample the input data with given size with or without replacement.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("固定条数随机采样")
@NameEn("Data Sampling With Fixed Size")
public class SampleWithSizeBatchOp extends BatchOperator <SampleWithSizeBatchOp>
	implements SampleWithSizeParams <SampleWithSizeBatchOp> {

	private static final long serialVersionUID = -4682487914099349334L;

	public SampleWithSizeBatchOp() {
		this(new Params());
	}

	public SampleWithSizeBatchOp(Params params) {
		super(params);
	}

	public SampleWithSizeBatchOp(int numSamples) {
		this(numSamples, false);
	}

	public SampleWithSizeBatchOp(int numSamples, boolean withReplacement) {
		this(new Params()
			.set(SIZE, numSamples)
			.set(WITH_REPLACEMENT, withReplacement)
		);
	}

	@Override
	public SampleWithSizeBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		boolean withReplacement = getWithReplacement();
		int numSamples = getSize();
		DataSet <Row> rows = DataSetUtils.sampleWithSize(in.getDataSet(), withReplacement, numSamples);
		this.setOutput(rows, in.getSchema());
		return this;
	}

}
