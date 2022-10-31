package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.dataproc.SampleWithSizeParams;
import com.alibaba.alink.params.shared.HasRandomSeed;

import java.util.Random;

/**
 * Sample the input data with given size with or without replacement.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("固定条数随机采样")
public class SampleWithSizeLocalOp extends LocalOperator <SampleWithSizeLocalOp>
	implements SampleWithSizeParams <SampleWithSizeLocalOp>, HasRandomSeed <SampleWithSizeLocalOp> {

	public SampleWithSizeLocalOp() {
		this(new Params());
	}

	public SampleWithSizeLocalOp(Params params) {
		super(params);
	}

	@Override
	public SampleWithSizeLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		MTable table = in.getOutputTable();
		boolean withReplacement = getWithReplacement();
		long seed = (null == getRandomSeed()) ? 0 : getRandomSeed();

		int numSamples = getSize();
		MTable out = withReplacement
			? table.sampleWithSizeReplacement(numSamples, new Random(seed))
			: table.sampleWithSize(numSamples, new Random(seed));
		setOutputTable(out);
		return this;
	}
}
