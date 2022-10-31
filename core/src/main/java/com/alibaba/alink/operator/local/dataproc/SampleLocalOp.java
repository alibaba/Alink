package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.api.java.sampling.BernoulliSampler;
import org.apache.flink.api.java.sampling.PoissonSampler;
import org.apache.flink.api.java.sampling.RandomSampler;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.dataproc.SampleParams;
import com.alibaba.alink.params.shared.HasRandomSeed;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Sample the input data with given ratio with or without replacement.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("随机采样")
public final class SampleLocalOp extends LocalOperator <SampleLocalOp>
	implements SampleParams <SampleLocalOp>, HasRandomSeed <SampleLocalOp> {

	public SampleLocalOp() {
		this(new Params());
	}

	public SampleLocalOp(Params params) {
		super(params);
	}

	@Override
	public SampleLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		boolean withReplacement = getWithReplacement();
		double fraction = getRatio();
		long seed = (null != getRandomSeed()) ? 0 : getRandomSeed();

		RandomSampler <Row> sampler;
		if (withReplacement) {
			sampler = new PoissonSampler <>(fraction, seed);
		} else {
			sampler = new BernoulliSampler <>(fraction, seed);
		}
		Iterator <Row> sampled = sampler.sample(in.getOutputTable().getRows().iterator());

		ArrayList <Row> result = new ArrayList <>();
		while (sampled.hasNext()) {
			result.add(sampled.next());
		}

		this.setOutputTable(new MTable(result, in.getSchema()));
		return this;
	}

}
