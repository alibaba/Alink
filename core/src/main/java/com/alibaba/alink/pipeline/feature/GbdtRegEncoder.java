package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.GbdtRegTrainBatchOp;
import com.alibaba.alink.params.feature.GbdtRegEncoderParams;
import com.alibaba.alink.params.regression.GbdtRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("Gbdt回归编码")
public class GbdtRegEncoder extends Trainer <GbdtRegEncoder, GbdtRegEncoderModel> implements
	GbdtRegTrainParams <GbdtRegEncoder>,
	GbdtRegEncoderParams <GbdtRegEncoder> {

	private static final long serialVersionUID = -6668055009648285896L;

	public GbdtRegEncoder() {
		this(new Params());
	}

	public GbdtRegEncoder(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new GbdtRegTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
