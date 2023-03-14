package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.StringIndexerModelMapper;
import com.alibaba.alink.operator.common.feature.TargetEncoderModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.feature.TargetEncoderPredictParams;

@ParamSelectColumnSpec(name = "selectedCols")
@NameCn("TargetEncoder")
@NameEn("TargetEncoder")
public class TargetEncoderPredictStreamOp extends ModelMapStreamOp <TargetEncoderPredictStreamOp>
	implements TargetEncoderPredictParams <TargetEncoderPredictStreamOp> {

	public TargetEncoderPredictStreamOp() {
		super(TargetEncoderModelMapper::new, new Params());
	}

	public TargetEncoderPredictStreamOp(Params params) {
		super(TargetEncoderModelMapper::new, params);
	}

	public TargetEncoderPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public TargetEncoderPredictStreamOp(BatchOperator model, Params params) {
		super(model, TargetEncoderModelMapper::new, params);
	}
}