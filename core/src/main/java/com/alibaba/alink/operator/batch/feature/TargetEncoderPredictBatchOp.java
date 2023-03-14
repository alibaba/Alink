package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.TargetEncoderModelMapper;
import com.alibaba.alink.params.feature.TargetEncoderPredictParams;

@ParamSelectColumnSpec(name = "selectedCols")
@NameCn("TargetEncoder")
@NameEn("TargetEncoder")
public class TargetEncoderPredictBatchOp extends ModelMapBatchOp <TargetEncoderPredictBatchOp>
	implements TargetEncoderPredictParams <TargetEncoderPredictBatchOp> {

	public TargetEncoderPredictBatchOp() {
		this(new Params());
	}

	public TargetEncoderPredictBatchOp(Params params) {
		super(TargetEncoderModelMapper::new, params);
	}
}
