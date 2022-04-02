package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.OneHotTrainBatchOp;
import com.alibaba.alink.operator.common.feature.OneHotModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.feature.OneHotPredictParams;

/**
 * *
 * A one-hot stream operator that maps a serial of columns of category indices to a column of
 * sparse binary vectors.
 */
@InputPorts(values = {
	@PortSpec(value = PortType.MODEL, opType = OpType.BATCH, desc = PortDesc.PREDICT_INPUT_MODEL, suggestions =
		OneHotTrainBatchOp.class),
	@PortSpec(value = PortType.DATA, desc = PortDesc.PREDICT_INPUT_DATA),
	@PortSpec(value = PortType.MODEL_STREAM, isOptional = true, desc = PortDesc.PREDICT_INPUT_MODEL_STREAM)
})
@NameCn("独热编码预测")
public final class OneHotPredictStreamOp extends ModelMapStreamOp <OneHotPredictStreamOp>
	implements OneHotPredictParams <OneHotPredictStreamOp> {

	private static final long serialVersionUID = 3986423530880867993L;

	/**
	 * constructor.
	 *
	 * @param model the model.
	 */
	public OneHotPredictStreamOp(BatchOperator model) {
		super(model, OneHotModelMapper::new, new Params());
	}

	/**
	 * @param model  the model.
	 * @param params the parameter set.
	 */
	public OneHotPredictStreamOp(BatchOperator model, Params params) {
		super(model, OneHotModelMapper::new, params);
	}
}
