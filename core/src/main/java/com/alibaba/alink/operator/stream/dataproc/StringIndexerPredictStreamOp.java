package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StringIndexerTrainBatchOp;
import com.alibaba.alink.operator.common.dataproc.StringIndexerModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.StringIndexerPredictParams;

/**
 * Map string to index.
 */

@InputPorts(values = {
	@PortSpec(value = PortType.MODEL, opType = OpType.BATCH, desc = PortDesc.PREDICT_INPUT_MODEL, suggestions =
		StringIndexerTrainBatchOp.class),
	@PortSpec(value = PortType.DATA, desc = PortDesc.PREDICT_INPUT_DATA),
	@PortSpec(value = PortType.MODEL_STREAM, isOptional = true, desc = PortDesc.PREDICT_INPUT_MODEL_STREAM)
})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.INT_LONG_STRING_TYPES)
@NameCn("StringIndexer预测")
public final class StringIndexerPredictStreamOp
	extends ModelMapStreamOp <StringIndexerPredictStreamOp>
	implements StringIndexerPredictParams <StringIndexerPredictStreamOp> {

	private static final long serialVersionUID = -6599742412462261688L;

	public StringIndexerPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public StringIndexerPredictStreamOp(BatchOperator model, Params params) {
		super(model, StringIndexerModelMapper::new, params);
	}
}
