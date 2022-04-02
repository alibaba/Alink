package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.StringIndexerModelMapper;
import com.alibaba.alink.params.dataproc.StringIndexerPredictParams;

/**
 * Map string to index.
 */
@InputPorts(values = {
	@PortSpec(value = PortType.MODEL, opType = OpType.BATCH, desc = PortDesc.PREDICT_INPUT_MODEL, suggestions =
		StringIndexerTrainBatchOp.class),
	@PortSpec(value = PortType.DATA, desc = PortDesc.PREDICT_INPUT_DATA)
})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.INT_LONG_STRING_TYPES)
@NameCn("StringIndexer预测")
public final class StringIndexerPredictBatchOp
	extends ModelMapBatchOp <StringIndexerPredictBatchOp>
	implements StringIndexerPredictParams <StringIndexerPredictBatchOp> {

	private static final long serialVersionUID = 3074096923032622056L;

	public StringIndexerPredictBatchOp() {
		this(new Params());
	}

	public StringIndexerPredictBatchOp(Params params) {
		super(StringIndexerModelMapper::new, params);
	}
}
