package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.SparseFeatureIndexerModelMapper;
import com.alibaba.alink.params.dataproc.SparseFeatureIndexerPredictParams;

@InputPorts(values = {
	@PortSpec(value = PortType.MODEL, opType = OpType.BATCH, desc = PortDesc.PREDICT_INPUT_MODEL, suggestions =
		SparseFeatureIndexerTrainBatchOp.class),
	@PortSpec(value = PortType.DATA, desc = PortDesc.PREDICT_INPUT_DATA)
})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("稀疏特征编码预测")
@NameEn("Sparse Feature Indexer Predict")
public final class SparseFeatureIndexerPredictBatchOp
	extends ModelMapBatchOp <SparseFeatureIndexerPredictBatchOp>
	implements SparseFeatureIndexerPredictParams <SparseFeatureIndexerPredictBatchOp> {

	private static final long serialVersionUID = 3074096923032622056L;

	public SparseFeatureIndexerPredictBatchOp() {
		this(new Params());
	}

	public SparseFeatureIndexerPredictBatchOp(Params params) {
		super(SparseFeatureIndexerModelMapper::new, params);
	}
}
