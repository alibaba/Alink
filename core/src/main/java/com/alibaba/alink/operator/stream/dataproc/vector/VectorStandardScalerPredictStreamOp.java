package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelMapper;
import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorStandardPredictParams;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
@NameCn("向量标准化预测")
@NameEn("Vector standard scaler prediction")
public class VectorStandardScalerPredictStreamOp extends ModelMapStreamOp <VectorStandardScalerPredictStreamOp>
	implements VectorStandardPredictParams <VectorStandardScalerPredictStreamOp> {

	private static final long serialVersionUID = -8439975525324629930L;

	public VectorStandardScalerPredictStreamOp() {
		super(VectorStandardScalerModelMapper::new, new Params());
	}

	public VectorStandardScalerPredictStreamOp(Params params) {
		super(VectorStandardScalerModelMapper::new, params);
	}

	public VectorStandardScalerPredictStreamOp(BatchOperator srt) {
		this(srt, new Params());
	}

	public VectorStandardScalerPredictStreamOp(BatchOperator srt, Params params) {
		super(srt, VectorStandardScalerModelMapper::new, params);
	}

}
