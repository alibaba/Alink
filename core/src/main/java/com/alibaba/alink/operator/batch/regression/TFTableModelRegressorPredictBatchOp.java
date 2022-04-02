package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.FlatModelMapBatchOp;
import com.alibaba.alink.operator.common.regression.tensorflow.TFTableModelRegressionFlatModelMapper;
import com.alibaba.alink.params.regression.TFTableModelRegressionPredictParams;

@Internal
@NameCn("TF表模型回归预测")
public class TFTableModelRegressorPredictBatchOp<T extends TFTableModelRegressorPredictBatchOp <T>>
	extends FlatModelMapBatchOp <T> implements TFTableModelRegressionPredictParams <T> {

	public TFTableModelRegressorPredictBatchOp() {
		this(new Params());
	}

	public TFTableModelRegressorPredictBatchOp(Params params) {
		super(TFTableModelRegressionFlatModelMapper::new, params);
	}
}
