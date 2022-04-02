package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.FlatModelMapBatchOp;
import com.alibaba.alink.operator.common.regression.tensorflow.TFTableModelRegressionFlatModelMapper;
import com.alibaba.alink.operator.common.regression.tensorflow.TFTableModelRegressionModelMapper;
import com.alibaba.alink.params.regression.TFTableModelRegressionPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@Internal
@NameCn("TF表模型回归模型")
public class TFTableModelRegressionModel<T extends TFTableModelRegressionModel <T>> extends MapModel <T>
	implements TFTableModelRegressionPredictParams <T> {

	public TFTableModelRegressionModel() {this(null);}

	public TFTableModelRegressionModel(Params params) {
		super(TFTableModelRegressionModelMapper::new, params);
	}

	@Override
	public BatchOperator <?> transform(BatchOperator <?> input) {
		return postProcessTransformResult(new FlatModelMapBatchOp <>(
			TFTableModelRegressionFlatModelMapper::new,
			params
		).linkFrom(this.getModelData(), input));
	}
}
