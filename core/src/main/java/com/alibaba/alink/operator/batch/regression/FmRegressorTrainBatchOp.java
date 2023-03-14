package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.batch.utils.WithTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.fm.FmRegressorModelInfo;
import com.alibaba.alink.operator.common.fm.FmRegressorModelTrainInfo;
import com.alibaba.alink.operator.common.fm.FmTrainBatchOp;
import com.alibaba.alink.params.recommendation.FmTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.List;

/**
 * Fm regression train algorithm. the input of this algorithm can be vector or table.
 */
@NameCn("FM回归训练")
@NameEn("FM Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.FmRegressor")
public class FmRegressorTrainBatchOp extends FmTrainBatchOp <FmRegressorTrainBatchOp>
	implements FmTrainParams <FmRegressorTrainBatchOp>,
	WithModelInfoBatchOp <FmRegressorModelInfo, FmRegressorTrainBatchOp, FmRegressorModelInfoBatchOp>,
	WithTrainInfo <FmRegressorModelTrainInfo, FmRegressorTrainBatchOp> {
	private static final long serialVersionUID = 8297633489045835451L;

	public FmRegressorTrainBatchOp(Params params) {
		super(params, Task.REGRESSION);
	}

	public FmRegressorTrainBatchOp() {
		super(new Params(),  Task.REGRESSION);
	}

	@Override
	public FmRegressorModelTrainInfo createTrainInfo(List <Row> rows) {
		return new FmRegressorModelTrainInfo(rows);
	}

	@Override
	public BatchOperator <?> getSideOutputTrainInfo() {
		return this.getSideOutput(0);
	}

	/**
	 * get model info of this train process.
	 *
	 * @return this
	 */
	@Override
	public FmRegressorModelInfoBatchOp getModelInfoBatchOp() {
		return new FmRegressorModelInfoBatchOp()
			.setMLEnvironmentId(this.getMLEnvironmentId()).linkFrom(this);
	}
}
