package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.batch.utils.WithTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.fm.FmClassifierModelInfo;
import com.alibaba.alink.operator.common.fm.FmClassifierModelTrainInfo;
import com.alibaba.alink.operator.common.fm.FmTrainBatchOp;
import com.alibaba.alink.params.recommendation.FmTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.List;

/**
 * Fm classification train algorithm. the input of this algorithm can be vector or table.
 */
@NameCn("FM分类训练")
@NameEn("FM Classification Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.FmClassifier")
public class FmClassifierTrainBatchOp extends FmTrainBatchOp <FmClassifierTrainBatchOp>
	implements FmTrainParams <FmClassifierTrainBatchOp>,
	WithModelInfoBatchOp <FmClassifierModelInfo, FmClassifierTrainBatchOp, FmClassifierModelInfoBatchOp>,
	WithTrainInfo <FmClassifierModelTrainInfo, FmClassifierTrainBatchOp> {

	private static final long serialVersionUID = -8385944325790904485L;

	public FmClassifierTrainBatchOp() {
		super(new Params(), Task.BINARY_CLASSIFICATION);
	}

	public FmClassifierTrainBatchOp(Params params) {
		super(params, Task.BINARY_CLASSIFICATION);
	}

	@Override
	public FmClassifierModelTrainInfo createTrainInfo(List <Row> rows) {
		return new FmClassifierModelTrainInfo(rows);
	}

	@Override
	public BatchOperator <?> getSideOutputTrainInfo() {
		return this.getSideOutput(0);
	}

	/**
	 * get model info of this train process.
	 *
	 * @return FmClassifierModelInfoBatchOp.
	 */
	@Override
	public FmClassifierModelInfoBatchOp getModelInfoBatchOp() {
		return new FmClassifierModelInfoBatchOp()
			.setMLEnvironmentId(this.getMLEnvironmentId()).linkFrom(this);
	}
}
