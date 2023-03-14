package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.params.classification.C45TrainParams;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaltAs10;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Fit a c45 model.
 */
@NameCn("C45决策树分类训练")
@NameEn("C45 Decision Tree Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.C45")
public class C45TrainBatchOp extends BaseRandomForestTrainBatchOp <C45TrainBatchOp>
	implements C45TrainParams <C45TrainBatchOp>,
	WithModelInfoBatchOp <TreeModelInfo.DecisionTreeModelInfo, C45TrainBatchOp, C45ModelInfoBatchOp> {

	private static final long serialVersionUID = -1894634246411633664L;

	public C45TrainBatchOp() {
		this(null);
	}

	public C45TrainBatchOp(Params parameter) {
		super(parameter);

		getParams().set(TreeUtil.TREE_TYPE, TreeUtil.TreeType.INFOGAINRATIO);
		getParams().set(HasNumTreesDefaltAs10.NUM_TREES, 1);
		getParams().set(HasFeatureSubsamplingRatio.FEATURE_SUBSAMPLING_RATIO, 1.0);
		getParams().set(HasSubsamplingRatio.SUBSAMPLING_RATIO, 1.0);
	}

	@Override
	public C45ModelInfoBatchOp getModelInfoBatchOp() {
		return new C45ModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
