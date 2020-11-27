package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.operator.common.tree.parallelcart.leafscoreupdater.LeafScoreUpdater;
import com.alibaba.alink.operator.common.tree.parallelcart.leafscoreupdater.LeafScoreUpdaterFactory;
import com.alibaba.alink.operator.common.tree.parallelcart.leafscoreupdater.LeafScoreUpdaterType;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class UpdateLeafScore extends ComputeFunction {
	private final static Logger LOG = LoggerFactory.getLogger(UpdateLeafScore.class);
	private static final long serialVersionUID = -1147524895767655755L;

	@Override
	public void calc(ComContext context) {
		BoostingObjs objs = context.getObj(InitBoostingObjs.BOOSTING_OBJS);

		if (context.getStepNo() == 1) {
			context.putObj(
				"updater",
				LeafScoreUpdaterFactory
					.createLeafScoreUpdater(objs.params.get(LeafScoreUpdaterType.LEAF_SCORE_UPDATER_TYPE))
			);
		}

		if (objs.inWeakLearner) {
			return;
		}

		LOG.info("taskId: {}, {} start", context.getTaskId(), UpdateLeafScore.class.getSimpleName());
		HistogramBaseTreeObjs tree = context.getObj("tree");
		LeafScoreUpdater updater = context.getObj("updater");

		final double learningRate = objs.params.get(GbdtTrainParams.LEARNING_RATE);

		for (NodeInfoPair.NodeInfo nodeInfo : tree.leaves) {
			updater.update(
				nodeInfo.node.getCounter().getWeightSum(),
				nodeInfo.node.getCounter().getDistributions(),
				learningRate
			);
		}
		LOG.info("taskId: {}, {} end", context.getTaskId(), UpdateLeafScore.class.getSimpleName());
	}
}
