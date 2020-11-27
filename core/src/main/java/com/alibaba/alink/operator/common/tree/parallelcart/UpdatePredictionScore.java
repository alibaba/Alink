package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class UpdatePredictionScore extends ComputeFunction {
	private final static Logger LOG = LoggerFactory.getLogger(UpdatePredictionScore.class);
	private static final long serialVersionUID = -1747045532702426860L;

	@Override
	public void calc(ComContext context) {
		BoostingObjs objs = context.getObj(InitBoostingObjs.BOOSTING_OBJS);

		if (objs.inWeakLearner) {
			return;
		}
		LOG.info("taskId: {}, {} start", context.getTaskId(), UpdatePredictionScore.class.getSimpleName());
		HistogramBaseTreeObjs tree = context.getObj("tree");

		for (NodeInfoPair.NodeInfo nodeInfo : tree.leaves) {
			double pred = nodeInfo.node.getCounter().getDistributions()[0];

			for (int i = nodeInfo.slice.start; i < nodeInfo.slice.end; ++i) {
				objs.pred[objs.indices[i]] += pred;
			}

			for (int i = nodeInfo.oob.start; i < nodeInfo.oob.end; ++i) {
				objs.pred[objs.indices[i]] += pred;
			}
		}
		LOG.info("taskId: {}, {} end", context.getTaskId(), UpdatePredictionScore.class.getSimpleName());
	}
}
