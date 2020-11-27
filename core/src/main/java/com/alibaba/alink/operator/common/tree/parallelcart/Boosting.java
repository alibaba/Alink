package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.operator.common.tree.parallelcart.booster.Booster;
import com.alibaba.alink.operator.common.tree.parallelcart.booster.BoosterFactory;
import com.alibaba.alink.operator.common.tree.parallelcart.booster.BoosterType;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Slice;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Boosting extends ComputeFunction {
	private final static Logger LOG = LoggerFactory.getLogger(Boosting.class);
	private static final long serialVersionUID = 9179338616517355846L;

	public static final String BOOSTER = "booster";

	@Override
	public void calc(ComContext context) {
		BoostingObjs boostingObjs = context.getObj(InitBoostingObjs.BOOSTING_OBJS);

		if (boostingObjs.inWeakLearner) {
			return;
		}

		LOG.info("taskId: {}, {} start", context.getTaskId(), Boosting.class.getSimpleName());

		if (context.getStepNo() == 1) {
			Booster booster;
			if (LossUtils.isRanking(boostingObjs.params.get(LossUtils.LOSS_TYPE))) {
				booster = BoosterFactory
					.createRankingBooster(
						boostingObjs.params.get(BoosterType.BOOSTER_TYPE),
						boostingObjs.rankingLoss,
						boostingObjs.data.getQueryIdOffset(),
						boostingObjs.data.getWeights(),
						new Slice(0, boostingObjs.data.getQueryIdOffset().length - 1),
						new Slice(0, boostingObjs.data.getM())
					);
			} else {
				booster = BoosterFactory
					.createBooster(
						boostingObjs.params.get(BoosterType.BOOSTER_TYPE),
						boostingObjs.loss,
						boostingObjs.data.getWeights(),
						new Slice(0, boostingObjs.data.getM())
					);
			}

			context.putObj(BOOSTER, booster);
		}

		context. <Booster>getObj(BOOSTER)
			.boosting(boostingObjs, boostingObjs.data.getLabels(), boostingObjs.pred);

		boostingObjs.numBoosting++;

		LOG.info("taskId: {}, {} end", context.getTaskId(), Boosting.class.getSimpleName());
	}
}
