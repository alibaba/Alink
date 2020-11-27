package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Data;
import com.alibaba.alink.operator.common.tree.parallelcart.data.DataUtil;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.GBRankLoss;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LambdaLoss;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LeastSquare;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LogLoss;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossType;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.RankingLossFunc;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.UnaryLossFuncWithPrior;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import com.alibaba.alink.params.shared.tree.HasSeed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;

public final class InitBoostingObjs extends ComputeFunction {
	private final static Logger LOG = LoggerFactory.getLogger(InitBoostingObjs.class);
	private static final long serialVersionUID = 3534441875143136651L;

	public static final String BOOSTING_OBJS = "boostingObjs";
	public static final String FEATURE_METAS = "featureMetas";
	public static final String TRAIN_DATA = "trainData";

	private final Params params;

	public InitBoostingObjs(Params params) {
		this.params = params;
	}

	@Override
	public void calc(ComContext context) {
		if (context.getStepNo() != 1) {
			return;
		}

		LOG.info("taskId: {}, {} start", context.getTaskId(), InitBoostingObjs.class.getSimpleName());
		List <Row> rawData = context.getObj(TRAIN_DATA);

		boolean useEpsilonApproQuantile = params.get(BaseGbdtTrainBatchOp.USE_EPSILON_APPRO_QUANTILE);

		Data data = DataUtil.createData(
			params, context.getObj(FEATURE_METAS),
			rawData == null ? 0 : rawData.size(),
			useEpsilonApproQuantile
		);

		if (useEpsilonApproQuantile) {
			data.loadFromRowWithContinues(rawData);
		} else {
			data.loadFromRow(rawData);
		}

		context.removeObj(FEATURE_METAS);
		context.removeObj(TRAIN_DATA);

		LOG.info("taskId: {}, data shape, M: {}, N: {}", context.getTaskId(), data.getM(), data.getN());

		BoostingObjs boostingObjs = new BoostingObjs();

		boostingObjs.params = params;
		boostingObjs.data = data;

		if (LossUtils.isRanking(params.get(LossUtils.LOSS_TYPE))) {
			boostingObjs.rankingLoss = createRankingLoss(
				params.get(LossUtils.LOSS_TYPE),
				params,
				data.getQueryIdOffset(),
				data.getLabels(),
				data.getWeights()
			);
		} else {
			List <Tuple2 <Double, Long>> initialVal = context.getObj("gbdt.y.sum");
			boostingObjs.loss = createUnaryLoss(
				params.get(LossUtils.LOSS_TYPE), initialVal.get(0)
			);
			boostingObjs.prior = boostingObjs.loss.prior();
			boostingObjs.numBaggingInstances = (int) Math.min(
				boostingObjs.data.getM(),
				Math.ceil(
					boostingObjs.data.getM() * params.get(GbdtTrainParams.SUBSAMPLING_RATIO)
				)
			);
		}

		boostingObjs.instanceRandomizer = new Random(
			context.getTaskId() + params.get(HasSeed.SEED)
		);
		boostingObjs.featureRandomizer = new Random(
			params.get(HasSeed.SEED)
		);

		boostingObjs.numBaggingFeatures = baggingFeatureCount(boostingObjs.data.getN());
		boostingObjs.featureIndices = new int[data.getN()];
		for (int i = 0; i < data.getN(); ++i) {
			boostingObjs.featureIndices[i] = i;
		}

		boostingObjs.indices = new int[boostingObjs.data.getM()];

		// for bagging flags.
		boostingObjs.baggingFlags = new BitSet(boostingObjs.data.getM());

		boostingObjs.pred = new double[boostingObjs.data.getM()];
		for (int i = 0; i < boostingObjs.data.getM(); ++i) {
			boostingObjs.indices[i] = i;
			boostingObjs.pred[i] = boostingObjs.prior;
		}

		boostingObjs.inWeakLearner = false;
		boostingObjs.numBoosting = 0;

		boostingObjs.executorService = Executors.newFixedThreadPool(8);

		context.putObj(BOOSTING_OBJS, boostingObjs);

		LOG.info("taskId: {}, {} end", context.getTaskId(), InitBoostingObjs.class.getSimpleName());
	}

	private int baggingFeatureCount(int nFeatureCol) {
		return Math.max(1,
			Math.min(
				(int) (params.get(GbdtTrainParams.FEATURE_SUBSAMPLING_RATIO) * nFeatureCol),
				nFeatureCol
			)
		);
	}

	private UnaryLossFuncWithPrior createUnaryLoss(LossType lossType, Tuple2 <Double, Long> labelStat) {
		switch (lossType) {
			case LOG_LOSS:
				return new LogLoss(labelStat.f0, labelStat.f1.doubleValue() - labelStat.f0);
			case LEASE_SQUARE:
				return new LeastSquare(labelStat.f0, labelStat.f1.doubleValue());
			default:
				throw new UnsupportedOperationException("Unsupported loss.");
		}
	}

	private RankingLossFunc createRankingLoss(
		LossType lossType,
		Params params,
		int[] queryOffset,
		double[] y,
		double[] weights) {

		switch (lossType) {
			case GBRANK:
				return new GBRankLoss(params, queryOffset, y, weights);
			case LAMBDA_NDCG:
				return new LambdaLoss(params, LambdaLoss.LambdaType.NDCG, queryOffset, y, weights);
			case LAMBDA_DCG:
				return new LambdaLoss(params, LambdaLoss.LambdaType.DCG, queryOffset, y, weights);
			default:
				throw new UnsupportedOperationException("Unsupported loss.");
		}
	}

}
