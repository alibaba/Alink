package com.alibaba.alink.operator.common.tree.xgboost;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.XGboostException;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.tree.XGBoostUtil;
import com.alibaba.alink.params.xgboost.HasGrowPolicy.GrowPolicy;
import com.alibaba.alink.params.xgboost.HasObjective.Objective;
import com.alibaba.alink.params.xgboost.HasProcessType.ProcessType;
import com.alibaba.alink.params.xgboost.HasSamplingMethod.SamplingMethod;
import com.alibaba.alink.params.xgboost.HasTreeMethod.TreeMethod;
import com.alibaba.alink.params.xgboost.HasTweedieVariancePower;
import com.alibaba.alink.params.xgboost.XGBoostCommandLineParams;
import com.alibaba.alink.params.xgboost.XGBoostLearningTaskParams;
import com.alibaba.alink.params.xgboost.XGBoostTreeBoosterParams;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.Rabit;
import ml.dmlc.xgboost4j.java.RabitTracker;
import ml.dmlc.xgboost4j.java.XGBoostError;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class XGBoostImpl implements XGBoost {

	@Override
	public Tracker initTracker(int numTask) throws XGboostException {
		try {
			return new TrackerImpl(new RabitTracker(numTask));
		} catch (XGBoostError e) {
			throw new XGboostException(e);
		}
	}

	@Override
	public void init(List <Tuple2 <String, String>> workerEnvs) throws XGboostException {
		try {
			Map <String, String> localWorkerEnvs = new HashMap <>();

			for (Tuple2 <String, String> env : workerEnvs) {
				localWorkerEnvs.put(env.f0, env.f1);
			}

			Rabit.init(localWorkerEnvs);
		} catch (XGBoostError e) {
			throw new XGboostException(e);
		}
	}

	@Override
	public Booster train(
		Iterator <Row> rowIterator, Function <Row, Row> preprocess,
		Function <Row, Tuple2 <Vector, float[]>> extractor,
		Params params) throws XGboostException {

		try {
			DMatrix dMatrix = new DMatrix(
				XGBoostUtil.asLabeledPointIterator(
					rowIterator,
					XGBoostUtil.asLabeledPointConverterFunction(
						preprocess,
						extractor
					)
				),
				// non-cacheInfo
				null
			);

			return new BoosterImpl(ml.dmlc.xgboost4j.java.XGBoost.train(
				dMatrix,
				xgboostParamsToMap(params),
				params.get(XGBoostCommandLineParams.NUM_ROUND),
				new HashMap <>(),
				null, null
			));

		} catch (XGBoostError e) {
			throw new XGboostException(e);
		}
	}

	@Override
	public void shutdown() throws XGboostException {
		try {
			Rabit.shutdown();
		} catch (XGBoostError e) {
			throw new XGboostException(e);
		}
	}

	@Override
	public Booster loadModel(InputStream in) throws XGboostException, IOException {
		try {
			return new BoosterImpl(ml.dmlc.xgboost4j.java.XGBoost.loadModel(in));
		} catch (XGBoostError e) {
			throw new XGboostException(e);
		}
	}

	private static Map <String, Object> xgboostParamsToMap(Params params) {
		Map <String, Object> xgboostStyleParamMap = new HashMap <>();

		//xgboostStyleParamMap.put("verbosity", 0);
		xgboostStyleParamMap.put("eta", params.get(XGBoostTreeBoosterParams.ETA));
		xgboostStyleParamMap.put("gamma", params.get(XGBoostTreeBoosterParams.GAMMA));
		xgboostStyleParamMap.put("max_depth", params.get(XGBoostTreeBoosterParams.MAX_DEPTH));
		xgboostStyleParamMap.put("min_child_weight", params.get(XGBoostTreeBoosterParams.MIN_CHILD_WEIGHT));
		xgboostStyleParamMap.put("max_delta_step", params.get(XGBoostTreeBoosterParams.MAX_DELTA_STEP));
		xgboostStyleParamMap.put("subsample", params.get(XGBoostTreeBoosterParams.SUB_SAMPLE));

		SamplingMethod samplingMethod = params.get(XGBoostTreeBoosterParams.SAMPLING_METHOD);

		switch (samplingMethod) {
			case UNIFORM:
				xgboostStyleParamMap.put("sampling_method", "uniform");
				break;
			case GRADIENT_BASED:
				xgboostStyleParamMap.put("sampling_method", "gradient_based");
				break;
			default:
				throw new IllegalArgumentException(String.format("Illegal sampling method: %s", samplingMethod));
		}

		xgboostStyleParamMap.put("colsample_bytree", params.get(XGBoostTreeBoosterParams.COL_SAMPLE_BY_TREE));
		xgboostStyleParamMap.put("colsample_bylevel", params.get(XGBoostTreeBoosterParams.COL_SAMPLE_BY_LEVEL));
		xgboostStyleParamMap.put("colsample_bynode", params.get(XGBoostTreeBoosterParams.COL_SAMPLE_BY_NODE));
		xgboostStyleParamMap.put("lambda", params.get(XGBoostTreeBoosterParams.LAMBDA));
		xgboostStyleParamMap.put("alpha", params.get(XGBoostTreeBoosterParams.ALPHA));

		TreeMethod treeMethod = params.get(XGBoostTreeBoosterParams.TREE_METHOD);

		switch (treeMethod) {
			case AUTO:
				xgboostStyleParamMap.put("tree_method", "auto");
				break;
			case EXACT:
				xgboostStyleParamMap.put("tree_method", "exact");
				break;
			case APPROX:
				xgboostStyleParamMap.put("tree_method", "approx");
				break;
			case HIST:
				xgboostStyleParamMap.put("tree_method", "hist");
				break;
			default:
				throw new IllegalArgumentException(String.format("Illegal sampling method: %s", treeMethod));
		}

		xgboostStyleParamMap.put("sketch_eps", params.get(XGBoostTreeBoosterParams.SKETCH_EPS));
		xgboostStyleParamMap.put("scale_pos_weight", params.get(XGBoostTreeBoosterParams.SCALE_POS_WEIGHT));
		if (params.contains(XGBoostTreeBoosterParams.UPDATER)) {
			xgboostStyleParamMap.put("updater", params.get(XGBoostTreeBoosterParams.UPDATER));
		}
		xgboostStyleParamMap.put("refresh_leaf", params.get(XGBoostTreeBoosterParams.REFRESH_LEAF));

		ProcessType processType = params.get(XGBoostTreeBoosterParams.PROCESS_TYPE);

		switch (processType) {
			case DEFAULT:
				xgboostStyleParamMap.put("process_type", "default");
				break;
			case UPDATE:
				xgboostStyleParamMap.put("process_type", "update");
				break;
			default:
				throw new IllegalArgumentException(String.format("Illegal process type: %s", processType));
		}

		GrowPolicy growPolicy = params.get(XGBoostTreeBoosterParams.GROW_POLICY);

		switch (growPolicy) {
			case DEPTH_WISE:
				xgboostStyleParamMap.put("grow_policy", "depthwise");
				break;
			case LOSS_GUIDE:
				xgboostStyleParamMap.put("grow_policy", "lossguide");
				break;
			default:
				throw new IllegalArgumentException(String.format("Illegal grow policy: %s", growPolicy));
		}

		xgboostStyleParamMap.put("max_leaves", params.get(XGBoostTreeBoosterParams.MAX_LEAVES));
		xgboostStyleParamMap.put("max_bin", params.get(XGBoostTreeBoosterParams.MAX_BIN));
		xgboostStyleParamMap.put("num_class", params.get(XGBoostTreeBoosterParams.NUM_CLASS));

		if (params.contains(XGBoostTreeBoosterParams.MONOTONE_CONSTRAINTS)) {
			xgboostStyleParamMap.put(
				"monotone_constraints",
				params.get(XGBoostTreeBoosterParams.MONOTONE_CONSTRAINTS)
			);
		}

		if (params.contains(XGBoostTreeBoosterParams.INTERACTION_CONSTRAINTS)) {
			xgboostStyleParamMap.put(
				"interaction_constraints",
				params.get(XGBoostTreeBoosterParams.INTERACTION_CONSTRAINTS)
			);
		}

		xgboostStyleParamMap.put(
			"single_precision_histogram",
			params.get(XGBoostTreeBoosterParams.SINGLE_PRECISION_HISTOGRAM)
		);

		xgboostStyleParamMap.put(
			"tweedie_variance_power",
			params.get(HasTweedieVariancePower.TWEEDIE_VARIANCE_POWER)
		);

		Objective objective = params.get(XGBoostLearningTaskParams.OBJECTIVE);

		switch (objective) {
			case REG_SQUAREDERROR:
				xgboostStyleParamMap.put("objective", "reg:squarederror");
				break;
			case REG_SQUAREDLOGERROR:
				xgboostStyleParamMap.put("objective", "reg:squaredlogerror");
				break;
			case REG_LOGISTIC:
				xgboostStyleParamMap.put("objective", "reg:logistic");
				break;
			case REG_PSEUDOHUBERERROR:
				xgboostStyleParamMap.put("objective", "reg:pseudohubererror");
				break;
			case BINARY_LOGISTIC:
				xgboostStyleParamMap.put("objective", "binary:logistic");
				break;
			case BINARY_LOGITRAW:
				xgboostStyleParamMap.put("objective", "binary:logitraw");
				break;
			case BINARY_HINGE:
				xgboostStyleParamMap.put("objective", "binary:hinge");
				break;
			case COUNT_POISSON:
				xgboostStyleParamMap.put("objective", "count:poisson");
				break;
			case SURVIVAL_COX:
				xgboostStyleParamMap.put("objective", "survival:cox");
				break;
			case SURVIVAL_AFT_LOSS_DISTRIBUTION:
				xgboostStyleParamMap.put("objective", "aft_loss_distribution");
				break;
			case MULTI_SOFTMAX:
				xgboostStyleParamMap.put("objective", "multi:softmax");
				break;
			case MULTI_SOFTPROB:
				xgboostStyleParamMap.put("objective", "multi:softprob");
				break;
			case RANK_NDCG:
				xgboostStyleParamMap.put("objective", "rank:ndcg");
				break;
			case RANK_MAP:
				xgboostStyleParamMap.put("objective", "rank:map");
				break;
			case REG_GAMMA:
				xgboostStyleParamMap.put("objective", "reg:gamma");
				break;
			case REG_TWEEDIE:
				xgboostStyleParamMap.put("objective", "reg:tweedie");
				break;
			default:
				throw new IllegalArgumentException(String.format("Illegal objective: %s", objective));
		}

		xgboostStyleParamMap.put(
			"base_score",
			params.get(XGBoostLearningTaskParams.BASE_SCORE)
		);

		return xgboostStyleParamMap;
	}

}
