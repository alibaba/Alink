package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.parallelcart.data.Data;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.RankingLossFunc;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.UnaryLossFuncWithPrior;

import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.ExecutorService;

public class BoostingObjs {
	public Params params;
	public Data data;
	public UnaryLossFuncWithPrior loss;
	public RankingLossFunc rankingLoss;
	public double prior;
	public Random instanceRandomizer;
	public Random featureRandomizer;
	public int numBaggingInstances;
	public int[] indices;
	public BitSet baggingFlags;
	public int numBaggingFeatures;
	public int[] featureIndices;
	public boolean inWeakLearner;
	public int numBoosting;
	public double[] pred;
	public ExecutorService executorService;
}
