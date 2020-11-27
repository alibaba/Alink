package com.alibaba.alink.operator.common.tree.parallelcart.loss;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public class LossUtils {

	public static final ParamInfo <LossType> LOSS_TYPE = ParamInfoFactory
		.createParamInfo("lossType", LossType.class)
		.build();

	public static boolean isRanking(LossType lossType) {
		switch (lossType) {
			case GBRANK:
			case LAMBDA_DCG:
			case LAMBDA_NDCG:
				return true;
			default:
				return false;
		}
	}

	public static boolean isClassification(LossType lossType) {
		return lossType == LossType.LOG_LOSS;
	}

	public static boolean isRegression(LossType lossType) {
		return lossType == LossType.LEASE_SQUARE;
	}

	public static boolean useInstanceCount(LossType lossType) {
		switch (lossType) {
			case LAMBDA_DCG:
			case LAMBDA_NDCG:
				return true;
			case LOG_LOSS:
			case LEASE_SQUARE:
			case GBRANK:
				return false;
		}

		throw new UnsupportedOperationException(String.format("Unsupported now. type: %s", lossType));
	}

	public static int lossTypeToInt(LossType lossType) {
		switch (lossType) {
			case LEASE_SQUARE:
				return 0;
			case LOG_LOSS:
				return 1;
			case LAMBDA_NDCG:
				return 2;
			case LAMBDA_DCG:
				return 3;
			case GBRANK:
				return 4;
			default:
				throw new IllegalArgumentException("Unsupported now. lossType: " + lossType);
		}
	}
}
