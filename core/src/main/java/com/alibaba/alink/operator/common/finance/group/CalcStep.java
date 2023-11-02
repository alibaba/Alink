package com.alibaba.alink.operator.common.finance.group;

public enum CalcStep {
	LR_TRAIN_LEFT,
	LR_PRED_LEFT,
	LR_TRAIN_RIGHT,
	LR_PRED_RIGHT,
	LR_PRED,
	EVAL,
	SPLIT,
	Terminate
}
