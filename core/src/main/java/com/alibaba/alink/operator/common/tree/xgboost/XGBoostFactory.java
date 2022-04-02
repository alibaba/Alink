package com.alibaba.alink.operator.common.tree.xgboost;

import java.io.Serializable;

public interface XGBoostFactory extends Serializable {
	XGBoost create();
}
