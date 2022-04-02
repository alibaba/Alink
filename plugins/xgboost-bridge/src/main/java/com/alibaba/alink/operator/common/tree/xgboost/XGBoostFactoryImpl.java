package com.alibaba.alink.operator.common.tree.xgboost;

public class XGBoostFactoryImpl implements XGBoostFactory {

	@Override
	public XGBoost create() {
		return new XGBoostImpl();
	}
}
