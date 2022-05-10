package com.alibaba.alink.params.xgboost;

import com.alibaba.alink.params.io.shared.HasPluginVersion;
import com.alibaba.alink.params.regression.RegPredictParams;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

public interface XGBoostRegPredictParams<T> extends
	RegPredictParams <T>,
	HasXGBoostPluginVersion <T> {
}
