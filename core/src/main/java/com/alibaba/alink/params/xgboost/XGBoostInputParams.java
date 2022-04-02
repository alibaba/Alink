package com.alibaba.alink.params.xgboost;

import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface XGBoostInputParams<T> extends
	HasFeatureColsDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T>,
	HasLabelCol <T> {
}
