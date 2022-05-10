package com.alibaba.alink.params.xgboost;

import com.alibaba.alink.params.io.shared.HasPluginVersion;
import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

public interface XGBoostPredictParams<T> extends
	RichModelMapperParams <T>,
	HasXGBoostPluginVersion <T> {
}
