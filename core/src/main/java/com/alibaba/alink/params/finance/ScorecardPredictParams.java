package com.alibaba.alink.params.finance;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface ScorecardPredictParams<T> extends
	ModelMapperParams <T>,
	HasPredictionScoreCol <T>,
	HasPredictionDetailCol <T>,
	HasCalculateScorePerFeature <T>,
	HasReservedColsDefaultAsNull <T> {
}
