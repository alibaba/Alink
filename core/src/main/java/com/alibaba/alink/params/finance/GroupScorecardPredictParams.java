package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface GroupScorecardPredictParams<T> extends
	ModelMapperParams <T>,
	HasPredictionScoreCol <T>,
	HasPredictionDetailCol <T>,
	HasCalculateScorePerFeature <T>,
	HasReservedColsDefaultAsNull <T> {
	
	ParamInfo <String[]> PREDICTION_SCORE_PER_FEATURE_COLS = ParamInfoFactory
		.createParamInfo("predictionScorePerFeatureCols", String[].class)
		.setDescription("prediction score per feature cols")
		.setHasDefaultValue(null)
		.build();

	default String[] getPredictionScorePerFeatureCols() {
		return get(PREDICTION_SCORE_PER_FEATURE_COLS);
	}

	default T setPredictionScorePerFeatureCols(String[] colName) {
		return set(PREDICTION_SCORE_PER_FEATURE_COLS, colName);
	}
}
