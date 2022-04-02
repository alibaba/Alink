package com.alibaba.alink.params.xgboost;

public interface XGBoostTreeBoosterParams<T> extends
	HasEta <T>,
	HasGamma <T>,
	HasMaxDepth <T>,
	HasMinChildWeight <T>,
	HasMaxDeltaStep <T>,
	HasSubSample <T>,
	HasSamplingMethod <T>,
	HasColSampleByTree <T>,
	HasColSampleByLevel <T>,
	HasColSampleByNode <T>,
	HasLambda <T>,
	HasAlpha <T>,
	HasTreeMethod <T>,
	HasSketchEps <T>,
	HasScalePosWeight <T>,
	HasUpdater <T>,
	HasRefreshLeaf <T>,
	HasProcessType <T>,
	HasGrowPolicy <T>,
	HasMaxLeaves <T>,
	HasMaxBin <T>,
	HasMonotoneConstraints <T>,
	HasInteractionConstraints <T>,
	HasSinglePrecisionHistogram <T>,
	HasNumClass <T> {
}
