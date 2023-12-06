package com.alibaba.alink.common.insights;

import java.io.Serializable;

public enum InsightType implements Serializable {
	Attribution,
	OutstandingNo1,
	OutstandingTop2,
	OutstandingLast,
	Evenness,
	ChangePoint,
	Outlier,
	Seasonality,
	Trend,
	Clustering2D,
	Correlation,
	CrossMeasureCorrelation,
	BasicStat,
	Distribution
}
