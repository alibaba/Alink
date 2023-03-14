package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.shared.clustering.HasDistanceType;
import com.alibaba.alink.params.shared.clustering.HasEpsilon;
import com.alibaba.alink.params.shared.clustering.HasMinPoints;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasGroupCols;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;

public interface GroupDbscanModelParams<T> extends
	HasDistanceType <T>,
	HasFeatureCols <T>,
	HasPredictionCol <T>,
	HasGroupCols <T>,
	HasMinPoints <T>,
	HasEpsilon <T>,
	HasGroupMaxSamples <T>,
	HasSkip <T> {
}
