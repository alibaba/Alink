package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.nlp.HasIdCol;
import com.alibaba.alink.params.shared.clustering.HasClusteringDistanceType;
import com.alibaba.alink.params.shared.clustering.HasEpsilonDefaultAs00001;
import com.alibaba.alink.params.shared.clustering.HasKDefaultAs2;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasGroupCols;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs10;

public interface GroupKMeansParams<T> extends
	HasFeatureCols <T>,
	HasClusteringDistanceType <T>,
	HasIdCol <T>,
	HasPredictionCol <T>,
	HasGroupCols <T>,
	HasKDefaultAs2 <T>,
	HasMaxIterDefaultAs10 <T>,
	HasEpsilonDefaultAs00001 <T> {
}
