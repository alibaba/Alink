package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.nlp.HasIdCol;
import com.alibaba.alink.params.shared.clustering.HasClusteringDistanceType;
import com.alibaba.alink.params.shared.clustering.HasDistanceThreshold;
import com.alibaba.alink.params.shared.clustering.HasKDefaultAs2;
import com.alibaba.alink.params.shared.clustering.HasLinkage;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

public interface AgnesParams<T> extends
	HasClusteringDistanceType <T>,
	HasLinkage <T>,
	HasIdCol <T>,
	HasVectorCol <T>,
	HasPredictionCol <T>,
	HasKDefaultAs2 <T>,
	HasDistanceThreshold <T> {
}
