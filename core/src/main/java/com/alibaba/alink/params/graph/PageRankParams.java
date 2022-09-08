package com.alibaba.alink.params.graph;

import com.alibaba.alink.params.nlp.HasDampingFactor;
import com.alibaba.alink.params.shared.clustering.HasEpsilonDefaultAs00001;

public interface PageRankParams<T> extends GraphVertexCols <T>,
	HasMaxIterDefaultAs50 <T>,
	HasEdgeWeightCol <T>,
	HasEpsilonDefaultAs00001 <T>,
	HasDampingFactor <T> {
}
