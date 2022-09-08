package com.alibaba.alink.params.graph;

import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;

public interface TreeDepthParams<T> extends
	CommonGraphParams <T>,
	HasEdgeWeightCol <T>,
	HasMaxIterDefaultAs100 <T> {
}
