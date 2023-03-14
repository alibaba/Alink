package com.alibaba.alink.operator.common.statistics.statistics;

import java.util.HashSet;

public class DistinctValueIterator<T> {
	public HashSet <T> mapFreq = null;

	public DistinctValueIterator() {
		this.mapFreq = new HashSet <>();
	}

	public void visit(T val) {
		mapFreq.add(val);
	}
}
