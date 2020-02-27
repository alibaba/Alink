package com.alibaba.alink.operator.common.feature.quantile;

import org.apache.flink.api.common.typeinfo.TypeInfo;

import com.alibaba.alink.operator.common.dataproc.SortUtils;

import java.io.Serializable;

@TypeInfo(PairComparableTypeInfoFactory.class)
public final class PairComparable
	implements Comparable<PairComparable>, Serializable {
	public Integer first;
	public Number second;

	@Override
	public int compareTo(PairComparable o) {
		int f = this.first.compareTo(o.first);

		return f == 0 ? SortUtils.OBJECT_COMPARATOR.compare(this.second, o.second) : f;
	}
}
