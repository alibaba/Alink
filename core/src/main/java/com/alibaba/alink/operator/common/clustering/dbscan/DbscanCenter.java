package com.alibaba.alink.operator.common.clustering.dbscan;

import org.apache.flink.types.Row;

import java.io.Serializable;

public class DbscanCenter<T> implements Serializable {

	private static final long serialVersionUID = -7360209191438865256L;
	private Row groupColNames;
	private long clusterId;
	private long count;
	private T value;

	public DbscanCenter(Row groupColNames, long clusterId, long count,
						T value) {
		this.groupColNames = groupColNames;
		this.clusterId = clusterId;
		this.count = count;
		this.value = value;
	}

	public Row getGroupColNames() {
		return groupColNames;
	}

	public long getClusterId() {
		return clusterId;
	}

	public long getCount() {
		return count;
	}

	public T getValue() {
		return value;
	}
}