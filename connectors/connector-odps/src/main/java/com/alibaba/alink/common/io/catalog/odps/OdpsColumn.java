package com.alibaba.alink.common.io.catalog.odps;

import com.aliyun.odps.OdpsType;

import java.io.Serializable;

public class OdpsColumn implements Serializable {
	private static final long serialVersionUID = 3385061492593160182L;
	private final String name;
	private final OdpsType type;
	private final boolean isPartition;

	public OdpsColumn(String name, OdpsType type) {
		this(name, type, false);
	}

	public OdpsColumn(String name, OdpsType type, boolean isPartition) {
		this.name = name;
		this.type = type;
		this.isPartition = isPartition;
	}

	public String getName() {
		return this.name;
	}

	public OdpsType getType() {
		return this.type;
	}

	public boolean isPartition() {
		return this.isPartition;
	}

	public String toString() {
		return "ODPSColumn{name='" + this.name + '\'' + ", type=" + this.type + ", isPartition=" + this.isPartition + '}';
	}
}