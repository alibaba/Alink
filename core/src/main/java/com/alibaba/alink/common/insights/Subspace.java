package com.alibaba.alink.common.insights;

import java.io.Serializable;

public class Subspace implements Serializable {

	public final String colName;

	public Object value;

	public Subspace(String colName, Object value) {
		this.colName = colName;
		this.value = value;
	}

	/**
	 * for whole table.
	 */
	public Subspace() {
		this.colName = null;
	}

	@Override
	public String toString() {
		return colName == null ? "" : colName + "=" + value;
	}

	public String strInDescription() {
		return String.format("当%s=%s时 ", this.colName, this.value);
	}
}
