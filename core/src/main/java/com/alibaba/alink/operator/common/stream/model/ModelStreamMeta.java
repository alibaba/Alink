package com.alibaba.alink.operator.common.stream.model;

public class ModelStreamMeta {
	public long count;
	public int numFiles;

	public ModelStreamMeta() {
	}

	public ModelStreamMeta(long count, int numFiles) {
		this.count = count;
		this.numFiles = numFiles;
	}
}
