package com.alibaba.alink.operator.common.modelstream;

import java.io.Serializable;

public class ModelStreamMeta implements Serializable {
	public long count;
	public int numFiles;

	public ModelStreamMeta() {
	}

	public ModelStreamMeta(long count, int numFiles) {
		this.count = count;
		this.numFiles = numFiles;
	}
}
