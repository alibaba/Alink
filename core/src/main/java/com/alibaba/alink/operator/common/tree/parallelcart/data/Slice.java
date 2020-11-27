package com.alibaba.alink.operator.common.tree.parallelcart.data;

public final class Slice {
	public Slice(int start, int end) {
		this.start = start;
		this.end = end;
	}

	public int start;
	public int end;
}
