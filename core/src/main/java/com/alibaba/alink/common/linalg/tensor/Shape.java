package com.alibaba.alink.common.linalg.tensor;

public final class Shape {
	private final org.tensorflow.ndarray.Shape internal;

	public Shape(long... dimensions) {
		this.internal = org.tensorflow.ndarray.Shape.of(dimensions);
	}

	public long[] asArray() {
		return internal.asArray();
	}

	public long size() {
		long[] sArray = asArray();
		long ret = 1;

		for (long s : sArray) {
			ret *= s;
		}

		return ret;
	}

	org.tensorflow.ndarray.Shape toNdArrayShape() {
		return internal;
	}

	static Shape fromNdArrayShape(org.tensorflow.ndarray.Shape ndArrayShape) {
		return new Shape(ndArrayShape.asArray());
	}
}
