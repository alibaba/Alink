package com.alibaba.alink.operator.batch.graph.storage;

import java.io.Serializable;

public class GraphEdge implements Comparable <GraphEdge>, Serializable {
	private long src, dst;
	private double value;
	private Character srcType, dstType;

	public GraphEdge(long src, long dst, double value) {
		this(src, dst, value, null, null);
	}

	public GraphEdge(long src, long dst, double value, Character srcType, Character dstType) {
		this.src = src;
		this.dst = dst;
		this.value = value;
		this.srcType = srcType;
		this.dstType = dstType;
	}

	public long getSource() {
		return src;
	}

	public long getTarget() {
		return dst;
	}

	public double getValue() {
		return value;
	}

	public Character getDstType() {
		return dstType;
	}

	public Character getSrcType() {
		return srcType;
	}

	@Override
	public int compareTo(GraphEdge o) {
		// homo edge
		if (this.dstType == null || o.dstType == null) {
			return (int) (this.src - o.src == 0? this.dst - o.dst: this.src - o.src);
		} else {
			// hete edge
			return this.src - o.src == 0 ? this.dstType.compareTo(o.dstType) : (int) (this.src - o.src);
		}
	}
}