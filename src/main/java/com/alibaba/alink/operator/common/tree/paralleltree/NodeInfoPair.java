package com.alibaba.alink.operator.common.tree.paralleltree;

import com.alibaba.alink.operator.common.tree.Node;

import java.io.Serializable;

public class NodeInfoPair implements Serializable {
	public Partition small;
	public Partition big;
	// -1 indict that pair is root
	public int parentQueueId;
	public int depth;
	public transient Node parentNode;
	public int[] baggingFeatures;

	public NodeInfoPair initialRoot(int start, int end) {
		this.small = new Partition();
		this.small.initialRoot(start, end);
		return this;
	}

	public boolean root() {
		return parentQueueId == -1;
	}

	public static class Partition {
		public int start;
		public int end;

		void initialRoot(int start, int end) {
			this.start = start;
			this.end = end;
		}
	}
}
