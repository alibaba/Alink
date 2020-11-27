package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Slice;

import java.io.Serializable;

class NodeInfoPair implements Serializable {
	private static final long serialVersionUID = 2214467783345148214L;

	NodeInfo small;

	NodeInfo big;

	static class NodeInfo {
		Node node;

		Slice slice;

		Slice oob;

		int depth;

		int[] baggingFeatures;

		NodeInfo(
			Node node,
			Slice slice,
			Slice oob,
			int depth,
			int[] baggingFeatures) {
			this.node = node;
			this.slice = slice;
			this.oob = oob;
			this.depth = depth;
			this.baggingFeatures = baggingFeatures;
		}

		void shrinkageMemory() {
			baggingFeatures = null;
		}
	}

	NodeInfoPair(NodeInfo small, NodeInfo big) {
		this.small = small;
		this.big = big;
	}
}
