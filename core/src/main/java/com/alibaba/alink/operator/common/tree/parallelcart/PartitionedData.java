package com.alibaba.alink.operator.common.tree.parallelcart;

import java.io.Serializable;

public class PartitionedData implements Serializable, Comparable <PartitionedData> {
	int index;
	byte[] features;    //byte for better locality
	double label;

	int group;
	int nodeId;

	public PartitionedData() {
	}

	@Override
	public int compareTo(PartitionedData o) {
		if (label > o.label) {
			return -1;
		}
		if (label < o.label) {
			return 1;
		}
		return 0;
	}

	public byte[] getFeatures() {
		return features;
	}

	public void setFeatures(byte[] features) {
		this.features = features;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public double getLabel() { return label; }

	public void setLabel(double label) { this.label = label; }

	public int getGroup() {
		return group;
	}

	public void setGroup(int group) {
		this.group = group;
	}
}
