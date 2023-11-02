package com.alibaba.alink.operator.common.finance.group;

import com.alibaba.alink.common.utils.JsonConverter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GroupNode implements Serializable {
	// current split value
	public SplitInfo split;
	// candidate set
	public List <SplitInfo> splitInfos;
	public EvalData evalData;
	public GroupNode leftNode;
	public GroupNode rightNode;
	public List <Integer> selectedDataIndices;
	public int nodeId;
	public int depth;
	public boolean isLeaf;
	public boolean isCalculated;

	public double gain;

	public String toJson() {
		return JsonConverter.toJson(this.cloneWithOneDepth());
	}

	public GroupNode clone() {
		GroupNode node = new GroupNode();
		if (split != null) {
			node.split = split.clone();
		}
		if (splitInfos != null) {
			node.splitInfos = new ArrayList <>();
			for (SplitInfo info : splitInfos) {
				node.splitInfos.add(info.clone());
			}
		}
		if (evalData != null) {
			node.evalData = evalData.clone();
		}
		if (leftNode != null) {
			node.leftNode = leftNode.clone();
		}
		if (rightNode != null) {
			node.rightNode = rightNode.clone();
		}
		node.selectedDataIndices = new ArrayList <>();
		node.selectedDataIndices.addAll(selectedDataIndices);
		node.nodeId = nodeId;
		node.depth = depth;
		node.isLeaf = isLeaf;
		node.isCalculated = isCalculated;
		node.gain = gain;
		return node;
	}

	public GroupNode cloneWithOneDepth() {
		GroupNode node = new GroupNode();
		if (split != null) {
			node.split = split.clone();
		}
		if (evalData != null) {
			node.evalData = evalData.clone();
		}
		if (leftNode != null) {
			node.leftNode = new GroupNode();
			node.leftNode.nodeId = leftNode.nodeId;
		}
		if (rightNode != null) {
			node.rightNode = new GroupNode();
			node.rightNode.nodeId = rightNode.nodeId;
		}
		node.nodeId = nodeId;
		node.depth = depth;
		node.isLeaf = isLeaf;
		node.isCalculated = isCalculated;
		node.gain = gain;
		return node;
	}
}
