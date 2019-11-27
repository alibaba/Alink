package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.operator.common.tree.Criteria;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TreeParaContainer implements Serializable {
	private List<TreeParas> treeParas = new ArrayList<>();
	private int cursor = -1;

	public void add(TreeParas para) {
		treeParas.add(para);
		cursor = treeParas.size() - 1;
	}

	public List<TreeParas> getTreeParas() {
		return treeParas;
	}

	public int getCursor() {
		return cursor;
	}

	public void setCursor(int cursor) {
		this.cursor = cursor;
	}

	public static class TreeParas implements Serializable {
		private int[] featureIdx;
		private int[] splitPoints;
		private boolean[] isCategorical;
		private ArrayList<ArrayList<Boolean>> childHash;
		private Criteria.MSE[] counters;
		private int treeId;
		private int depth;

		public TreeParas(int d) {
			int nodeSize = (1 << d) - 1;

			featureIdx = new int[nodeSize];
			splitPoints = new int[nodeSize];
			isCategorical = new boolean[nodeSize];
			counters = new Criteria.MSE[nodeSize];

			for (int i = 0; i < nodeSize; ++i) {
				featureIdx[i] = -1;
				splitPoints[i] = -1;
				isCategorical[i] = false;
			}

			treeId = 0;

			this.depth = 1;
		}

		public static int depthSize(int depth) {
			return 1 << (depth - 1);
		}

		public static int parentIdx(int cur) {
			return (cur - 1) / 2;
		}

		public static int childLeftIdx(int cur) {
			return (cur + 1) * 2 - 1;
		}

		public static int childRightIdx(int cur) {
			return (cur + 1) * 2;
		}

		public static int depthStart(int depth) {
			return (1 << (depth - 1)) - 1;
		}

		public static int depth(int cur) {
			return (int) (Math.log(cur + 1.01) / Math.log(2)) + 1;
		}

		public int[] getSplitPoints() {
			return splitPoints;
		}

		public int[] getFeatureIdx() {
			return featureIdx;
		}

		public Criteria.MSE[] getCounters() {
			return counters;
		}

		public int getTreeId() {
			return treeId;
		}

		public void setTreeId(int treeId) {
			this.treeId = treeId;
		}

		public int getDepth() {
			return depth;
		}

		public void setDepth(int depth) {
			this.depth = depth;
		}

		public void incDepth() {
			this.depth++;
		}

		public boolean[] getIsCategorical() {
			return isCategorical;
		}

		public ArrayList<ArrayList<Boolean>> getChildHash() {
			return childHash;
		}

		public void setChildHash(ArrayList<ArrayList<Boolean>> childHash) {
			this.childHash = childHash;
		}
	}
}
