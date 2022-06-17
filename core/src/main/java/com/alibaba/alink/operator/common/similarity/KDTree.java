package com.alibaba.alink.operator.common.similarity;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;

public class KDTree implements Serializable {
	private static final long serialVersionUID = 2164916702239332666L;
	private int vectorSize;
	FastDistanceVectorData[] samples;
	FastDistance distance;
	private final int LEAF_SIZE = 40;
	private int nLevel;
	private int nodeNum;

	public TreeNode getRoot() {
		return root;
	}

	public void setRoot(TreeNode root) {
		this.root = root;
	}

	private TreeNode root;

	public KDTree(FastDistanceVectorData[] samples, int vectorSize, FastDistance distance) {
		this.vectorSize = vectorSize;
		this.samples = samples;
		this.distance = distance;
	}

	public void buildTree() {
		nLevel = 1 + (int) Math.max(0, (Math.log((samples.length - 1) / LEAF_SIZE) / Math.log(2)));
		nodeNum = (int) Math.pow(2, nLevel) - 1;
		this.root = recursiveBuild(0, 0, samples.length);
	}

	public List <FastDistanceVectorData> rangeSearch(double epsilon, FastDistanceVectorData sample) {
		List <FastDistanceVectorData> list = new ArrayList <>();
		Stack <TreeNode> stack = new Stack <>();
		stack.push(root);
		while (!stack.empty()) {
			TreeNode node = stack.pop();
			if (null == node) {
				continue;
			}
			Tuple2 <Double, Double> tuple = minMaxDistance(node, sample.getVector());
			double min = tuple.f0, max = tuple.f1;
			if (distance instanceof EuclideanDistance && epsilon >= max) {
				list.addAll(Arrays.asList(samples).subList(node.startIndex, node.endIndex));
			} else if (distance instanceof EuclideanDistance && Math.abs(epsilon - min) > 1e-12 && epsilon < min) {
			} else {
				if (node.isLeaf) {
					for (int i = node.startIndex; i < node.endIndex; i++) {
						if (distance.calc(sample, samples[i]).get(0, 0) <= epsilon) {
							list.add(samples[node.nodeIndex]);
						}
					}
				} else {
					if (distance.calc(sample, samples[node.nodeIndex]).get(0, 0) <= epsilon) {
						list.add(samples[node.nodeIndex]);
					}
					stack.add(node.left);
					stack.add(node.right);
				}
			}
		}
		return list;
	}

	public Tuple2 <Double, Row>[] getTopN(int topN, FastDistanceVectorData sample) {
		Queue <Tuple2 <Double, Row>> queue = new PriorityQueue <>(topN,
			new Comparator <Tuple2 <Double, Row>>() {
				@Override
				public int compare(Tuple2 <Double, Row> s1, Tuple2 <Double, Row> s2) {
					int res = s2.f0.compareTo(s1.f0);
					if (res == 0) {
						return s2.f1.equals(s1.f1) ? 0 : 1;
					}
					return res;
				}
			});
		Stack <TreeNode> nodeStack = new Stack <>();
		Stack <Double> distanceStack = new Stack <>();
		nodeStack.push(root);
		distanceStack.push(distance.calc(sample, samples[root.nodeIndex]).get(0, 0));
		while (!nodeStack.empty()) {
			TreeNode node = nodeStack.pop();
			double nodeDistance = distanceStack.pop();
			if (null == node) {
				continue;
			}
			if (queue.size() < topN) {
				if (node.isLeaf) {
					// traversal search of leaf nodes
					for (int i = node.startIndex; i < node.endIndex; i++) {
						double d = distance.calc(sample, samples[i]).get(0, 0);
						if (queue.size() < topN) {
							queue.add(Tuple2.of(d, samples[i].getRows()[0]));
						} else if (d < queue.peek().f0) {
							Tuple2 <Double, Row> tuple = queue.poll();
							tuple.f0 = d;
							tuple.f1 = samples[i].getRows()[0];
							queue.add(tuple);
						}
					}
				} else {
					queue.add(Tuple2.of(nodeDistance, samples[node.nodeIndex].getRows()[0]));
				}
			} else {
				Tuple2 <Double, Row> peek = queue.peek();
				double min = minMaxDistance(node, sample.getVector()).f0;
				if (distance instanceof EuclideanDistance && Math.abs(peek.f0 - min) > 1e-12 && peek.f0 < min) {
					continue;
				}
				if (nodeDistance < peek.f0) {
					queue.poll();
					peek.f0 = nodeDistance;
					peek.f1 = samples[node.nodeIndex].getRows()[0];
					queue.add(peek);
				}
				if (node.isLeaf) {
					// traversal search of leaf nodes
					for (int i = node.startIndex + 1; i < node.endIndex; i++) {
						double d = distance.calc(sample, samples[i]).get(0, 0);
						if (d < queue.peek().f0) {
							Tuple2 <Double, Row> tuple = queue.poll();
							tuple.f0 = d;
							tuple.f1 = samples[i].getRows()[0];
							queue.add(tuple);
						}
					}
				}
			}
			// compare left and right children's average distance, closer cluster do binary search first
			double leftDistance = node.left == null ? Double.MAX_VALUE : distance.calc(sample,
				samples[node.left.nodeIndex]).get(0, 0);
			double rightDistance = node.right == null ? Double.MAX_VALUE : distance.calc(sample,
				samples[node.right.nodeIndex]).get(0, 0);
			if (leftDistance >= rightDistance) {
				nodeStack.add(node.left);
				nodeStack.add(node.right);
				distanceStack.add(leftDistance);
				distanceStack.add(rightDistance);
			} else {
				nodeStack.add(node.right);
				nodeStack.add(node.left);
				distanceStack.add(rightDistance);
				distanceStack.add(leftDistance);
			}
		}
		Tuple2 <Double, Row>[] res = new Tuple2[queue.size()];
		int c = res.length - 1;
		while (!queue.isEmpty()) {
			res[c--] = queue.poll();
		}
		return res;

	}

	private Tuple2 <Double, Double> minMaxDistance(TreeNode node, Vector sample) {
		double min = 0, max = 0;
		if (distance instanceof EuclideanDistance) {
			for (int i = 0; i < vectorSize; i++) {
				double value = sample.get(i);
				if (value < node.downThre[i]) {
					min += Math.pow(value - node.downThre[i], 2);
					max += Math.pow(value - node.upThre[i], 2);
				} else if (value > node.upThre[i]) {
					min += Math.pow(value - node.upThre[i], 2);
					max += Math.pow(value - node.downThre[i], 2);
				} else {
					max += Math.max(Math.pow(value - node.downThre[i], 2), Math.pow(value - node.upThre[i], 2));
				}
			}
		}
		return Tuple2.of(Math.sqrt(min), Math.sqrt(max));
	}

	TreeNode recursiveBuild(int startIndex, int endIndex) {
		return recursiveBuild(startIndex, startIndex, endIndex);
	}

	TreeNode recursiveBuild(int thisIndex, int startIndex, int endIndex) {
		if (startIndex >= endIndex) {
			return null;
		}
		TreeNode node = new TreeNode(startIndex, endIndex);
		findBounds(node);
		if (2 * thisIndex + 1 > nodeNum) {
			node.isLeaf = true;
			return node;
		}
		node.splitDim = pickSplitDim(startIndex, endIndex);
		node.nodeIndex = split(startIndex, endIndex, node.splitDim);
		node.left = recursiveBuild(thisIndex * 2 + 1, node.startIndex, node.nodeIndex);
		node.right = recursiveBuild(thisIndex * 2 + 2, node.nodeIndex + 1, node.endIndex);
		return node;
	}

	void findBounds(TreeNode node) {
		double[] downThre = new double[vectorSize], upThre = new double[vectorSize];
		for (int i = 0; i < vectorSize; i++) {
			downThre[i] = Double.MAX_VALUE;
			upThre[i] = Double.NEGATIVE_INFINITY;
			for (int j = node.startIndex; j < node.endIndex; j++) {
				double value = samples[j].getVector().get(i);
				downThre[i] = Math.min(downThre[i], value);
				upThre[i] = Math.max(upThre[i], value);
			}
		}
		node.upThre = upThre;
		node.downThre = downThre;
	}

	int pickSplitDim(int startIndex, int endIndex) {
		double maxInterval = Double.NEGATIVE_INFINITY;
		int dim = -1;
		if (startIndex + 1 == endIndex) {
			dim = -1;
		}
		for (int i = 0; i < vectorSize; i++) {
			double min = Double.MAX_VALUE, max = Double.NEGATIVE_INFINITY;
			for (int j = startIndex; j < endIndex; j++) {
				double value = samples[j].getVector().get(i);
				min = Math.min(min, value);
				max = Math.max(max, value);
			}
			if (max - min > maxInterval) {
				dim = i;
				maxInterval = max - min;
			}
		}
		return dim;
	}

	int split(int startIndex, int endIndex, int splitDim) {
		endIndex -= 1;
		int midIndex = startIndex + (endIndex - startIndex) / 2;
		while (true) {
			double key = samples[startIndex].getVector().get(splitDim);
			FastDistanceVectorData tuple = samples[startIndex];
			int left = startIndex, right = endIndex;
			while (left < right) {
				while (left < right && samples[right].getVector().get(splitDim) >= key) {
					right--;
				}
				if (left < right) {
					samples[left] = samples[right];
					left++;
				}
				while (left < right && samples[left].getVector().get(splitDim) <= key) {
					left++;
				}
				if (left < right) {
					samples[right] = samples[left];
					right--;
				}
			}
			samples[left] = tuple;
			if (left == midIndex) {
				return midIndex;
			} else if (left < midIndex) {
				startIndex = left + 1;
			} else {
				endIndex = left - 1;
			}
		}
	}

	public static class TreeNode implements Serializable {
		private static final long serialVersionUID = 2059317701009040907L;
		public int nodeIndex, startIndex, endIndex;
		public int splitDim;
		public TreeNode left, right;
		public double[] downThre, upThre;
		boolean isLeaf = false;

		public TreeNode(int startIndex, int endIndex) {
			this.nodeIndex = startIndex;
			this.startIndex = startIndex;
			this.endIndex = endIndex;
			isLeaf = false;
		}

		public TreeNode() {
			this.nodeIndex = 0;
			this.startIndex = 0;
			this.endIndex = 0;
		}

	}
}
