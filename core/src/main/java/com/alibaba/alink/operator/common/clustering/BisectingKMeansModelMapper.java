package com.alibaba.alink.operator.common.clustering;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.clustering.BisectingKMeansTrainBatchOp;
import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelData.ClusterSummary;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansUtil;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class BisectingKMeansModelMapper extends RichModelMapper {

	private static final long serialVersionUID = 1293356859097519385L;
	private BisectingKMeansModelData modelData;
	private Tree tree;
	private int vectorColIdx;

	public BisectingKMeansModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected TypeInformation<?> initPredResultColType(TableSchema modelSchema) {
		return Types.LONG;
	}

	private double[] computeProbability(long nodeId, List <Long> otherNodeIds) {
		double[] distances = new double[otherNodeIds.size()];
		for (int i = 0; i < distances.length; i++) {
			distances[i] = nodeDistanceInTree(nodeId, otherNodeIds.get(i));
		}
		return KMeansUtil.getProbArrayFromDistanceArray(distances);
	}

	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		return predictResultDetail(selection).f0;
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		Vector x = VectorUtil.getVector(selection.get(vectorColIdx));
		if (x.size() != this.modelData.vectorSize) {
			throw new AkIllegalDataException(
				"Dim of predict data not equal to vectorSize of training data: " + this.modelData.vectorSize);
		}
		ContinuousDistance distance = this.modelData.distanceType.getFastDistance();
		Tuple2 <Long, Long> clusterIdAndTreeNodeId = this.tree.predict(x, distance);
		double[] prob = computeProbability(clusterIdAndTreeNodeId.f1, tree.treeNodeIds);
		return Tuple2.of(clusterIdAndTreeNodeId.f0, VectorUtil.serialize(new DenseVector(prob)));
	}

	private int level(long node) {
		int l = 0;
		while (node > 1) {
			node /= 2;
			l++;
		}
		return l;
	}

	private double nodeDistanceInTree(long node1, long node2) {
		int level1 = level(node1);
		int level2 = level(node2);
		int d = 0;
		if (level1 > level2) {
			while (level1 > level2) {
				// parent
				node1 = node1 / 2;
				level1 = level(node1);
				d++;
			}
		} else if (level2 > level1) {
			while (level2 > level1) {
				// parent
				node2 = node2 / 2;
				level2 = level(node2);
				d++;
			}
		}

		while (node1 != node2) {
			node1 = node1 / 2;
			node2 = node2 / 2;
			d += 2;
		}
		return (double) d;
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.modelData = new BisectingKMeansModelDataConverter().load(modelRows);

		this.vectorColIdx = TableUtil.findColIndexWithAssert(super.getDataSchema().getFieldNames(),
			this.modelData.vectorColName);

		this.tree = new Tree(modelData.summaries);
	}

	private static class TreeNode {
		/**
		 * Id of cluster. All ids are consecutive and starts from zero.
		 */
		long clusterId;

		/**
		 * Id of the cluster in the tree. The ids are indexed as positions in a full binary tree.
		 */
		long treeNodeId;

		DenseVector center;
		Tuple2 <DenseVector, Double> middlePlane; // middle plane of its two childs.

		TreeNode leftChild;
		TreeNode rightChild;

		public TreeNode(long clusterIdInTree, DenseVector center) {
			this.treeNodeId = clusterIdInTree;
			this.center = center;
			leftChild = null;
			rightChild = null;
			clusterId = -1;
		}

		public boolean isLeaf() {
			return leftChild == null && rightChild == null;
		}

		void constructMiddlePlane() {
			if (isLeaf()) {
				return;
			}
			DenseVector v = rightChild.center.clone();
			DenseVector l = leftChild.center.clone();
			DenseVector m = v.clone();
			BLAS.axpy(1., l, m);
			BLAS.axpy(-1., l, v);
			BLAS.scal(0.5, m);
			double length = BLAS.dot(m, v);
			middlePlane = Tuple2.of(v, length);
			if (leftChild != null) {
				leftChild.constructMiddlePlane();
			}
			if (rightChild != null) {
				rightChild.constructMiddlePlane();
			}
		}

		/**
		 * Find the cluster the sample belongs to
		 *
		 * @param sample   Sample vector.
		 * @param distance Distance
		 * @return The cluster id and tree node id
		 */
		public Tuple2 <Long, Long> predict(Vector sample, ContinuousDistance distance) {
			if (isLeaf()) {
				return Tuple2.of(clusterId, treeNodeId);
			}
			TreeNode child;
			if (distance instanceof EuclideanDistance) {
				double d = MatVecOp.dot(sample, middlePlane.f0);
				child = d < middlePlane.f1 ? leftChild : rightChild;
			} else {
				long whichChild = BisectingKMeansTrainBatchOp.getClosestNode(0, leftChild.center, 1, rightChild.center,
					sample, distance);
				child = whichChild == 0L ? leftChild : rightChild;
			}
			return child.predict(sample, distance);
		}
	}

	public static class Tree {
		TreeNode root;

		List <Long> treeNodeIds;

		public Tree(Map <Long, ClusterSummary> summaries) {
			root = new TreeNode(BisectingKMeansTrainBatchOp.ROOT_INDEX,
				summaries.get(BisectingKMeansTrainBatchOp.ROOT_INDEX).center);
			Queue <TreeNode> queue = new ArrayDeque <>();
			queue.add(root);

			while (!queue.isEmpty()) {
				TreeNode top = queue.poll();
				long leftChildIndex = BisectingKMeansTrainBatchOp.leftChildIndex(top.treeNodeId);
				long rightChildIndex = BisectingKMeansTrainBatchOp.rightChildIndex(top.treeNodeId);
				if (summaries.containsKey(leftChildIndex)) {
					TreeNode child = new TreeNode(leftChildIndex, summaries.get(leftChildIndex).center);
					top.leftChild = child;
					queue.add(child);
				}
				if (summaries.containsKey(rightChildIndex)) {
					TreeNode child = new TreeNode(rightChildIndex, summaries.get(rightChildIndex).center);
					top.rightChild = child;
					queue.add(child);
				}
			}

			root.constructMiddlePlane();
			assignClusterId();
		}

		private void assignClusterId() {
			Queue <TreeNode> queue = new ArrayDeque <>();
			queue.add(root);
			long id = 0L;
			treeNodeIds = new ArrayList <>();

			while (!queue.isEmpty()) {
				TreeNode top = queue.poll();
				if (top.isLeaf()) {
					top.clusterId = id;
					treeNodeIds.add(top.treeNodeId);
					id++;
				} else {
					if (top.leftChild != null) {
						queue.add(top.leftChild);
					}
					if (top.rightChild != null) {
						queue.add(top.rightChild);
					}
				}
			}
		}

		public Tuple2 <Long, Long> predict(Vector x, ContinuousDistance distance) {
			return root.predict(x, distance);
		}

		public List <Long> getTreeNodeIds() {
			return treeNodeIds;
		}
	}
}
