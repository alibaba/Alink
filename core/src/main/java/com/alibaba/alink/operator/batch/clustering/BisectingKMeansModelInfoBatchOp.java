package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelData;
import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelDataConverter;
import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelMapper.Tree;
import com.alibaba.alink.operator.common.clustering.ClusteringModelInfo;

import java.util.List;

/**
 * BisectingKMeansModelInfoBatchOp can be linked to the output of BisectingKMeansTrainBatchOp to summary the
 * BisectingKMeans model.
 */
public class BisectingKMeansModelInfoBatchOp
	extends
	ExtractModelInfoBatchOp <BisectingKMeansModelInfoBatchOp.BisectingKMeansModelInfo,
		BisectingKMeansModelInfoBatchOp> {
	private static final long serialVersionUID = 1735133462550836751L;

	public BisectingKMeansModelInfoBatchOp() {
		this(null);
	}

	public BisectingKMeansModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public BisectingKMeansModelInfo createModelInfo(List <Row> rows) {
		return new BisectingKMeansModelInfo(new BisectingKMeansModelDataConverter().load(rows));
	}

	/**
	 * Summary of BisectingKMeansModel.
	 */
	public static class BisectingKMeansModelInfo extends ClusteringModelInfo {
		private static final long serialVersionUID = -6001342718616929666L;
		private Tree modelTree;
		private BisectingKMeansModelData modelData;
		private int totalSamples = 0;
		private int vectorSize;

		public BisectingKMeansModelInfo(BisectingKMeansModelData modelData) {
			this.modelData = modelData;
			modelTree = new Tree(modelData.summaries);
			for (BisectingKMeansModelData.ClusterSummary summary : modelData.summaries.values()) {
				if (summary.clusterId == 1L) {
					totalSamples = (int) summary.size;
				}
				vectorSize = summary.center.size();
			}
		}

		@Override
		public int getClusterNumber() {
			return modelTree.getTreeNodeIds().size();
		}

		@Override
		public DenseVector getClusterCenter(long clusterId) {
			return modelData.summaries.get(modelTree.getTreeNodeIds().get((int) clusterId)).center;
		}

		@Override
		public String toString() {
			return clusterCenterToString(20, "BisectingKMeans",
				" Clustering on " + totalSamples + " samples of " + vectorSize + " dimension based on " +
					modelData.distanceType.toString() + ".");
		}
	}
}
