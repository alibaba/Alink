package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GMMModelInfoBatchOp can be linked to the output of GmmTrainBatchOp to summary the Gaussian Mixture model.
 */
public class GmmModelInfoBatchOp extends ExtractModelInfoBatchOp<GmmModelInfoBatchOp.GmmModelInfo, GmmModelInfoBatchOp> {
    public GmmModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected GmmModelInfo createModelInfo(List<Row> rows) {
        GmmModelData modelData = new GmmModelDataConverter().load(rows);
        return new GmmModelInfo(modelData);
    }

    /**
     * Summary of Gaussian Mixture model.
     */
    public static class GmmModelInfo extends ClusteringModelInfo {

        GmmModelData modelData;

        public GmmModelInfo(GmmModelData modelData) {
            this.modelData = modelData;
        }

        @Override
        public int getClusterNumber() {
            return modelData.data.size();
        }

        @Override
        public DenseVector getClusterCenter(long clusterId) {
            return modelData.data.get((int) clusterId).mean;
        }

        public DenseMatrix getClusterCovarianceMatrix(long clusterId) {
            DenseVector v = modelData.data.get((int) clusterId).cov;
            int n = modelData.data.get((int) clusterId).mean.size();
            return GmmModelData.expandCovarianceMatrix(v, n);
        }

        @Override
        public String toString() {
            StringBuilder sbd = new StringBuilder();
            sbd.append(clusterCenterToString(5, "GMM"));
            sbd.append(PrettyDisplayUtils.displayHeadline("CovarianceMatrix of each clusters", '-'));
            Map<Integer, DenseMatrix> sigmas = new HashMap<>();
            for (int i = 0; i < getClusterNumber(); i++) {
                sigmas.put(i, getClusterCovarianceMatrix(i));
            }
            sbd.append(PrettyDisplayUtils.displayMap(sigmas, 2, true));
            return sbd.toString();
        }
    }
}
