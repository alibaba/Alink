package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * ClusteringModelSummary is the base summary class of clustering model.
 */
public abstract class ClusteringModelInfo implements Serializable {
    public abstract int getClusterNumber();

    public abstract DenseVector getClusterCenter(long clusterId);

    protected String clusterCenterToString(int maxRowNumber, String modelName, String... otherInfo){
        StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline(modelName + "Info", '-'));
        sbd.append(modelName + " clustering with ").append(getClusterNumber()).append(" clusters.");
        for(String s : otherInfo) {
            sbd.append(s);
        }
        sbd.append("\n");
        sbd.append(PrettyDisplayUtils.displayHeadline("ClusterCenters", '='));
        Map<Integer, DenseVector> list = new HashMap<>();
        for(int i = 0; i < getClusterNumber(); i++){
            list.put(i, getClusterCenter((long)i));
        }
        sbd.append(PrettyDisplayUtils.displayMap(list, maxRowNumber / 2, true)).append("\n");
        return sbd.toString();
    }
}
