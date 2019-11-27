package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * Cluster evaluation metrics.
 */
public abstract class BaseSimpleClusterMetrics<T extends BaseMetrics<T>> extends BaseMetrics<T> {

    /**
     * A higher Calinski-Harabasz score relates to a model with better defined clusters.
     * <p>
     * CH = Tr(Bk) * (N - k) / Tr(Wk) / (k - 1)
     * <p>
     * Tr(Bk) is SSB, Tr(Wk) is SSW, k is the cluster number, N is the total sample number.
     */
    public static final ParamInfo<Double> CALINSKI_HARABAZ = ParamInfoFactory
        .createParamInfo("calinskiHarabaz", Double.class)
        .setDescription("calinskiHarabaz")
        .setHasDefaultValue(null)
        .build();

    /**
     * The average distance between each point of cluster and the centroid of that cluster, also know as cluster
     * diameter.
     * <p>
     * Compactness_i = sum(||x_i - u_i||)/|C_i|
     * <p>
     * Compactness = avg(Compactness_i)
     * <p>
     * x_i is a sample in cluster i, u_i is the center of the cluster, |C_i| is the sample number in cluster i.
     */
    public static final ParamInfo<Double> COMPACTNESS = ParamInfoFactory
        .createParamInfo("compactness", Double.class)
        .setDescription("compactness")
        .setHasDefaultValue(null)
        .build();

    /**
     * The average distance between centers of one cluster and another cluster.
     * <p>
     * Seperation = sum(sum(||u_i - u_j||)) * 2 / (k^2 - k)
     * <p>
     * u_i is the center of the cluster, k is the cluster number.
     */
    public static final ParamInfo<Double> SEPERATION = ParamInfoFactory
        .createParamInfo("seperation", Double.class)
        .setDescription("seperation")
        .setHasDefaultValue(null)
        .build();

    /**
     * A lower Davies-Bouldin index relates to a model with better separation between the clusters.
     * <p>
     * The index is defined as the average similarity between each cluster C_i and its most similar one C_j.
     * <p>
     * R_i,j = (compactness_i + compactness_j) / d_i,j
     * <p>
     * R_i,j the the similarity between C_i and C_j, d_i,j is the distance between centers i and j.
     * <p>
     * DB = sum(max R_i,j) / k
     */
    public static final ParamInfo<Double> DAVIES_BOULDIN = ParamInfoFactory
        .createParamInfo("daviesBouldin", Double.class)
        .setDescription("daviesBouldin")
        .setHasDefaultValue(null)
        .build();

    public Double getCalinskiHarabaz() {return get(CALINSKI_HARABAZ);}

    public Double getCompactness() {return get(COMPACTNESS);}

    public Double getSeperation() {return get(SEPERATION);}

    public Double getDaviesBouldin() {return get(DAVIES_BOULDIN);}

    public BaseSimpleClusterMetrics(Row row) {
        super(row);
    }

    public BaseSimpleClusterMetrics(Params params) {
        super(params);
    }
}
