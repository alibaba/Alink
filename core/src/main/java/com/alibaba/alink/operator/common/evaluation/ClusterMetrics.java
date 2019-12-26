package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * Cluster evaluation metrics.
 */
public class ClusterMetrics extends BaseSimpleClusterMetrics<ClusterMetrics> {
    /**
     * Cluster Number.
     */
    public static final ParamInfo<Integer> K = ParamInfoFactory
        .createParamInfo("k", Integer.class)
        .setDescription("cluster number")
        .setRequired()
        .build();

    /**
     * Count.
     */
    public static final ParamInfo<Integer> COUNT = ParamInfoFactory
        .createParamInfo("count", Integer.class)
        .setDescription("count")
        .setRequired()
        .build();

    /**
     * SSW is Tr(Wk), Wk is the within-cluster dispersion matrix.
     * <p>
     * SSW = sum(sum(x - u_i))
     * <p>
     * x is a sample in cluster i, u_i is the center of cluster i.
     */
    public static final ParamInfo<Double> SSW = ParamInfoFactory
        .createParamInfo("SSW", Double.class)
        .setDescription("ssw")
        .setHasDefaultValue(null)
        .build();

    /**
     * SSB is Tr(Bk), BK is the between group dispersion matrix.
     * <p>
     * SSB = sum(n_i * ||u_i - u|| ^ 2)
     * <p>
     * n_i is the sample, number in cluster i, u_i is the center of cluster i, u is the center of the dataset.
     */
    public static final ParamInfo<Double> SSB = ParamInfoFactory
        .createParamInfo("SSB", Double.class)
        .setDescription("ssb")
        .setHasDefaultValue(null)
        .build();

    public static final ParamInfo<String[]> CLUSTER_ARRAY = ParamInfoFactory
        .createParamInfo("clusterArray", String[].class)
        .setDescription("clusterArray")
        .setRequired()
        .build();

    public static final ParamInfo<double[]> COUNT_ARRAY = ParamInfoFactory
        .createParamInfo("countArray", double[].class)
        .setDescription("countArray")
        .setRequired()
        .build();

    /**
     * NMI = MutualInformation * 2/ (entropy(pred) + entropy(actual))
     * MatrixRatio(i,j) = matrix(i,j) / total
     * PredRatio(i) = predict(i) / total
     * PredRatio(j) = actual(j) / total
     * MutualInformation = sum(sum(MatrixRatio(i,j) * log2(MatrixRatio(i,j) / PredRatio(i) / PredRatio(j)))
     * Entropy(pred) = -sum(actual / total * log2(actual / total)
     * Entropy(actual) = -sum(prediction / total * log2(prediction / total)
     */
    public static final ParamInfo<Double> NMI = ParamInfoFactory
        .createParamInfo("NMI", Double.class)
        .setDescription("Normalized Mutual Information")
        .setHasDefaultValue(null)
        .build();

    /**
     * For each cluster, get the max number of one class.
     * Purity = sum(mi) / total
     */
    public static final ParamInfo<Double> PURITY = ParamInfoFactory
        .createParamInfo("purity", Double.class)
        .setDescription("purity")
        .setHasDefaultValue(null)
        .build();

    /**
     * Index = sum(sum(combination(matrix(i,j)))
     * RI = (TP + TN) / (TP + TN + FP + FN)
     * TP + FP = sum(combination(actual))
     * TP + FN = sum(combination(predict))
     * TP = Index
     */
    public static final ParamInfo<Double> RI = ParamInfoFactory
        .createParamInfo("ri", Double.class)
        .setDescription("rand index")
        .setHasDefaultValue(null)
        .build();

    /**
     * ExpectedIndex = (TP + FP) * (TP + FN) / (TP + TN + FP + FN)
     * MaxIndex = (TP + FP + TP + FN) / 2
     * ARI = (Index - ExpectedIndex) / (MaxIndex - ExpectedIndex)
     */
    public static final ParamInfo<Double> ARI = ParamInfoFactory
        .createParamInfo("ari", Double.class)
        .setDescription("adjusted rand index")
        .setHasDefaultValue(null)
        .build();

    /**
     * A higher Silhouette Coefficient score relates to a model with better defined clusters, range[-1, 1].
     *
     * s = (b - a)/max(a, b)
     *
     * a: The mean distance between a sample and all other points in the same class.
     *
     * b: The mean distance between a sample and all other points in the next nearest cluster.
     */
    public static final ParamInfo<Double> SILHOUETTE_COEFFICIENT = ParamInfoFactory
        .createParamInfo("silhouetteCoefficient", Double.class)
        .setDescription("silhouetteCoefficient")
        .setHasDefaultValue(null)
        .build();

    public Integer getK() {
        return get(K);
    }

    public Integer getCount() {return get(COUNT);}

    public Double getSsw() {return get(SSW);}

    public Double getSsb() {return get(SSB);}

    public String[] getClusterArray() {return get(CLUSTER_ARRAY);}

    public double[] getCountArray() {return get(COUNT_ARRAY);}

    public Double getNmi() {
        return get(NMI);
    }

    public Double getPurity() {
        return get(PURITY);
    }

    public Double getRi() {
        return get(RI);
    }

    public Double getAri() {
        return get(ARI);
    }

    public Double getSilhouetteCoefficient() {
        return get(SILHOUETTE_COEFFICIENT);
    }

    public ClusterMetrics(Row row) {
        super(row);
    }

    public ClusterMetrics(Params params) {
        super(params);
    }

}
