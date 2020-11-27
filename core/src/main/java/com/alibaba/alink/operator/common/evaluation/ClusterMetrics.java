package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

/**
 * Cluster evaluation metrics.
 */
public class ClusterMetrics extends BaseMetrics <ClusterMetrics> {

	private static final long serialVersionUID = 8668204043992648726L;

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline("Metrics:", '-'));

		sbd.append("k:").append(getK()).append("\n");
		if (getVrc() != null) {
			sbd.append("VRC:").append(PrettyDisplayUtils.display(getVrc())).append("\t")
				.append("DB:").append(PrettyDisplayUtils.display(getDb())).append("\t")
				.append("SilhouetteCoefficient:").append(PrettyDisplayUtils.display(getSilhouetteCoefficient()))
				.append(
				"\n");
		}
		if (getAri() != null) {
			sbd.append("ARI:").append(PrettyDisplayUtils.display(getAri())).append("\t")
				.append("NMI:").append(PrettyDisplayUtils.display(getNmi())).append("\t")
				.append("Purity:").append(PrettyDisplayUtils.display(getPurity())).append("\n");
		}
		if (getConfusionMatrix() != null) {
			long[][] matrix = getConfusionMatrix();
			Long[][] confusionMatrixPri = new Long[matrix[0].length][matrix.length];
			for (int i = 0; i < matrix.length; i++) {
				for (int j = 0; j < matrix[0].length; j++) {
					confusionMatrixPri[j][i] = matrix[i][j];
				}
			}
			sbd.append("\n").append(PrettyDisplayUtils.displayTable(confusionMatrixPri,
				matrix[0].length, matrix.length, this.get(LABEL_ARRAY), this.get(PRED_ARRAY), "Label\\Cluster", 10,
				10));
		}
		return sbd.toString();
	}

	/**
	 * SSW is Tr(Wk), Wk is the within-cluster dispersion matrix.
	 * <p>
	 * SSW = sum(sum(x - u_i))
	 * <p>
	 * x is a sample in cluster i, u_i is the center of cluster i.
	 */
	static final ParamInfo <Double> SSW = ParamInfoFactory
		.createParamInfo("ssw", Double.class)
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
	static final ParamInfo <Double> SSB = ParamInfoFactory
		.createParamInfo("ssb", Double.class)
		.setDescription("ssb")
		.setHasDefaultValue(null)
		.build();

	/**
	 * A higher Calinski-Harabasz score(VRC) relates to a model with better defined clusters.
	 * <p>
	 * CH = Tr(Bk) * (N - k) / Tr(Wk) / (k - 1)
	 * <p>
	 * Tr(Bk) is SSB, Tr(Wk) is SSW, k is the cluster number, N is the total sample number.
	 */
	static final ParamInfo <Double> VRC = ParamInfoFactory
		.createParamInfo("vrc", Double.class)
		.setDescription("calinskiHarabaz(VRC)")
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
	static final ParamInfo <Double> CP = ParamInfoFactory
		.createParamInfo("cp", Double.class)
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
	static final ParamInfo <Double> SP = ParamInfoFactory
		.createParamInfo("sp", Double.class)
		.setDescription("seperation")
		.setHasDefaultValue(null)
		.build();

	/**
	 * A lower Davies-Bouldin index relates to a model with better separation between the clusters.
	 * <p>
	 * The index is defined as the average calc between each cluster C_i and its most similar one C_j.
	 * <p>
	 * R_i,j = (compactness_i + compactness_j) / d_i,j
	 * <p>
	 * R_i,j the the calc between C_i and C_j, d_i,j is the distance between centers i and j.
	 * <p>
	 * DB = sum(max R_i,j) / k
	 */
	static final ParamInfo <Double> DB = ParamInfoFactory
		.createParamInfo("db", Double.class)
		.setDescription("daviesBouldin")
		.setHasDefaultValue(null)
		.build();

	/**
	 * Cluster Number.
	 */
	static final ParamInfo <Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("cluster number")
		.setRequired()
		.build();

	/**
	 * Count.
	 */
	static final ParamInfo <Integer> COUNT = ParamInfoFactory
		.createParamInfo("count", Integer.class)
		.setDescription("count")
		.setRequired()
		.build();

	static final ParamInfo <String[]> CLUSTER_ARRAY = ParamInfoFactory
		.createParamInfo("clusterArray", String[].class)
		.setDescription("clusterArray")
		.setRequired()
		.build();

	static final ParamInfo <double[]> COUNT_ARRAY = ParamInfoFactory
		.createParamInfo("countArray", double[].class)
		.setDescription("countArray")
		.setRequired()
		.build();

	static final ParamInfo <long[][]> CONFUSION_MATRIX = ParamInfoFactory
		.createParamInfo("confusionMatrix", long[][].class)
		.setDescription("confusionMatrix")
		.setHasDefaultValue(null)
		.build();

	static final ParamInfo <String[]> LABEL_ARRAY = ParamInfoFactory
		.createParamInfo("labelArray", String[].class)
		.setDescription("labelArray")
		.setRequired()
		.build();

	static final ParamInfo <String[]> PRED_ARRAY = ParamInfoFactory
		.createParamInfo("predArray", String[].class)
		.setDescription("predArray")
		.setRequired()
		.build();

	/**
	 * NMI = MutualInformation * 2/ (entropy(pred) + entropy(actual))
	 * MatrixRatio(i,j) = matrix(i,j) / total
	 * PredRatio(i) = predict(i) / total
	 * PredRatio(j) = actual(j) / total
	 * MutualInformation = sum(sum(MatrixRatio(i,j) * log2(MatrixRatio(i,j) / PredRatio(i) / PredRatio(j)))
	 * Entropy(pred) = -sum(pred / total * log2(pred / total)
	 * Entropy(actual) = -sum(actual / total * log2(actual / total)
	 */
	static final ParamInfo <Double> NMI = ParamInfoFactory
		.createParamInfo("nmi", Double.class)
		.setDescription("Normalized Mutual Information")
		.setHasDefaultValue(null)
		.build();

	/**
	 * For each cluster, get the max number of one class.
	 * Purity = sum(mi) / total
	 */
	static final ParamInfo <Double> PURITY = ParamInfoFactory
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
	static final ParamInfo <Double> RI = ParamInfoFactory
		.createParamInfo("ri", Double.class)
		.setDescription("rand index")
		.setHasDefaultValue(null)
		.build();

	/**
	 * ExpectedIndex = (TP + FP) * (TP + FN) / (TP + TN + FP + FN)
	 * MaxIndex = (TP + FP + TP + FN) / 2
	 * ARI = (Index - ExpectedIndex) / (MaxIndex - ExpectedIndex)
	 */
	static final ParamInfo <Double> ARI = ParamInfoFactory
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
	static final ParamInfo <Double> SILHOUETTE_COEFFICIENT = ParamInfoFactory
		.createParamInfo("silhouetteCoefficient", Double.class)
		.setDescription("silhouetteCoefficient")
		.setHasDefaultValue(null)
		.build();

	public ClusterMetrics(Row row) {
		super(row);
	}

	public ClusterMetrics(Params params) {
		super(params);
	}

	public Double getVrc() {return get(VRC);}

	public Double getCp() {return get(CP);}

	public Double getSp() {return get(SP);}

	public Double getDb() {return get(DB);}

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

	public long[][] getConfusionMatrix() {
		return get(CONFUSION_MATRIX);
	}
}
