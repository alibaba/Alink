package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.common.similarity.KDTree;
import com.alibaba.alink.params.outlier.HasKDEKernelType.KernelType;
import com.alibaba.alink.params.outlier.KdeDetectorParams;
import com.alibaba.alink.params.shared.clustering.HasFastDistanceType.DistanceType;
import org.apache.commons.math3.special.Gamma;

import java.util.HashMap;
import java.util.Map;

public class KdeDetector extends OutlierDetector {
	private static final double EPS = 1e-18;
	private final DistanceType distanceType;
	private final KernelType kernelType;
	private int numNeighbors;
	private final double threshold;
	private final double bandwidth;

	public KdeDetector(TableSchema dataSchema,
					   Params params) {
		super(dataSchema, params);
		numNeighbors = params.get(KdeDetectorParams.NUM_NEIGHBORS);
		threshold = params.get(KdeDetectorParams.OUTLIER_THRESHOLD);
		kernelType = params.get(KdeDetectorParams.KDE_KERNEL_TYPE);
		bandwidth = params.get(KdeDetectorParams.BANDWIDTH);
		distanceType = params.get(KdeDetectorParams.DISTANCE_TYPE);
	}

	@Override
	protected Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast)
		throws Exception {
		Vector[] values = OutlierUtil.getVectors(series, params);
		FastDistance fastDistance = distanceType.getFastDistance();
		int n = values.length;

		if (0 == n) {
			//noinspection unchecked
			return new Tuple3[0];
		}

		if (numNeighbors < 0 || numNeighbors > n) {
			numNeighbors = n;
		}

		// Get vector size
		int vectorSize = values[0].size();
		if (values[0] instanceof SparseVector && values[0].size() < 0) {
			for (Vector value : values) {
				SparseVector sv = (SparseVector) value;
				int[] indices = sv.getIndices();
				vectorSize = Math.max(vectorSize, indices[indices.length - 1] + 1);
			}
		}

		// For each sample, get k-neighbors and k-distance.
		FastDistanceVectorData[] fastDistanceVectors = new FastDistanceVectorData[n];
		for (int k = 0; k < n; k += 1) {
			fastDistanceVectors[k] = fastDistance.prepareVectorData(Row.of(values[k], k), 0, 1);
		}
		// The array is cloned as KDTree will reorder it.
		KDTree kdTree = null;
		if (numNeighbors < n) {
			new KDTree(fastDistanceVectors.clone(), vectorSize, fastDistance);
			kdTree.buildTree();
		}

		double[][] kNeighborDists = new double[n][numNeighbors];
		for (int i = 0; i < n; i++) {
			// get local neighbors distance
			if (numNeighbors < n) {
				Tuple2 <Double, Row>[] results = kdTree.getTopN(numNeighbors, fastDistanceVectors[i]);
				int k = 0;
				for (Tuple2 <Double, Row> kNeighbor : results) {
					int index = (Integer) kNeighbor.f1.getField(0);
					kNeighborDists[i][k] = Math.max(kNeighbor.f0, EPS);
					k += 1;
				}
			} else {
				for (int k = 0; k < n; k++) {
					kNeighborDists[i][k] = fastDistance.calc(fastDistanceVectors[i].getVector(),
						fastDistanceVectors[k].getVector());
				}
			}
		}

		// Calculate lde (local dense estimate using reachability distance instead of direct distance) 
		// reach-distance: reach(p, o)=max(k-distance(p),distance(p, o))
		double[] kde = new double[n];
		double[] zscore = new double[n];
		for (int k = 0; k < n; k += 1) {
			for (int i = 0; i < numNeighbors; i += 1) {
				double rd = kNeighborDists[k][i];
				double logKernel = this.logKernelFunction(bandwidth, rd, kernelType);
				kde[k] = i == 0 ? logKernel : logAddExp(kde[k], logKernel);
			}
			double tmp = Math.exp(logKernelNorm(bandwidth, vectorSize, kernelType));
			kde[k] = tmp * Math.exp(kde[k]) / numNeighbors;
			zscore[k] = 1 / Math.max(kde[k], EPS);
		}

		//noinspection unchecked
		Tuple3 <Boolean, Double, Map <String, String>>[] results = detectLast ? new Tuple3[1] : new Tuple3[n];
		if (detectLast) {
			Map <String, String> info = new HashMap <>();
			info.put("KDE", String.valueOf(kde[n - 1]));
			results[0] = Tuple3.of(zscore[n - 1] > threshold, zscore[n - 1], info);
		} else {
			for (int k = 0; k < n; k += 1) {
				Map <String, String> info = new HashMap <>();
				info.put("KDE", String.valueOf(kde[k]));
				results[k] = Tuple3.of(zscore[k] > threshold, zscore[k], info);
			}
		}
		return results;
	}

	private static double LOG_2PI = Math.log(2 * Math.PI);

	private static double LOG_PI = Math.log(Math.PI);

	private static double logVn(int n) {
		return 0.5 * n * LOG_PI - Gamma.logGamma(0.5 * n + 1);
	}

	private static double logSn(int n) {
		return LOG_2PI + logVn(n - 1);
	}

	private static double logAddExp(double x1, double x2) {
		return Math.log(Math.exp(x1) + Math.exp(x2));
	}

	private double logKernelNorm(double h, int d, KernelType kernelType) {
		double factor = 0;
		switch (kernelType) {
			case GAUSSIAN:
				factor = 0.5 * d * LOG_2PI;
				break;
			case LINEAR:
				factor = logVn(d) - Math.log(d + 1.0);
				break;
			default:
				throw new IllegalArgumentException("KDE Kernel not recognized");
		}
		return -factor - d * Math.log(h);
	}

	private double logKernelFunction(double h, double dist, KernelType kernelType) {
		switch (kernelType) {
			case GAUSSIAN:
				return -0.5 * (dist * dist) / (h * h);
			case LINEAR:
				return dist < h ? Math.log(1 - dist / h) : Double.NEGATIVE_INFINITY;
			default:
				throw new IllegalArgumentException("KDE Kernel not recognized");
		}
	}
}
