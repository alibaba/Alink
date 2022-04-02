package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.common.similarity.KDTree;
import com.alibaba.alink.params.shared.clustering.HasFastDistanceType.DistanceType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 DBSCAN is a clustering method. This outlier determines the parameter eps using statistical techniques.
 (https://link.springer.com/chapter/10.1007/978-3-319-44944-9_24)
 */
public class DbscanDetector extends OutlierDetector {
	private static final double ZERO = 1e-18;
	private static final int MAX_CONSIDERED_NEIGHBOR_NUM = 126;
	private static final int WITHIN_STANDARD_DEVIATION_NUM = 1;
	private static final int NUM_THREAD = 12;
	private DistanceType distanceType;
	private int K;

	public DbscanDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.distanceType = params.get(DbscanDetectorParams.DISTANCE_TYPE);
		this.K = params.get(DbscanDetectorParams.MIN_POINTS);
	}

	@Override
	public Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast) throws Exception {
		List <Row> rowTable = series.getRows();
		int m = series.getNumRow();
		int n = series.getNumCol();
		int iStart = detectLast ? m - 1 : 0;
		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[m - iStart];
		if (m < K) {
			if (detectLast) {
				Map <String, String> infoMap = new HashMap <>();
				infoMap.put("cluster_size", "1");
				infoMap.put("label", "-1");
				results[0] = Tuple3.of(true, 1.0, infoMap);
			} else {
				for (int i = 0; i < m; i++) {
					Map <String, String> infoMap = new HashMap <>();
					infoMap.put("cluster_size", "1");
					infoMap.put("label", "-1");
					results[i] = Tuple3.of(true, 1.0, infoMap);
				}
			}
		}
		UnionJoin unionJoin = new UnionJoin(m);
		TableSchema schema = series.getSchema();
		Vector[] vectors = OutlierUtil.getVectors(series, this.params);

		/*
		 To avoid recording every distance and space out of memory, use consideredNeighborNum to decrease 
		 the number of adjacent neighbors. 
		 Based on the k-th distance of every point, eps is calculated:
		 eps = mean_of_k_th_dist + WITHIN_STANDARD_DEVIATION_NUM * standard_deviation
		 */
		int consideredNeighborNum = Math.min(MAX_CONSIDERED_NEIGHBOR_NUM, m - 1);
		consideredNeighborNum = Math.max(consideredNeighborNum, K);
		int[][] ascendingNeighbors = new int[m][consideredNeighborNum];
		double[][] neighborDists = new double[m][consideredNeighborNum];
		List <Tuple2 <Integer, Double>> kDist = new ArrayList <>(m);
		/*
		Use multi threads to calculate distance of k nearest neighbors of every point
		Use one thread when samples are not too many.
		 */
		int numThread = m >= MAX_CONSIDERED_NEIGHBOR_NUM * NUM_THREAD ? NUM_THREAD : 1;
		parallelDistanceSort[] parallelJobs = new parallelDistanceSort[numThread];
		for (int i = 0; i < numThread; i++) {
			parallelJobs[i] = new parallelDistanceSort(i, numThread, K, consideredNeighborNum, distanceType,
				vectors.clone());
			parallelJobs[i].start();
		}
		for (int i = 0; i < m; i++) {
			kDist.add(Tuple2.of(0, 0.0));
		}
		for (int i = 0; i < numThread; i++) {
			parallelJobs[i].join();
			int s = parallelJobs[i].start;
			int t = parallelJobs[i].end;
			for (int j = s; j < t; j++) {
				System.arraycopy(parallelJobs[i].neighborDists[j - s], 0, neighborDists[j], 0, consideredNeighborNum);
				System.arraycopy(parallelJobs[i].ascendingNeighbors[j - s], 0, ascendingNeighbors[j], 0,
					consideredNeighborNum);
				kDist.set(j, parallelJobs[i].kDist.get(j - s));
			}
		}

		/* 
		 samples are considered in ascending order of their k-dist values 
		 */
		kDist.sort(new Comparator <Tuple2 <Integer, Double>>() {
			@Override
			public int compare(Tuple2 <Integer, Double> o1, Tuple2 <Integer, Double> o2) {
				return o1.f1 - o2.f1 < 0 ? -1 : o1.f1.equals(o2.f1) ? 0 : 1;
			}
		});
		
		/*
		 then taking each point p , if the k-dist' value for any point in its four nearest neighbors is not set
		 so far, this value will be set to the 4-dist value of point p.
		 */
		double[] kDistHat = new double[m];
		Arrays.fill(kDistHat, -1);
		double mean = 0.0;
		for (int i = 0; i < m; i++) {
			Tuple2 <Integer, Double> topI = kDist.get(i);
			int topIId = topI.f0;
			double topIDist = topI.f1;
			if (kDistHat[topIId] != -1) {
				mean = mean + (kDistHat[topIId] - mean) / (i + 1);
				continue;
			} else {
				kDistHat[topIId] = topIDist;
				mean = mean + (kDistHat[topIId] - mean) / (i + 1);
			}
			for (int j = 0; j < K; j++) {
				int neighbor = ascendingNeighbors[topIId][j];
				kDistHat[neighbor] = kDistHat[neighbor] == -1 ? topIDist : kDistHat[neighbor];
			}
		}

		/*
		 calculate average and standard deviation of kDistHat
		 */
		double sd = 0.0;
		for (int i = 0; i < m; i++) {
			double val = Math.pow((kDistHat[i] - mean), 2);
			sd = sd + (val - sd) / (i + 1);
		}
		sd = Math.sqrt(sd);
		
		/*
		 Use given eps if it is set. 
		 */
		double eps = mean + WITHIN_STANDARD_DEVIATION_NUM * sd;
		eps = params.get(DbscanDetectorParams.EPSILON) > 0 ? params.get(DbscanDetectorParams.EPSILON) : eps;


		/*
		 To union join connected samples 
		 */
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < consideredNeighborNum; j++) {
				if (neighborDists[i][j] <= eps) {
					unionJoin.join(i, ascendingNeighbors[i][j]);
				} else {
					break;
				}
			}
		}
		
		/*
		 Outlier detection，based on the size of connected samples 
		 */
		if (detectLast) {
			Map <String, String> infoMap = new HashMap <>();
			int clusterSize = unionJoin.getClusterSize(m - 1);
			double score = clusterSize > K ? Math.min(1.0, neighborDists[iStart][K - 1] / eps)
				: neighborDists[iStart][K - 1] / eps;
			infoMap.put("cluster_size", String.valueOf(clusterSize));
			infoMap.put("label", clusterSize <= K ? "-1" : String.valueOf(unionJoin.find(m - 1)));
			results[0] = Tuple3.of(clusterSize <= K ? true : false, score, infoMap);
		} else {
			for (int i = 0; i < m; i++) {
				Map <String, String> infoMap = new HashMap <>();
				int clusterSize = unionJoin.getClusterSize(i);
				double score = clusterSize > K ? Math.min(1.0, neighborDists[i][K - 1] / eps)
					: neighborDists[i][K - 1] / eps;
				infoMap.put("cluster_size", String.valueOf(clusterSize));
				infoMap.put("label", clusterSize <= K ? "-1" : String.valueOf(unionJoin.find(i)));
				results[i] = Tuple3.of(clusterSize <= K ? true : false,
					score, infoMap);
			}
		}
		return results;
	}

	public static class UnionJoin {
		/*
		pre records cluster id，num records cluster size
		 */
		private int[] pre;
		private int[] num;
		private final int n;

		public UnionJoin(int n) {
			this.n = n;
			this.pre = new int[n];
			this.num = new int[n];
			for (int i = 0; i < n; i++) { pre[i] = i; }
			Arrays.fill(num, 1);
		}

		public UnionJoin(UnionJoin src) {
			this.n = src.n;
			this.pre = new int[n];
			this.num = new int[n];
			System.arraycopy(src.pre, 0, this.pre, 0, n);
			System.arraycopy(src.num, 0, this.num, 0, n);
		}

		public int find(int x) {
			if (pre[x] == x) {return x;}
			return pre[x] = find(pre[x]);
		}

		public boolean join(int x, int y) {
			x = find(x);
			y = find(y);
			if (x == y) {
				return false;
			} else if (x < y) {
				pre[y] = x;
				num[x] += num[y];
				num[y] = 0;
			} else {
				pre[x] = y;
				num[y] += num[x];
				num[x] = 0;
			}
			return true;
		}

		public int getN() {
			return n;
		}

		public int getClusterSize(int i) {
			return this.num[this.find(i)];
		}
	}

	public static class parallelDistanceSort extends Thread {
		final int threadId;
		final int size;
		final int K;
		final int start;
		final int end;
		final int neighborNum;
		private KDTree kdTree;
		private FastDistanceVectorData[] fastDistanceVectors;
		int[][] ascendingNeighbors;
		double[][] neighborDists;
		List <Tuple2 <Integer, Double>> kDist;

		public parallelDistanceSort(int i, int nJob, int k, int neighborNum, DistanceType distanceType,
									Vector[] vectors) {
			this.threadId = i;
			this.K = k;
			this.neighborNum = neighborNum;
			this.size = vectors.length;
			i = size - size / nJob * nJob;
			this.start = this.threadId < i ? this.threadId * (size / nJob + 1)
				: i * (size / nJob + 1) + (this.threadId - i) * (size / nJob);
			this.end = this.threadId < i ? this.start + (size / nJob + 1)
				: this.start + (size / nJob);
			ascendingNeighbors = new int[end - start][neighborNum];
			neighborDists = new double[end - start][neighborNum];
			kDist = new ArrayList <>(end - start);
			fastDistanceVectors = new FastDistanceVectorData[this.size];
			FastDistance distance = distanceType.getFastDistance();
			for (i = 0; i < this.size; i++) {
				fastDistanceVectors[i] = distance.prepareVectorData(Row.of(vectors[i], i), 0, 1);
			}
			kdTree = new KDTree(fastDistanceVectors.clone(), vectors[0].size(), distance);
			kdTree.buildTree();
		}

		@Override
		public void run() {

			for (int i = 0; i < end - start; i += 1) {
				// The sample itself could be returned, so fetch K + 1 neighbors
				Tuple2 <Double, Row>[] results = kdTree.getTopN(neighborNum + 1,
					fastDistanceVectors[start + i]);
				int p = 0;
				for (Tuple2 <Double, Row> kNeighbor : results) {
					int index = (Integer) kNeighbor.f1.getField(0);
					if (index == start + i) {
						continue;
					}
					neighborDists[i][p] = Math.max(kNeighbor.f0, ZERO);
					ascendingNeighbors[i][p] = (Integer) kNeighbor.f1.getField(0);
					p += 1;
					if (p == neighborNum) {
						break;
					}
				}
				if(neighborNum==1 && size==1){
					/* if only has one sample, return itself */
					kDist.add(Tuple2.of(i + start, 0.0));
					return;
				}
				kDist.add(Tuple2.of(i + start, neighborDists[i][K - 1]));
			}
		}
	}
}
