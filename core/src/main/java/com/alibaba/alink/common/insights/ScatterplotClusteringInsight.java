package com.alibaba.alink.common.insights;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.operator.local.LocalOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;

public class ScatterplotClusteringInsight extends CrossMeasureCorrelationInsight {

	private static int MAX_CLUSTERS_NUM = 3;
	private static double MAX_OUTLIER_PERCENT = 0.1;
	private static double RADIUS_EXPAND_RATIO = 1.1;
	private static int RETRY_NUM = 10;
	private static int MIN_SAMPLE_NUM = 10;

	public ScatterplotClusteringInsight(Subject subject, InsightType type) {
		super(subject, type);
	}

	@Override
	public double computeScore(LocalOperator <?>... source) {
		String[] columns = new String[] {insight.subject.breakdown.colName, MEASURE_NAME_PREFIX + "0"};
		HashMap <Object, Double> measuerValues0 = initData(source[0].select(columns));
		HashMap<Object, Double> measuerValues1 = initData(source[1].select(columns));

		List <Tuple2 <Double, Double>> measureValues = new ArrayList <>();

		for (Entry <Object, Double> entry : measuerValues0.entrySet()) {
			if (!measuerValues1.containsKey(entry.getKey())) {
				continue;
			}
			Double value0 = entry.getValue();
			Double value1 = measuerValues1.get(entry.getKey());
			measureValues.add(Tuple2.of(value0, value1));
		}

		int count = measureValues.size();
		if (count <= MIN_SAMPLE_NUM) {
			return 0;
		}
		double totalDistance = 0.0;
		double[][] distanceMatrix = new double[count][count];
		for (int i = 0; i < count - 1; i++) {
			for (int j = i + 1; j < count; j++) {
				Double x = measureValues.get(i).f0 - measureValues.get(j).f0;
				Double y = measureValues.get(i).f1 - measureValues.get(j).f1;
				Double distance = Math.sqrt(x * x + y * y);
				distanceMatrix[i][j] = distance;
				distanceMatrix[j][i] = distance;
				totalDistance += distance;
			}
		}
		double avgDistance = totalDistance / (count * (count - 1) / 2);
		int minPts = count / 10;
		double radius = avgDistance / 5;
		Tuple3<Integer, Integer, Double> result = dbscan(distanceMatrix, minPts, radius, count);
		// 当聚类数量大于3个，或者离群点大于10%，减小minPts或者增加radius，继续计算
		int currentTry = 0;
		while (result.f0 > MAX_CLUSTERS_NUM || result.f1 > MAX_OUTLIER_PERCENT * count) {
			radius = radius * RADIUS_EXPAND_RATIO;
			result = dbscan(distanceMatrix, minPts, radius, count);
			currentTry++;
			// 达到最大尝试次数
			if (currentTry >= RETRY_NUM) {
				break;
			}
		}
		// 太分散，没有离群点，没有有价值信息
		if (result.f0 == 1 || result.f0 > 3 || result.f1 > MAX_OUTLIER_PERCENT * count) {
			return 0;
		}
		// 类的间距越大，信息量越大，参考逻辑回归
		double ex = Math.pow(Math.E, result.f2);
		double score = ex / (1 + ex);
		return score;
	}

	private Tuple3 <Integer, Integer, Double> dbscan(double[][] distanceMatrix, int minPts, double radius, int count) {
		boolean[] isCorePoint = new boolean[count];
		int[] clusterId = new int[count];
		int[][] isCloseMatrix = new int[count][count];
		for (int i = 0; i < count; i++) {
			isCorePoint[i] = false;
			clusterId[i] = -1;
		}
		for (int i = 0; i < count - 1; i++) {
			isCloseMatrix[i][i] = 0;
			for (int j = 0; j < count; j++) {
				if (i == j) {
					continue;
				}
				if (distanceMatrix[i][j] < radius) {
					isCloseMatrix[i][j] = 1;
					isCloseMatrix[j][i] = 1;
				} else {
					isCloseMatrix[i][j] = 0;
					isCloseMatrix[j][i] = 0;
				}
			}
		}
		for (int i = 0; i < count; i++) {
			if (Arrays.stream(isCloseMatrix[i]).sum() >= minPts) {
				isCorePoint[i] = true;
			} else {
				isCorePoint[i] = false;
			}
		}
		int clusterStartId = 0;
		Queue <Integer> queue = new LinkedList <>();
		for (int i = 0; i < count; i++) {
			// 不是核心点，或者已经有类别，则不处理
			if (!isCorePoint[i] || clusterId[i] >= 0) {
				continue;
			}
			clusterId[i] = clusterStartId;
			for (int j = 0; j < count; j++) {
				if (i == j || clusterId[j] >= 0) {
					continue;
				}
				if (isCloseMatrix[i][j] == 1) {
					clusterId[j] = clusterId[i];
					if (isCorePoint[j]) {
						queue.add(Integer.valueOf(j));
					}
				}
			}
			while (!queue.isEmpty()) {
				int currentIdx = queue.poll();
				for (int x = 0; x < count; x++) {
					if (currentIdx == x || clusterId[x] >= 0) {
						continue;
					}
					if (isCloseMatrix[currentIdx][x] == 1) {
						clusterId[x] = clusterId[currentIdx];
						if (isCorePoint[x]) {
							queue.add(Integer.valueOf(x));
						}
					}
				}
			}
			clusterStartId++;
		}
		int outliers = 0;
		for (int i = 0; i < count; i++) {
			if (clusterId[i] < 0) {
				outliers ++;
			}
		}
		double outDistance = 0.0;
		int outCount = 0;
		double inDistance = 0.0;
		int inCount = 0;
		for (int i = 0; i < count - 1; i++) {
			for (int j = i + 1; j < count; j++) {
				if (isCloseMatrix[i][j] == 1) {
					inDistance += distanceMatrix[i][j];
					inCount++;
				} else {
					outDistance += distanceMatrix[i][j];
					outCount++;
				}
			}
		}
		if (inCount != 0) {
			inDistance = inDistance / inCount;
		}
		if (outCount != 0) {
			outDistance = outDistance / outCount;
		}
		Double distanceRatio = 0.0;
		if (inDistance != 0) {
			distanceRatio = (outDistance / inDistance - 1.0) * 0.5;
		}
		return Tuple3.of(clusterStartId, Integer.valueOf(outliers), distanceRatio);
	}
}
