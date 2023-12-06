package com.alibaba.alink.common.insights;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
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
	private static double MAX_OUTLIER_PERCENT = 0.05;
	private static double RADIUS_EXPAND_RATIO = 2;
	private static int RETRY_NUM = 3;
	private static int MIN_SAMPLE_NUM = 10;
	private static int MAX_SAMPLE_NUM = 1000;
	private static String CLUSTER_COLUMN_NAME = "cluster";

	public ScatterplotClusteringInsight(Insight insight) {
		super(insight);
	}

	@Override
	public void fillLayout() {
		this.insight.layout.xAxis = this.insight.subject.measures.get(0).aggr + "(" + this.insight.subject.measures.get(0).colName + ")";
		this.insight.layout.yAxis = this.insight.subject.measures.get(1).aggr + "(" + this.insight.subject.measures.get(1).colName + ")";
		this.insight.layout.title = insight.layout.xAxis + " and " + insight.layout.yAxis + " cluster";
		StringBuilder builder = new StringBuilder();
		if (null != insight.subject.subspaces && !insight.subject.subspaces.isEmpty()) {
			builder.append(insight.getSubspaceStr(insight.subject.subspaces)).append(" 条件下，");
		}
		builder.append(this.insight.layout.xAxis).append(" 与 ").append(this.insight.layout.yAxis).append(" 有聚类特性");
		this.insight.layout.description = builder.toString();
	}

	@Override
	public double computeScore(LocalOperator <?>... sources) {
		//String[] columns = new String[] {insight.subject.breakdown.colName, MEASURE_NAME_PREFIX + "0"};
		HashMap <Object, Number> measureValues0 = initData(sources[0]);
		HashMap<Object, Number> measureValues1 = initData(sources[1]);

		List <Tuple3 <Double, Double, Object>> measureValues = new ArrayList <>();

		for (Entry <Object, Number> entry : measureValues0.entrySet()) {
			if (!measureValues1.containsKey(entry.getKey())) {
				continue;
			}
			Double value0 = entry.getValue().doubleValue();
			Double value1 = measureValues1.get(entry.getKey()).doubleValue();
			measureValues.add(Tuple3.of(value0, value1, entry.getKey()));
		}

		int count = measureValues.size();
		// 小于或者大于一定条数，返回0分
		if (count <= MIN_SAMPLE_NUM || count >= MAX_SAMPLE_NUM) {
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
		double radius = avgDistance;
		Tuple4 <Integer, Integer, Double, int[]> result = dbscan(distanceMatrix, minPts, radius, count);
		double distanceDiff = -1.0;
		int currentTry = 0;
		while (true) {
			// 聚类结果为2或者3，离群点数量小于5%，符合一定规律，退出搜索过程
			if (result.f0 > 1 && result.f0 <= MAX_CLUSTERS_NUM && result.f1 < MAX_OUTLIER_PERCENT * count) {
				distanceDiff = result.f2;
				break;
			}
			// 聚类结果大于3，分类太多，或者离群点太多，降低半径分类结果分类或离群点会更多，退出搜索过程
			if (result.f0 > MAX_CLUSTERS_NUM || result.f1 >= MAX_OUTLIER_PERCENT * count) {
				break;
			}
			currentTry++;
			if (currentTry >= RETRY_NUM) {
				break;
			}
			radius /= RADIUS_EXPAND_RATIO;
			result = dbscan(distanceMatrix, minPts, radius, count);
		}
		if (distanceDiff < 0) {
			return 0;
		}
		// 类的间距越大，信息量越大，参考逻辑回归
		double ex = Math.pow(Math.E, result.f2);
		double score = ex / (1 + ex);

		TableSchema outSchema = new TableSchema(new String[]{
			sources[0].getSchema().getFieldName(0).get(),
			MEASURE_NAME_PREFIX + "0",
			MEASURE_NAME_PREFIX + "1",
			CLUSTER_COLUMN_NAME
		}, new TypeInformation[]{
			sources[0].getSchema().getFieldType(0).get(),
			sources[0].getSchema().getFieldType(1).get(),
			sources[1].getSchema().getFieldType(1).get(),
			Types.INT
		});
		List<Row> rows = new ArrayList <>();
		for (int i = 0; i < measureValues.size(); i++) {
			Row row = new Row(4);
			Object key = measureValues.get(i).f2;
			row.setField(0, measureValues.get(i).f2);
			row.setField(1, measureValues0.get(key));
			row.setField(2, measureValues1.get(key));
			row.setField(3, result.f3[i]);
		}
		MTable mTable = new MTable(rows, outSchema);
		this.insight.layout.data = mTable;
		return score;
	}

	private Tuple4 <Integer, Integer, Double, int[]> dbscan(double[][] distanceMatrix, int minPts, double radius, int count) {
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
		return Tuple4.of(clusterStartId, Integer.valueOf(outliers), distanceRatio, clusterId);
	}
}
