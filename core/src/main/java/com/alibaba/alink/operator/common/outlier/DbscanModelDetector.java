package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.distance.CosineDistance;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.distance.ManHattanDistance;
import com.alibaba.alink.operator.common.outlier.DbscanDetector.UnionJoin;
import com.alibaba.alink.params.shared.clustering.HasFastDistanceType.DistanceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DbscanModelDetector extends ModelOutlierDetector {
	private static final Logger LOG = LoggerFactory.getLogger(DbscanModelDetector.class);
	private final String[] selectedCols;
	private final String[] modelCols;
	private final DistanceType distanceType;
	private final double eps;
	private final double minPoints;
	private List <Row> modelData;
	private UnionJoin dbscanClusters;
	private FastDistance fastDistance;

	public DbscanModelDetector(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		this.selectedCols = params.contains(DbscanDetectorParams.FEATURE_COLS) ? params.get(
			DbscanDetectorParams.FEATURE_COLS) : new String[] {dataSchema.getFieldNames()[0]};
		this.modelCols = modelSchema.getFieldNames();
		this.distanceType = params.get(DbscanDetectorParams.DISTANCE_TYPE);
		if (this.distanceType == DistanceType.COSINE) {
			fastDistance = new CosineDistance();
		} else if (this.distanceType == DistanceType.CITYBLOCK) {
			fastDistance = new ManHattanDistance();
		} else {
			fastDistance = new EuclideanDistance();
		}
		eps = params.get(DbscanDetectorParams.EPSILON);
		minPoints = params.get(DbscanDetectorParams.MIN_POINTS);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		modelData = new ArrayList <>();
		int[] featureCols = TableUtil.findColIndicesWithAssertAndHint(modelCols, selectedCols);
		int featureNum = featureCols.length;

		/*
		Step1: 用模型的数据初始化一个距离矩阵，模型实际上是一些样本
		 */
		for (Row r : modelRows) {
			DenseVector vec = new DenseVector(featureNum);
			for (int j = 0; j < featureNum; j++) {
				vec.set(j, ((Number) r.getField(featureCols[j])).doubleValue());
			}
			modelData.add(Row.of(vec));
		}
		FastDistanceMatrixData fastDistanceMatrixData = (FastDistanceMatrixData)
			fastDistance.prepareMatrixData(modelData, 0).get(0);
		DenseMatrix neighborhoods = fastDistance.calc(fastDistanceMatrixData, fastDistanceMatrixData);

		/*
		Step2: 对模型中的样本使用DBSCAN聚类算法
		 */
		int m = modelData.size();
		dbscanClusters = new UnionJoin(m);
		for (int i = 0; i < m; i++) {
			for (int j = i + 1; j < m; j++) {
				if (neighborhoods.get(i, j) <= eps) {
					dbscanClusters.join(i, j);
				}
			}
		}

	}

	@Override
	protected Tuple3 <Boolean, Double, Map <String, String>> detect(SlicedSelectedSample selection) throws Exception {
		UnionJoin unionJoin = new UnionJoin(dbscanClusters);
		int n = unionJoin.getN();
		List neighbors = new LinkedList <Integer>();
		DenseVector sample = new DenseVector(selection.length());
		
		for (int j = 0; j < selection.length(); j++) {
			sample.set(j, ((Number) selection.get(j)).doubleValue());
		}

		/*
		Step1: 计算样本到每个模型元素的聚类，与样本相邻的模型元素加入neighbors
		 */
		for (int i = 0; i < n; i++) {
			DenseVector vec = (DenseVector) modelData.get(i).getField(0);
			double distance = fastDistance.calc(vec, sample);
			if (distance <= eps) {
				neighbors.add(i);
			}
		}

		/*
		Step2: 没有相邻元素的样本被判定为异常点
		 */
		int neighborNum = neighbors.size();
		if (neighborNum == 0) {
			return Tuple3.of(true, 0.0, null);
		}

		/*
		Step3: 连接neighbor中元素属于的簇，用最终得到的簇的大小判断样本是否属于异常点
		 */
		for (int i = 1; i < neighborNum; i++) {
			unionJoin.join((int) neighbors.get(i), (int) neighbors.get(i - 1));
		}
		int clusterSize = 1 + unionJoin.getClusterSize((int) neighbors.get(0));
		return Tuple3.of(clusterSize < minPoints ? true : false, Math.pow(2, -(clusterSize / (double) minPoints)),
			null);
	}
}
