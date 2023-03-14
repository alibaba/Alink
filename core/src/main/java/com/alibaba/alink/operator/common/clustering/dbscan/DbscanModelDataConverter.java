package com.alibaba.alink.operator.common.clustering.dbscan;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.operator.common.clustering.ClusterSummary;
import com.alibaba.alink.params.clustering.DbscanParams;
import com.alibaba.alink.params.shared.clustering.HasClusteringDistanceType;

import java.util.ArrayList;
import java.util.List;

/**
 * *
 *
 * @author guotao.gt
 */
public class DbscanModelDataConverter extends SimpleModelDataConverter <DbscanModelTrainData, DbscanModelPredictData> {

	public DbscanModelDataConverter() {
	}

	@Override
	public Tuple2 <Params, Iterable <String>> serializeModel(DbscanModelTrainData modelData) {
		List <String> modelRows = new ArrayList <>();
		for (Tuple2 <Vector, Long> centroid : modelData.coreObjects) {
			ClusterSummary c = new ClusterSummary(VectorUtil.toString(centroid.f0), centroid.f1, null,
				Type.CORE.name());
			modelRows.add(c.serialize());
		}
		Params meta = new Params()
			.set(DbscanParams.VECTOR_COL, modelData.vectorColName)
			.set(DbscanParams.EPSILON, modelData.epsilon)
			.set(DbscanParams.DISTANCE_TYPE, modelData.distanceType);
		return Tuple2.of(meta, modelRows);
	}

	@Override
	public DbscanModelPredictData deserializeModel(Params meta, Iterable <String> data) {
		DbscanModelPredictData modelData = new DbscanModelPredictData();
		modelData.epsilon = meta.get(DbscanParams.EPSILON);
		modelData.vectorColName = meta.get(DbscanParams.VECTOR_COL);
		HasClusteringDistanceType.DistanceType distanceType = meta.get(DbscanParams.DISTANCE_TYPE);
		modelData.baseDistance = distanceType.getFastDistance();

		modelData.coreObjects = new ArrayList <>();

		// get the model data
		for (String row : data) {
			try {
				ClusterSummary c = ClusterSummary.deserialize(row);
				Vector vec = VectorUtil.getVector(c.center);
				modelData.coreObjects.add(
					modelData.baseDistance.prepareVectorData(Tuple2.of(vec, Row.of(c.clusterId))));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return modelData;
	}
}
