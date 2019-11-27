package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansTrainModelData.ClusterSummary;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.ArrayList;
import java.util.List;

/**
 * KMeans Model.
 * Save the id, center point and point number of clusters.
 */
public class KMeansModelDataConverter extends SimpleModelDataConverter<KMeansTrainModelData, KMeansPredictModelData> {
	public KMeansModelDataConverter() {}

	@Override
	public Tuple2<Params, Iterable<String>> serializeModel(KMeansTrainModelData modelData) {
		List <String> data = new ArrayList <>();
		for (ClusterSummary centroid : modelData.centroids) {
			data.add(JsonConverter.toJson(centroid));
		}
		return Tuple2.of(modelData.params.toParams(), data);
	}

	@Override
	public KMeansPredictModelData deserializeModel(Params params, Iterable<String> data) {
		KMeansTrainModelData trainModelData = KMeansUtil.loadModelForTrain(params, data);
		return KMeansUtil.transformTrainDataToPredictData(trainModelData);
	}
}
