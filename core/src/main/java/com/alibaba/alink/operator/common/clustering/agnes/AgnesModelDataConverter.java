package com.alibaba.alink.operator.common.clustering.agnes;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.operator.common.clustering.ClusterSummary;
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.params.clustering.AgnesParams;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * @author guotao.gt
 */
public class AgnesModelDataConverter extends SimpleModelDataConverter<AgnesModelData, AgnesModelData> {
	/**
	 * predict scope (after loading, before predicting)
	 */

	public AgnesModelDataConverter() {
	}

	@Override
	public Tuple2<Params, Iterable<String>> serializeModel(AgnesModelData modelData) {
		List <String> modelRows = new ArrayList <>();
		for (AgnesSample agnesSample : modelData.centroids) {
			String str = gson.toJson(agnesSample.getVector());
			modelRows.add(new ClusterSummary(str, agnesSample.getClusterId(), agnesSample.getWeight()).serialize());
		}
		Params meta = new Params()
			.set(AgnesParams.K, modelData.k)
			.set(AgnesParams.DISTANCE_THRESHOLD, modelData.distanceThreshold)
			.set(AgnesParams.DISTANCE_TYPE, modelData.distanceType)
			.set(AgnesParams.LINKAGE, modelData.linkage)
			.set(AgnesParams.ID_COL, modelData.idCol);
		return Tuple2.of(meta, modelRows);
	}

	@Override
	public AgnesModelData deserializeModel(Params meta, Iterable<String> data) {
		AgnesModelData modelData = new AgnesModelData();
		modelData.k = meta.get(AgnesParams.K);
		modelData.distanceThreshold = meta.get(AgnesParams.DISTANCE_THRESHOLD);
		modelData.linkage = meta.get(AgnesParams.LINKAGE);
		modelData.idCol = meta.contains(AgnesParams.ID_COL) ? meta.get(AgnesParams.ID_COL) : "";
		modelData.centroids = new ArrayList <AgnesSample>();
		modelData.distanceType = meta.get(AgnesParams.DISTANCE_TYPE);

		// get the model data
		for (String row : data) {
			ClusterSummary c = ClusterSummary.deserialize(row);
			long clusterId = c.clusterId;
			double weight = c.weight;
			AgnesSample agnesSample = new AgnesSample(null, clusterId, gson.fromJson(c.center, DenseVector.class),
				weight);
			modelData.centroids.add(agnesSample);
		}
		return modelData;
	}
}
