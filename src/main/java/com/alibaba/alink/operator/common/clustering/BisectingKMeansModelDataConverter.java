package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelData.ClusterSummary;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.params.clustering.BisectingKMeansTrainParams;
import com.alibaba.alink.params.shared.HasVectorSizeDv100;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

public class BisectingKMeansModelDataConverter extends SimpleModelDataConverter<BisectingKMeansModelData, BisectingKMeansModelData> {

	public BisectingKMeansModelDataConverter() {
	}

	@Override
	public Tuple2<Params, Iterable<String>> serializeModel(BisectingKMeansModelData modelData) {
		List <String> modelRows = new ArrayList <>();
		modelData.summaries.forEach((k, v) -> {
			v.clusterId = k;
			modelRows.add(gson.toJson(v, ClusterSummary.class));
		});
		Params meta = new Params().set(BisectingKMeansTrainParams.DISTANCE_TYPE, modelData.distanceType.name())
			.set(BisectingKMeansTrainParams.K, modelData.k)
			.set(HasVectorSizeDv100.VECTOR_SIZE, modelData.vectorSize)
			.set(BisectingKMeansTrainParams.VECTOR_COL, modelData.vectorColName);
		return Tuple2.of(meta, modelRows);
	}

	@Override
	public BisectingKMeansModelData deserializeModel(Params meta, Iterable<String> data) {
		BisectingKMeansModelData modelData = new BisectingKMeansModelData();
		modelData.k = meta.get(BisectingKMeansTrainParams.K);
		modelData.vectorSize = meta.get(HasVectorSizeDv100.VECTOR_SIZE);
		modelData.distanceType = DistanceType.valueOf(meta.get(BisectingKMeansTrainParams.DISTANCE_TYPE).toUpperCase());
		modelData.vectorColName = meta.get(BisectingKMeansTrainParams.VECTOR_COL);
		modelData.summaries = new HashMap <>();
		for (String c : data) {
			ClusterSummary summary = gson.fromJson(c, ClusterSummary.class);
			long clusterId = summary.clusterId;
			modelData.summaries.put(clusterId, summary);
		}
		return modelData;
	}
}
