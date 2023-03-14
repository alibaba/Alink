package com.alibaba.alink.operator.common.clustering.kmodes;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.operator.common.clustering.ClusterSummary;
import com.alibaba.alink.operator.common.distance.OneZeroDistance;
import com.alibaba.alink.params.clustering.KModesTrainParams;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * @author guotao.gt
 */
@NameCn("Kmodes模型")
public class KModesModel extends SimpleModelDataConverter <KModesModelData, KModesModelData> {
	public KModesModel() {
	}

	public static long findCluster(Iterable <Tuple3 <Long, Double, String[]>> centroids, String[] sample,
								   OneZeroDistance oneZeroDistance) {
		long clusterId = -1;
		double d = Double.POSITIVE_INFINITY;

		for (Tuple3 <Long, Double, String[]> c : centroids) {
			double distance = oneZeroDistance.calc(sample, c.f2);
			if (distance < d) {
				clusterId = c.f0;
				d = distance;
			}
		}
		return clusterId;
	}

	@Override
	public Tuple2 <Params, Iterable <String>> serializeModel(KModesModelData modelData) {
		List <String> modelRows = new ArrayList <>();
		for (Tuple3 <Long, Double, String[]> centroid : modelData.centroids) {
			ClusterSummary c = new ClusterSummary(gson.toJson(centroid.f2), centroid.f0, centroid.f1);
			modelRows.add(c.serialize());
		}
		Params meta = new Params()
			.set(KModesTrainParams.FEATURE_COLS, modelData.featureColNames);
		return Tuple2.of(meta, modelRows);
	}

	@Override
	public KModesModelData deserializeModel(Params meta, Iterable <String> rows) {
		KModesModelData modelData = new KModesModelData();
		modelData.centroids = new ArrayList <>();
		modelData.featureColNames = meta.get(KModesTrainParams.FEATURE_COLS);

		for (String row : rows) {
			ClusterSummary c = ClusterSummary.deserialize(row);
			Tuple3 <Long, Double, String[]> centroid =
				new Tuple3 <>(c.clusterId, c.weight,
					gson.fromJson(c.center, String[].class));
			modelData.centroids.add(centroid);
		}
		return modelData;
	}
}
