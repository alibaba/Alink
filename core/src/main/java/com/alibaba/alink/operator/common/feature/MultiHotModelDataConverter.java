package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.feature.MultiHotTrainParams;
import com.alibaba.alink.params.shared.HasSize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ModelDataConverter for MultiHot.
 */
public class MultiHotModelDataConverter extends SimpleModelDataConverter <MultiHotModelData, MultiHotModelData> {
	public MultiHotModelDataConverter() {
	}

	@Override
	public Tuple2 <Params, Iterable <String>> serializeModel(MultiHotModelData data) {
		List <String> modelStrList = new ArrayList <>(data.modelData.size());
		for (String key : data.modelData.keySet()) {
			Map <String, Tuple2 < Integer, Integer>> map = data.modelData.get(key);
			for (String innerKey : map.keySet()) {
				Tuple2 < Integer, Integer> t3 = map.get(innerKey);
				Object[] objects = new Object[]{key, innerKey, t3.f0, t3.f1};
				modelStrList.add(JsonConverter.toJson(objects));
			}
		}
		Params params = new Params().set(MultiHotTrainParams.DELIMITER, data.delimiter);
		return Tuple2.of(params, modelStrList);
	}

	@Override
	public MultiHotModelData deserializeModel(Params meta, Iterable <String> modelData) {
		MultiHotModelData multiHotModelData = new MultiHotModelData();
		multiHotModelData.delimiter = meta.get(MultiHotTrainParams.DELIMITER);
		multiHotModelData.modelData = new HashMap <>(1);
		for (String str : modelData) {
			Object[] objects = JsonConverter.fromJson(str, Object[].class);
			String key = (String) objects[0];
			String innerKey = (String) objects[1];
			int lIndex = (int) objects[2];
			int numAcc = (int) objects[3];
			if (multiHotModelData.modelData.containsKey(key)) {
				multiHotModelData.modelData.get(key).put(innerKey, Tuple2.of(lIndex, numAcc));
			} else {
				Map <String, Tuple2 <Integer, Integer>> map = new HashMap <>(1);
				map.put(innerKey, Tuple2.of(lIndex, numAcc));
				multiHotModelData.modelData.put(key, map);
			}
		}
		return multiHotModelData;
	}
}
