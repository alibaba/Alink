package com.alibaba.alink.operator.common.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.JsonConverter;

import java.util.HashMap;

public class TargetEncoderModelData {
	private HashMap <String, HashMap<String, Double>> modelData;

	public TargetEncoderModelData() {
		modelData = new HashMap <>();
	}

	public HashMap<String, Double> getData(String key) {
		return modelData.get(key);
	}

	public void setData(Row data) {
		String key = (String) data.getField(0);
		String strMap = (String) data.getField(1);
		this.modelData.put(key, JsonConverter.fromJson(strMap, HashMap.class));
	}

}
