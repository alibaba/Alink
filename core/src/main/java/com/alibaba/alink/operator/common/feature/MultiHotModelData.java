package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Map;

/**
 * Multi-hot model data.
 */
public class MultiHotModelData implements Serializable {
	private static final long serialVersionUID = -2535262865832754665L;
	public Map <String, Map <String, Tuple2 <Integer, Integer>>> modelData;
	public String delimiter;

	public MultiHotModelData() {
	}

	public MultiHotModelData(Map <String, Map <String, Tuple2 <Integer, Integer>>> modelData, String delimiter) {
		this.modelData = modelData;
		this.delimiter = delimiter;
	}

	public boolean getEnableElse(String[] key) {
		boolean enableElse = false;
		for (int i = 0; i < key.length; ++i) {
			Map <String, Tuple2 <Integer, Integer>> map = modelData.get(key[i]);
			for (String innerKey : map.keySet()) {
				Tuple2 <Integer, Integer> t2 = map.get(innerKey);
				if (t2.f0 == -1) {
					enableElse = true;
					break;
				}
			}
			if (enableElse) {
				break;
			}
		}
		return enableElse;
	}
}
