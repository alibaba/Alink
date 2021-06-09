package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Summary of MultiHotModel.
 */
public class MultiHotModelInfo implements Serializable {
	private static final long serialVersionUID = -4990829552802168917L;

	public Map <String, List<String>> tokensMap;
	public String delimiter;
	public Map <String, Integer> distinceTokenNumber;

	public MultiHotModelInfo(List <Row> rows) {
		MultiHotModelData multiHotModelData = new MultiHotModelDataConverter().load(rows);
		delimiter = multiHotModelData.delimiter;
		distinceTokenNumber = new HashMap <>(multiHotModelData.modelData.size());
		tokensMap = new HashMap <>(multiHotModelData.modelData.size());
		for (String key : multiHotModelData.modelData.keySet()) {
			Map<String, Tuple2<Integer, Integer>> map = multiHotModelData.modelData.get(key);
			distinceTokenNumber.put(key, map.size());
			List<String> tokens = new ArrayList <>(map.size());
			for (String str : map.keySet()) {
				tokens.add(str);
			}
			tokensMap.put(key, tokens);
		}
	}

	public String[] getSelectedColsInModel() {
		return tokensMap.keySet().toArray(new String[0]);
	}

	public int getDistinctTokenNumber(String columnName) {
		Preconditions.checkState(tokensMap.containsKey(columnName),
			columnName + "is not contained in the model!");
		return distinceTokenNumber.get(columnName);
	}

	public String[] getTokens(String columnName) {
		Preconditions.checkState(tokensMap.containsKey(columnName),
			columnName + "is not contained in the model!");
		return tokensMap.get(columnName).toArray(new String[0]);
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline("MultiHotModelInfo", '-'));
		sbd.append("MultiHotEncoder on ")
			.append(tokensMap.size())
			.append(" features: ")
			.append(PrettyDisplayUtils.displayList(new ArrayList <>(tokensMap.keySet()), 3, false))
			.append("\n")
			.append(" delimiter: ")
			.append(delimiter)
			.append("\n")
			.append(mapToString(distinceTokenNumber))
			.append(QuantileDiscretizerModelInfo.mapToString(tokensMap));

		return sbd.toString();
	}

	static String mapToString(Map <String, Integer> categorySize) {
		StringBuilder sbd = new StringBuilder();
		sbd.append(PrettyDisplayUtils.displayHeadline("DistinctTokenNumber", '='));
		sbd.append(PrettyDisplayUtils.displayMap(categorySize, 3, false));
		sbd.append("\n");
		return sbd.toString();
	}
}
