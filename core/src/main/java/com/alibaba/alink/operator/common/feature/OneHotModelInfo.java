package com.alibaba.alink.operator.common.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Summary of OneHotModel.
 */
public class OneHotModelInfo implements Serializable {
	private static final long serialVersionUID = -4990829552802168917L;
	public Map <String, List <String>> tokensMap;
	public Map <String, Long> distinceTokenNumber;

	public OneHotModelInfo(List <Row> rows) {
		OneHotModelData modelData = new OneHotModelDataConverter().load(rows);
		tokensMap = new HashMap <>();
		distinceTokenNumber = new HashMap <>();
		String[] colNames = modelData.modelData.meta.get(HasSelectedCols.SELECTED_COLS);
		for (String s : colNames) {
			tokensMap.put(s, modelData.modelData.getTokens(s));
			distinceTokenNumber.put(s, modelData.modelData.getNumberOfTokensOfColumn(s));
		}
	}

	public String[] getSelectedColsInModel() {
		return tokensMap.keySet().toArray(new String[0]);
	}

	public Long getDistinctTokenNumber(String columnName) {
		AkPreconditions.checkState(tokensMap.containsKey(columnName),
			columnName + "is not contained in the model!");
		return distinceTokenNumber.get(columnName);
	}

	public String[] getTokens(String columnName) {
		AkPreconditions.checkState(tokensMap.containsKey(columnName),
			columnName + "is not contained in the model!");
		return tokensMap.get(columnName).toArray(new String[0]);
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline("OneHotModelInfo", '-'));
		sbd.append("OneHotEncoder on ")
			.append(tokensMap.size())
			.append(" features: ")
			.append(PrettyDisplayUtils.displayList(new ArrayList <>(distinceTokenNumber.keySet()), 3, false))
			.append("\n")
			.append(mapToString(distinceTokenNumber))
			.append(QuantileDiscretizerModelInfo.mapToString(tokensMap));
		return sbd.toString();
	}

	static String mapToString(Map <String, Long> categorySize) {
		StringBuilder sbd = new StringBuilder();
		sbd.append(PrettyDisplayUtils.displayHeadline("DistinctTokenNumber", '='));
		sbd.append(PrettyDisplayUtils.displayMap(categorySize, 3, false));
		sbd.append("\n");
		return sbd.toString();
	}
}
