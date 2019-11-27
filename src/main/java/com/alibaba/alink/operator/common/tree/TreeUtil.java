package com.alibaba.alink.operator.common.tree;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TreeUtil {
	public static Map<String, Integer> extractCategoricalColsSize(
		List<Row> stringIndexerModelData, String[] lookUpColNames) {
		MultiStringIndexerModelData stringIndexerModel
			= new MultiStringIndexerModelDataConverter().load(stringIndexerModelData);

		Map<String, Integer> categoricalColsSize = new HashMap<>();

		for (String lookUpColName : lookUpColNames) {
			categoricalColsSize.put(lookUpColName, (int) stringIndexerModel.getNumberOfTokensOfColumn(lookUpColName));
		}

		return categoricalColsSize;
	}

	public static FeatureMeta[] getFeatureMeta(
		String[] featureColNames,
		Map<String, Integer> categoricalSizes) {
		FeatureMeta[] featureMetas = new FeatureMeta[featureColNames.length];
		for (int i = 0; i < featureColNames.length; ++i) {
			if (categoricalSizes.containsKey(featureColNames[i])) {
				featureMetas[i] = new FeatureMeta(featureColNames[i], i, categoricalSizes.get(featureColNames[i]));
			} else {
				featureMetas[i] = new FeatureMeta(featureColNames[i], i);
			}
		}

		return featureMetas;
	}

	public static FeatureMeta getLabelMeta(
		String labelColName, int labelColIndex, Map<String, Integer> categoricalSizes) {
		if (categoricalSizes.containsKey(labelColName)) {
			return new FeatureMeta(labelColName, labelColIndex, categoricalSizes.get(labelColName));
		} else {
			return new FeatureMeta(labelColName, labelColIndex);
		}
	}

	public static String[] trainColNames(Params params) {
		ArrayList<String> colNames = new ArrayList<>(
			Arrays.asList(params.get(HasFeatureCols.FEATURE_COLS))
		);

		if (params.contains(HasLabelCol.LABEL_COL)) {
			colNames.add(params.get(HasLabelCol.LABEL_COL));
		}

		if (params.get(HasWeightColDefaultAsNull.WEIGHT_COL) != null) {
			colNames.add(params.get(HasWeightColDefaultAsNull.WEIGHT_COL));
		}

		return colNames.toArray(new String[0]);
	}
}
