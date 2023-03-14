package com.alibaba.alink.operator.common.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
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
	public static Map <String, Integer> extractCategoricalColsSize(
		List <Row> stringIndexerModelData, String[] lookUpColNames) {
		MultiStringIndexerModelData stringIndexerModel
			= new MultiStringIndexerModelDataConverter().load(stringIndexerModelData);
		return extractCategoricalColsSize(stringIndexerModel, lookUpColNames);
	}

	public static Map <String, Integer> extractCategoricalColsSize(
		MultiStringIndexerModelData stringIndexerModel, String[] lookUpColNames) {
		Map <String, Integer> categoricalColsSize = new HashMap <>();

		for (String lookUpColName : lookUpColNames) {
			categoricalColsSize.put(lookUpColName, (int) stringIndexerModel.getNumberOfTokensOfColumn(lookUpColName));
		}

		return categoricalColsSize;
	}

	public static FeatureMeta[] getFeatureMeta(
		String[] featureColNames,
		Map <String, Integer> categoricalSizes) {
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
		String labelColName, int labelColIndex, Map <String, Integer> categoricalSizes) {
		if (categoricalSizes.containsKey(labelColName)) {
			return new FeatureMeta(labelColName, labelColIndex, categoricalSizes.get(labelColName));
		} else {
			return new FeatureMeta(labelColName, labelColIndex);
		}
	}

	public static String[] trainColNames(Params params, String[] featureCols) {
		ArrayList <String> colNames = new ArrayList <>(
			Arrays.asList(featureCols)
		);

		if (params.contains(HasLabelCol.LABEL_COL)) {
			colNames.add(params.get(HasLabelCol.LABEL_COL));
		}

		if (params.get(HasWeightColDefaultAsNull.WEIGHT_COL) != null) {
			colNames.add(params.get(HasWeightColDefaultAsNull.WEIGHT_COL));
		}

		return colNames.toArray(new String[0]);
	}

	public static ParamInfo <TreeType> TREE_TYPE = ParamInfoFactory
		.createParamInfo("treeType", TreeUtil.TreeType.class)
		.setDescription("The criteria of the tree. " +
			"There are three options: \"AVG\", \"partition\" or \"gini(infoGain, infoGainRatio, mse)\""
		)
		.setHasDefaultValue(TreeUtil.TreeType.AVG)
		.build();

	/**
	 * Indicate that the criteria using in the tree.
	 */
	public enum TreeType {
		/**
		 * Ave Partition the tree as hybrid.
		 */
		AVG,

		/**
		 * Partition the tree as hybrid.
		 */
		PARTITION,

		/**
		 * Gini index. ref: cart.
		 */
		GINI,

		/**
		 * Info gain. ref: id3
		 */
		INFOGAIN,

		/**
		 * Info gain ratio. ref: c4.5
		 */
		INFOGAINRATIO,

		/**
		 * mse. ref: cart regression.
		 */
		MSE
	}
}
