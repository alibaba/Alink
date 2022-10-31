package com.alibaba.alink.operator.common.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;
import com.alibaba.alink.operator.common.tree.viz.TreeModelViz;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class TreeModelInfo implements Serializable {
	private static final long serialVersionUID = 316584854790096878L;
	final static ParamInfo <String> FEATURE_IMPORTANCE = ParamInfoFactory
		.createParamInfo("featureImportance", String.class)
		.build();

	public static class DecisionTreeModelInfo extends TreeModelInfo {
		private static final long serialVersionUID = -3670502627904480174L;

		public DecisionTreeModelInfo(List <Row> rows) {
			super(rows);
		}

		public String getCaseWhenRule() {
			AkPreconditions.checkArgument(
				dataConverter.roots.length == 1,
				"This is not a decision tree model. length: %d",
				dataConverter.roots.length
			);
			return getCaseWhenRuleFromTreeId(0);
		}

		public TreeModelInfo saveTreeAsImage(String path, boolean isOverwrite) throws IOException {
			AkPreconditions.checkArgument(
				dataConverter.roots.length == 1,
				"This is not a decision tree model. length: %d",
				dataConverter.roots.length
			);

			return saveTreeAsImageFromTreeId(path, 0, isOverwrite);
		}
	}

	static class MultiTreeModelInfo extends TreeModelInfo {

		private static final long serialVersionUID = -8437257628657305619L;

		public MultiTreeModelInfo(List <Row> rows) {
			super(rows);
		}

		public String getCaseWhenRule(int treeId) {
			return getCaseWhenRuleFromTreeId(treeId);
		}

		public TreeModelInfo saveTreeAsImage(String path, int treeId, boolean isOverwrite) throws IOException {
			return saveTreeAsImageFromTreeId(path, treeId, isOverwrite);
		}
	}

	public static final class RandomForestModelInfo extends MultiTreeModelInfo {

		private static final long serialVersionUID = -6423403615369604045L;

		public RandomForestModelInfo(List <Row> rows) {
			super(rows);
		}
	}

	public static final class GbdtModelInfo extends MultiTreeModelInfo {

		private static final long serialVersionUID = -859598180490206967L;

		public GbdtModelInfo(List <Row> rows) {
			super(rows);
		}
	}

	TreeModelDataConverter dataConverter;
	MultiStringIndexerModelData multiStringIndexerModelData;
	Map <String, Double> featureImportance;
	boolean isRegressionTree;

	public TreeModelInfo(List <Row> rows) {
		dataConverter = new TreeModelDataConverter().load(rows);

		if (dataConverter.stringIndexerModelSerialized != null) {
			multiStringIndexerModelData = new MultiStringIndexerModelDataConverter().load(
				dataConverter.stringIndexerModelSerialized
			);
		}

		if (dataConverter.meta.contains(FEATURE_IMPORTANCE)) {
			featureImportance = JsonConverter.fromJson(
				dataConverter.meta.get(FEATURE_IMPORTANCE),
				new TypeReference <Map <String, Double>>() {
				}.getType()
			);
		}

		isRegressionTree = isRegressionTree();
	}

	private boolean isRegressionTree() {
		if (dataConverter.meta.contains(LossUtils.LOSS_TYPE)
			|| dataConverter.meta.contains(BaseGbdtTrainBatchOp.ALGO_TYPE)) {
			return true;
		} else {
			return Criteria.isRegression(dataConverter.meta.get(TreeUtil.TREE_TYPE));
		}
	}

	protected String getCaseWhenRuleFromTreeId(int treeId) {
		AkPreconditions.checkArgument(
			treeId >= 0 && treeId < dataConverter.roots.length,
			"treeId should be in range [0, %d), treeId: %d",
			dataConverter.roots.length,
			treeId
		);

		if (getFeatures() != null) {
			StringBuilder sbd = new StringBuilder();
			appendNode(dataConverter.roots[treeId], getFeatures(), sbd);
			return sbd.toString();
		} else {
			return null;
		}
	}

	protected TreeModelInfo saveTreeAsImageFromTreeId(String path, int treeId, boolean isOverwrite) throws
		IOException {
		AkPreconditions.checkArgument(
			treeId >= 0 && treeId < dataConverter.roots.length,
			"treeId should be in range [0, %d), treeId: %d",
			dataConverter.roots.length,
			treeId
		);

		TreeModelViz.toImageFile(path, dataConverter, treeId, isOverwrite);

		return this;
	}

	public Map <String, Double> getFeatureImportance() {
		return featureImportance;
	}

	public int getNumTrees() {
		return dataConverter.meta.get(RandomForestTrainParams.NUM_TREES);
	}

	public String[] getFeatures() {
		if (dataConverter.meta.contains(RandomForestTrainParams.FEATURE_COLS)) {
			return dataConverter.meta.get(RandomForestTrainParams.FEATURE_COLS);
		} else {
			return null;
		}
	}

	public String[] getCategoricalFeatures() {
		if (dataConverter.meta.contains(RandomForestTrainParams.CATEGORICAL_COLS)) {
			return dataConverter.meta.get(RandomForestTrainParams.CATEGORICAL_COLS);
		} else {
			return null;
		}
	}

	public List <String> getCategoricalValues(String categoricalCol) {
		if (multiStringIndexerModelData != null) {
			return multiStringIndexerModelData.getTokens(categoricalCol);
		} else {
			return null;
		}
	}

	public Object[] getLabels() {
		return dataConverter.labels;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();

		if (isRegressionTree) {
			sbd.append("Regression trees modelInfo: \n");
		} else {
			sbd.append("Classification trees modelInfo: \n");
		}

		sbd.append("Number of trees: ")
			.append(getNumTrees())
			.append("\n");

		String[] categoricalCols = getCategoricalFeatures();

		if (getFeatures() != null) {
			sbd.append("Number of features: ")
				.append(getFeatures().length)
				.append("\n");

			sbd.append("Number of categorical features: ")
				.append(categoricalCols == null || categoricalCols.length == 0 ? 0 : categoricalCols.length)
				.append("\n");
		}

		if (getLabels() != null) {
			sbd.append("Labels: ");
			sbd.append(PrettyDisplayUtils.displayList(Arrays.asList(getLabels())));
			sbd.append("\n");
		}

		if (categoricalCols != null && categoricalCols.length > 0) {
			sbd.append("\nCategorical feature info:\n");

			Object[][] categoricalTable = new Object[categoricalCols.length][2];

			for (int i = 0; i < categoricalCols.length; ++i) {
				List <String> categoricalValues = getCategoricalValues(categoricalCols[i]);

				categoricalTable[i] = new Object[] {
					categoricalCols[i],
					categoricalValues == null ? 0 : categoricalValues.size()
				};
			}

			String[] categoricalColSummaryHeader = new String[] {"feature", "number of categorical value"};

			sbd.append(
				PrettyDisplayUtils.displayTable(
					categoricalTable, categoricalCols.length, 2, null, categoricalColSummaryHeader, null
				)
			);
		}

		if (getFeatureImportance() != null && !getFeatureImportance().isEmpty()) {
			Map <String, Double> featureImportance = getFeatureImportance();
			int topN = Math.min(featureImportance.size(), 50);

			sbd.append("\nTable of feature importance Top ").append(topN).append(": ").append("\n");

			Object[][] featureImportanceTable = new Object[featureImportance.size()][2];
			int index = 0;
			for (Map.Entry <String, Double> entry : featureImportance.entrySet()) {
				featureImportanceTable[index] = new Object[] {entry.getKey(), entry.getValue()};
				index++;
			}

			Arrays.sort(featureImportanceTable, (o1, o2) -> Double.compare((double) o2[1], (double) o1[1]));

			featureImportanceTable = ArrayUtils.subarray(featureImportanceTable, 0, topN);

			String[] featureImportanceTableHeader = new String[] {"feature", "importance"};

			sbd.append(
				PrettyDisplayUtils.displayTable(
					featureImportanceTable,
					featureImportanceTable.length,
					2, null, featureImportanceTableHeader, null,
					topN, 0, 2, false
				)
			);
		}

		return sbd.toString();
	}

	private void appendNode(Node root, String[] featureCols, StringBuilder sbd) {
		if (root.isLeaf()) {
			if (!isRegressionTree) {
				double max = 0.0;
				int maxIndex = -1;

				for (int j = 0; j < root.getCounter().getDistributions().length; ++j) {
					if (max < root.getCounter().getDistributions()[j]) {
						max = root.getCounter().getDistributions()[j];
						maxIndex = j;
					}
				}

				AkPreconditions.checkArgument(
					maxIndex >= 0,
					"Can not find the probability: {}",
					JsonConverter.toJson(root.getCounter().getDistributions())
				);

				sbd.append(dataConverter.labels[maxIndex]);
			} else {
				sbd.append(printEightDecimal(root.getCounter().getDistributions()[0]));
			}

			return;
		}

		if (root.getCategoricalSplit() != null) {
			boolean first = true;

			int[] categoricalSplit = root.getCategoricalSplit();

			int children = root.getNextNodes().length;

			for (int i = 0; i < children; ++i) {
				StringBuilder subSbd = new StringBuilder();
				if (first) {
					subSbd.append(" CASE WHEN ");
				} else {
					subSbd.append(" WHEN ");
				}

				first = false;

				boolean localFirst = true;

				for (int j = 0; j < root.getCategoricalSplit().length; ++j) {

					if (root.getCategoricalSplit()[j] == i) {
						if (!localFirst) {
							subSbd.append(" or ");
						}

						subSbd.append(featureCols[root.getFeatureIndex()]);
						subSbd.append(" = ");
						subSbd.append(
							multiStringIndexerModelData
								.getToken(featureCols[root.getFeatureIndex()], (long) j)
						);

						localFirst = false;
					}
				}

				subSbd.append(" THEN ");
				appendNode(root.getNextNodes()[i], featureCols, subSbd);
				sbd.append(subSbd);
			}
			sbd.append(" END");
		} else {
			StringBuilder subSbd = new StringBuilder();
			subSbd.append("CASE WHEN ");
			subSbd.append(featureCols[root.getFeatureIndex()]);
			subSbd.append(" <= ");
			subSbd.append(printEightDecimal(root.getContinuousSplit()));
			subSbd.append(" THEN ");
			appendNode(root.getNextNodes()[0], featureCols, subSbd);
			sbd.append(subSbd);

			subSbd = new StringBuilder();
			subSbd.append(" WHEN ");
			subSbd.append(featureCols[root.getFeatureIndex()]);
			subSbd.append(" > ");
			subSbd.append(printEightDecimal(root.getContinuousSplit()));
			subSbd.append(" THEN ");
			appendNode(root.getNextNodes()[1], featureCols, subSbd);
			sbd.append(subSbd);
			sbd.append(" END");
		}
	}

	private static String printEightDecimal(double val) {
		return val == Math.floor(val) && !Double.isInfinite(val) ?
			String.format("%d", (int) val) :
			(new BigDecimal(val).setScale(8, BigDecimal.ROUND_HALF_UP).doubleValue() == val ?
				String.valueOf(val) : String.format("%.8f", val));
	}
}
