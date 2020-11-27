package com.alibaba.alink.operator.common.tree.predictors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.tree.LabelCounter;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp;
import com.alibaba.alink.operator.common.tree.parallelcart.SaveModel;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GbdtModelMapper extends TreeModelMapper {
	private static final long serialVersionUID = 75264909895533116L;
	private double period;
	private boolean isClassification;

	private int vectorColIndex = -1;

	public GbdtModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);

		if (params.contains(GbdtTrainParams.VECTOR_COL)) {
			vectorColIndex = TableUtil.findColIndexWithAssertAndHint(dataSchema,
				params.get(HasVectorColDefaultAsNull.VECTOR_COL));
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		init(modelRows);
		period = treeModel.meta.get(SaveModel.GBDT_Y_PERIOD);

		if (treeModel.meta.contains(LossUtils.LOSS_TYPE)) {
			isClassification = LossUtils.isClassification(
				treeModel.meta.get(LossUtils.LOSS_TYPE)
			);
		} else {
			// ALGO_TYPE is compatible with old version.
			isClassification = treeModel.meta.get(BaseGbdtTrainBatchOp.ALGO_TYPE) == 1;
		}

	}

	@Override
	protected Object predictResult(Row row) throws Exception {
		return predictResultDetail(row).f0;
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(Row row) throws Exception {
		Tuple2 <Object, Map <String, Double>> result;

		if (vectorColIndex >= 0) {
			result = predictResultDetailVector(treeModel.roots, VectorUtil.getVector(row.getField(vectorColIndex)));
		} else {
			result = predictResultDetailTable(treeModel.roots, row);
		}

		return new Tuple2 <>(result.f0, result.f1 == null ? null : JsonConverter.toJson(result.f1));
	}

	private void predictVector(Vector vector, Node node, LabelCounter result, double weight) {
		if (node.isLeaf()) {
			result.add(node.getCounter(), weight);
			return;
		}

		double val = vector.get(node.getFeatureIndex());

		if (Preprocessing.isMissing(val, zeroAsMissing)) {
			if (node.getMissingSplit() == null || node.getMissingSplit().length != 1) {
				throw new IllegalArgumentException("When the value is missing, there must be missing split.");
			}

			predictVector(vector, node.getNextNodes()[node.getMissingSplit()[0]], result, weight);
		} else {
			if (node.getCategoricalSplit() == null) {
				if (val <= node.getContinuousSplit()) {
					//left
					predictVector(vector, node.getNextNodes()[0], result, weight);
				} else {
					//right
					predictVector(vector, node.getNextNodes()[1], result, weight);
				}
			} else {
				throw new IllegalStateException("Unsupported categorical feature now.");
			}
		}
	}

	private Tuple2 <Object, Map <String, Double>> predictResultDetailWithLabelCounter(LabelCounter labelCounter) {
		Object result = null;
		Map <String, Double> detail = null;

		if (isClassification) {
			//no need to add "period" for classification

			double p = 1.0 / (1.0 + Math.exp(-(labelCounter.getDistributions()[0] + period)));

			if (p >= 0.5) {
				result = treeModel.labels[1];
			} else {
				result = treeModel.labels[0];
			}

			detail = new HashMap <>();
			detail.put(treeModel.labels[0].toString(), 1.0 - p);
			detail.put(treeModel.labels[1].toString(), p);
		} else {
			result = labelCounter.getDistributions()[0] + period;
		}

		return Tuple2.of(result, detail);
	}

	private Tuple2 <Object, Map <String, Double>> predictResultDetailVector(Node[] root, Vector vector) {
		int len = root.length;

		Object result = null;
		Map <String, Double> detail = null;

		if (len > 0) {
			LabelCounter labelCounter = new LabelCounter(
				0, 0, new double[root[0].getCounter().getDistributions().length]);

			predictVector(vector, root[0], labelCounter, 1.0);

			for (int i = 1; i < len; ++i) {
				if (root[i] != null) {
					predictVector(vector, root[i], labelCounter, 1.0);
				}
			}

			return predictResultDetailWithLabelCounter(labelCounter);
		}

		return Tuple2.of(result, detail);
	}

	private Tuple2 <Object, Map <String, Double>> predictResultDetailTable(Node[] root, Row row) throws Exception {
		Row transRow = Row.copy(row);

		transRow = transRow(transRow);

		int len = root.length;

		Object result = null;
		Map <String, Double> detail = null;

		if (len > 0) {
			LabelCounter labelCounter = new LabelCounter(
				0, 0, new double[root[0].getCounter().getDistributions().length]);

			predict(transRow, root[0], labelCounter, 1.0);

			for (int i = 1; i < len; ++i) {
				if (root[i] != null) {
					predict(transRow, root[i], labelCounter, 1.0);
				}
			}

			return predictResultDetailWithLabelCounter(labelCounter);
		}

		return Tuple2.of(result, detail);
	}
}
