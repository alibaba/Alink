package com.alibaba.alink.operator.common.tree.predictors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.LabelCounter;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.parallelcart.SaveModel;
import com.alibaba.alink.common.utils.JsonConverter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.tree.BaseGbdtTrainBatchOp.ALGO_TYPE;

public class GbdtModelMapper extends TreeModelMapper {
	private double period;
	private int algoType;

	public GbdtModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void loadModel(List<Row> modelRows) {
		init(modelRows);
		period = treeModel.meta.get(SaveModel.GBDT_Y_PERIOD);
		algoType = treeModel.meta.get(ALGO_TYPE);
	}

	@Override
	protected Object predictResult(Row row) throws Exception {
		return predictResultDetail(row).f0;
	}

	@Override
	protected Tuple2<Object, String> predictResultDetail(Row row) throws Exception {
		Node[] root = treeModel.roots;

		Row transRow = Row.copy(row);

		transRow = transRow(transRow);

		int len = root.length;

		Object result = null;
		Map<String, Double> detail = null;

		if (len > 0) {
			LabelCounter labelCounter = new LabelCounter(
				0, 0, new double[root[0].getCounter().getDistributions().length]);

			Predict(transRow, root[0], labelCounter, 1.0);

			for (int i = 1; i < len; ++i) {
				if (root[i] != null) {
					Predict(transRow, root[i], labelCounter, 1.0);
				}
			}

			if (algoType == 1) {
				//no need to add "period" for classification

				double p = 1.0 / (1.0 + Math.exp(-labelCounter.getDistributions()[0]));

				if (p >= 0.5) {
					result = treeModel.labels[1];
				} else {
					result = treeModel.labels[0];
				}

				detail = new HashMap<>();
				detail.put(treeModel.labels[0].toString(), 1.0 - p);
				detail.put(treeModel.labels[1].toString(), p);
			} else {
				result = labelCounter.getDistributions()[0] + period;
			}
		}

		return new Tuple2<>(result, detail == null ? null : JsonConverter.gson.toJson(detail));
	}
}
