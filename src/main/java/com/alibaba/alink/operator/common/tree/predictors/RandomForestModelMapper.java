package com.alibaba.alink.operator.common.tree.predictors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.LabelCounter;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.shared.tree.HasTreeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RandomForestModelMapper extends TreeModelMapper {
	private static final Logger LOG = LoggerFactory.getLogger(RandomForestModelMapper.class);

	public RandomForestModelMapper(
			TableSchema modelSchema,
			TableSchema dataSchema,
			Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void loadModel(List<Row> modelRows) {
		init(modelRows);
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
				Predict(transRow, root[i], labelCounter, 1.0);
			}

			labelCounter.normWithWeight();

			if (!Criteria.isRegression(treeModel.meta.get(HasTreeType.TREE_TYPE))) {
				detail = new HashMap<>();
				double[] probability = labelCounter.getDistributions();
				double max = 0.0;
				int maxIndex = -1;
				for (int i = 0; i < probability.length; ++i) {
					detail.put(String.valueOf(treeModel.labels[i]), probability[i]);
					if (max < probability[i]) {
						max = probability[i];
						maxIndex = i;
					}
				}

				if (maxIndex == -1) {
					LOG.warn("Can not find the probability: {}", JsonConverter.toJson(probability));
				}

				result = treeModel.labels[maxIndex];
			} else {
				result = labelCounter.getDistributions()[0];
			}
		}

		return new Tuple2<>(result, detail == null ? null : JsonConverter.toJson(detail));
	}
}
