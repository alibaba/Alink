package com.alibaba.alink.operator.batch.feature;

import java.util.*;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.OneHotModelData;
import com.alibaba.alink.operator.common.feature.OneHotModelDataConverter;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.feature.OneHotTrainParams;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * One-hot maps a serial of columns of category indices to a column of
 * sparse binary vector. It will produce a model of one hot, and then it can transform
 * data to binary format using this model.
 */
public final class OneHotTrainBatchOp extends BatchOperator<OneHotTrainBatchOp>
	implements OneHotTrainParams <OneHotTrainBatchOp> {

	public final static String DELIMITER = "@ # %";
	private final static String NULL_VALUE = "null";

	/**
	 * null constructor.
	 */
	public OneHotTrainBatchOp() {
		super(null);
	}

	/**
	 * constructor.
	 *
	 * @param params the parameters set.
	 */
	public OneHotTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public OneHotTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
		BatchOperator<?> in = checkAndGetFirst(inputs);
		// encoding columns names
		String[] selectedColNames = getSelectedCols();

		// the type to processing the NULL values
		boolean ignoreNull = getIgnoreNull();

		// drop the last coding value for nonlinear of kv vectors
		boolean dropLast = getDropLast();

		String[] colNames = in.getColNames();
		int[] idx = new int[selectedColNames.length];
		for (int i = 0; i < selectedColNames.length; ++i) {
			idx[i] = TableUtil.findColIndex(colNames, selectedColNames[i]);
		}

		DataSet <Row> mapping = in.getDataSet()
			.mapPartition(new ParseItem(selectedColNames, idx))
			.reduceGroup(new ReduceItem(dropLast, ignoreNull, selectedColNames))
			.setParallelism(1);

		this.setOutput(mapping, new OneHotModelDataConverter().getModelSchema());
		return this;
	}

	/**
	 * reduce all the items and then build model.
	 */
	public static class ReduceItem implements GroupReduceFunction<Row, Row> {
		private boolean dropLast;
		private boolean ignoreNull;
		private String[] selectedColNames;

		public ReduceItem(boolean dropLast, boolean ignoreNull, String[] selectedColNames) {
			this.dropLast = dropLast;
			this.ignoreNull = ignoreNull;
			this.selectedColNames = selectedColNames;
		}

		@Override
		public void reduce(Iterable<Row> rows, Collector<Row> collector) throws Exception {
			Map <String, HashSet <String>> map = new LinkedHashMap <>(0);
			for (Row row : rows) {
				String colName = row.getField(0).toString();
				String value = row.getField(1).toString();
				if (map.containsKey(colName)) {
					map.get(colName).add(value);
				} else {
					HashSet <String> set = new HashSet <>();
					set.add(value);
					map.put(colName, set);
				}
			}

			// data for model.
			// construct model data.
			ArrayList <String> data = new ArrayList <>();
			int mapIteration = 0;
			if (ignoreNull) {
				for (String selectedColName : selectedColNames) {
					HashSet<String> cate = map.get(selectedColName);
					for (String val : cate) {
						if (val.equalsIgnoreCase(NULL_VALUE)) {
							continue;
						}
						String tmp = selectedColName + DELIMITER
								+ val + DELIMITER
								+ mapIteration++;
						data.add(tmp);
					}
				}
				if (dropLast) {
					if (!(data.get(data.size() - 1).equalsIgnoreCase(NULL_VALUE))) {
						data.remove(data.size() - 1);
						mapIteration--;
					}
				}
			} else {
				for (String selectedColName : selectedColNames) {
					HashSet<String> cate = map.get(selectedColName);
					for (String val : cate) {
						String tmp = selectedColName + DELIMITER
								+ val + DELIMITER
								+ mapIteration++;
						data.add(tmp);
					}
				}
				if (dropLast) {
					data.remove(data.size() - 1);
					mapIteration--;
				}
			}

			// save model.
			OneHotModelData model = new OneHotModelData();
			model.data = data;
			model.meta.set(ModelParamName.VECTOR_SIZE, mapIteration + 1);

			new OneHotModelDataConverter().save(model, collector);
		}
	}

	/**
	 * Count all the possible values of each column and then keep them.
	 */
	public static class ParseItem implements MapPartitionFunction <Row, Row> {
		private String[] binaryColNames;
		private int[] idx;

		ParseItem(String[] binaryColNames, int[] idx) {
			this.binaryColNames = binaryColNames;
			this.idx = idx;
		}

		@Override
		public void mapPartition(Iterable <Row> rows, Collector <Row> collector) throws Exception {
			Map <String, HashSet <String>> map = new HashMap<>(0);
			int m = this.binaryColNames.length;

			for (Row row : rows) {
				for (int i = 0; i < m; i++) {
					String colName = this.binaryColNames[i];
					Object obj = row.getField(idx[i]);
					String value = (obj == null) ? "null" : obj.toString();
					if (map.containsKey(colName)) {
						HashSet <String> set = map.get(colName);
						set.add(value);
					} else {
						HashSet <String> set = new HashSet <>();
						set.add(value);
						map.put(colName, set);
					}
				}
			}
			//for each value of one column, keep it and its column value in a row.
			for (Map.Entry <String, HashSet <String>> entry : map.entrySet()) {
				String name = entry.getKey();
				HashSet <String> values = entry.getValue();
				for (String value : values) {
					Row r = new Row(2);
					r.setField(0, name);
					r.setField(1, value);
					collector.collect(r);
				}
			}
		}
	}
}