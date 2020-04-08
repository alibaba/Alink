package com.alibaba.alink.operator.batch.feature;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.StringIndexerUtil;
import com.alibaba.alink.operator.common.feature.OneHotModelDataConverter;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.dataproc.HasSelectedColTypes;
import com.alibaba.alink.params.feature.HasEnableElse;
import com.alibaba.alink.params.feature.OneHotTrainParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * One-hot maps a serial of columns of category indices to a column of sparse binary vector. It will produce a model of
 * one hot, and then it can transform data to binary format using this model.
 */
public final class OneHotTrainBatchOp extends BatchOperator<OneHotTrainBatchOp>
	implements OneHotTrainParams<OneHotTrainBatchOp> {

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

		final String[] selectedColNames = getSelectedCols();
		final String[] selectedColSqlType = new String[selectedColNames.length];
		for (int i = 0; i < selectedColNames.length; i++) {
			selectedColSqlType[i] = FlinkTypeConverter.getTypeString(
				TableUtil.findColTypeWithAssertAndHint(in.getSchema(), selectedColNames[i]));
		}
		int[] thresholdArray;

		if (!getParams().contains(OneHotTrainParams.DISCRETE_THRESHOLDS_ARRAY)) {
			thresholdArray = new int[selectedColNames.length];
			Arrays.fill(thresholdArray, getDiscreteThresholds());
		} else {
			thresholdArray = Arrays.stream(getDiscreteThresholdsArray()).mapToInt(Integer::intValue).toArray();
		}

		boolean enableElse = isEnableElse(thresholdArray);

		DataSet<Row> inputRows = in.select(selectedColNames).getDataSet();
		DataSet<Tuple3<Integer, String, Long>> countTokens = StringIndexerUtil.countTokens(inputRows, true)
			.filter(new FilterFunction<Tuple3<Integer, String, Long>>() {
				@Override
				public boolean filter(Tuple3<Integer, String, Long> value) {
					return value.f2 >= thresholdArray[value.f0];
				}
			});

		DataSet<Tuple3<Integer, String, Long>> indexedToken = StringIndexerUtil
			.zipWithIndexPerColumn((DataSet)countTokens.project(0, 1))
			.project(1, 2, 0);

		DataSet<Row> values = indexedToken
			.mapPartition(new RichMapPartitionFunction<Tuple3<Integer, String, Long>, Row>() {
				@Override
				public void mapPartition(Iterable<Tuple3<Integer, String, Long>> values, Collector<Row> out)
					throws Exception {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						meta = new Params().set(HasSelectedCols.SELECTED_COLS, selectedColNames)
							.set(HasSelectedColTypes.SELECTED_COL_TYPES, selectedColSqlType)
							.set(HasEnableElse.ENABLE_ELSE, enableElse);
					}
					new OneHotModelDataConverter().save(Tuple2.of(meta, values), out);
				}
			})
			.name("build_model");

		this.setOutput(values, new OneHotModelDataConverter().getModelSchema());
		return this;
	}

	/**
	 * If the thresholdArray is set and greater than 0, enableElse is true.
	 *
	 * @param thresholdArray thresholds for each column.
	 * @return enableElse.
	 */
	private static boolean isEnableElse(int[] thresholdArray) {
		for (int threshold : thresholdArray) {
			if (threshold > 0) {
				return true;
			}
		}
		return false;
	}
}