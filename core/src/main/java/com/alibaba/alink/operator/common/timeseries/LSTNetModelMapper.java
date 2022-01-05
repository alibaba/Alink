package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.operator.common.tensorflow.TFTableModelPredictModelMapper;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class LSTNetModelMapper extends TimeSeriesModelMapper {

	private static final String[] TF_MODEL_MAPPER_INPUT_COL_NAMES
		= new String[] {"agg_to_tensor_tensor_col_internal_impl"};
	private static final TypeInformation <?>[] TF_MODEL_MAPPER_INPUT_COL_TYPES
		= new TypeInformation <?>[] {TensorTypes.FLOAT_TENSOR};

	private static Params createTfModelMapperParams() {
		return new Params()
			.set(TFTableModelPredictParams.SELECTED_COLS, TF_MODEL_MAPPER_INPUT_COL_NAMES)
			.set(TFTableModelPredictParams.SIGNATURE_DEF_KEY, "serving_default")
			.set(TFTableModelPredictParams.INPUT_SIGNATURE_DEFS, new String[] {"tensor"})
			.set(TFTableModelPredictParams.OUTPUT_SCHEMA_STR, "pred FLOAT_TENSOR")
			.set(TFTableModelPredictParams.OUTPUT_SIGNATURE_DEFS, new String[] {"add"})
			.set(TFTableModelPredictParams.RESERVED_COLS, new String[] {});
	}

	private final TFTableModelPredictModelMapper tfTableModelPredictModelMapper;

	public LSTNetModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);

		this.tfTableModelPredictModelMapper = new TFTableModelPredictModelMapper(
			modelSchema,
			new TableSchema(
				TF_MODEL_MAPPER_INPUT_COL_NAMES,
				TF_MODEL_MAPPER_INPUT_COL_TYPES
			),
			createTfModelMapperParams(),
			new TFPredictorClassLoaderFactory()
		);
	}

	@Override
	protected Tuple2 <double[], String> predictSingleVar(
		Timestamp[] historyTimes, double[] historyVals, int predictNum) {

		Tuple2 <Timestamp[], FloatTensor> t = toTensor(historyTimes, historyVals);

		FloatTensor pred = null;

		try {
			pred = (FloatTensor) tfTableModelPredictModelMapper.map(Row.of(t.f1)).getField(0);
		} catch (Exception ex) {
			// pass
		}

		return pred == null ? Tuple2.of(null, null) : Tuple2.of(new double[] {pred.getFloat(0)}, null);
	}

	@Override
	protected Tuple2 <Vector[], String> predictMultiVar(Timestamp[] historyTimes, Vector[] historyVals,
														int predictNum) {
		Tuple2 <Timestamp[], FloatTensor> t = toTensor(historyTimes, historyVals);

		FloatTensor pred = null;

		try {
			pred = (FloatTensor) tfTableModelPredictModelMapper.map(Row.of(t.f1)).getField(0);
		} catch (Exception ex) {
			// pass
		}

		return pred == null ? Tuple2.of(null, null) : Tuple2.of(new Vector[] {DoubleTensor.of(pred).toVector()}, null);
	}

	@Override
	public void open() {
		super.open();
		tfTableModelPredictModelMapper.open();
	}

	@Override
	public void close() {
		tfTableModelPredictModelMapper.close();
		super.close();
	}

	public void loadModel(List <Row> modelRows) {
		tfTableModelPredictModelMapper.loadModel(modelRows);
	}

	private static Tuple2 <Timestamp[], FloatTensor> toTensor(
		Timestamp[] historyTimes, double[] historyVals) {

		return Tuple2.of(
			historyTimes,
			Tensor.stack(
				Arrays
					.stream(historyVals)
					.mapToObj(value -> new FloatTensor(new float[] {(float) value}))
					.toArray(FloatTensor[]::new),
				0, null
			)
		);
	}

	private static Tuple2 <Timestamp[], FloatTensor> toTensor(
		Timestamp[] historyTimes, Vector[] historyVals) {

		return Tuple2.of(
			historyTimes,
			Tensor.stack(
				Arrays
					.stream(historyVals)
					.map(value -> FloatTensor.of(TensorUtil.getTensor(value)))
					.toArray(FloatTensor[]::new),
				0, null
			)
		);
	}
}
