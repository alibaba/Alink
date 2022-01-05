package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.operator.common.tensorflow.TFTableModelPredictModelMapper;
import com.alibaba.alink.operator.common.timeseries.DeepARModelDataConverter.DeepARModelData;
import com.alibaba.alink.operator.common.timeseries.TimestampUtil.TimestampToCalendar;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;
import com.alibaba.alink.params.timeseries.HasTimeFrequency;
import com.alibaba.alink.params.timeseries.HasTimeFrequency.TimeFrequency;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class DeepARModelMapper extends TimeSeriesModelMapper {

	private static final String[] TF_MODEL_MAPPER_INPUT_COL_NAMES
		= new String[] {"agg_to_tensor_tensor_col_internal_impl"};
	private static final TypeInformation <?>[] TF_MODEL_MAPPER_INPUT_COL_TYPES
		= new TypeInformation <?>[] {TensorTypes.FLOAT_TENSOR};

	private transient TimeFrequency unit;

	private static Params createTfModelMapperParams() {
		return new Params()
			.set(TFTableModelPredictParams.SELECTED_COLS, TF_MODEL_MAPPER_INPUT_COL_NAMES)
			.set(TFTableModelPredictParams.SIGNATURE_DEF_KEY, "serving_default")
			.set(TFTableModelPredictParams.INPUT_SIGNATURE_DEFS, new String[] {"tensor"})
			.set(TFTableModelPredictParams.OUTPUT_SCHEMA_STR, "pred FLOAT_TENSOR")
			.set(TFTableModelPredictParams.OUTPUT_SIGNATURE_DEFS, new String[] {"tf_op_layer_output"})
			.set(TFTableModelPredictParams.RESERVED_COLS, new String[] {});
	}

	private final TFTableModelPredictModelMapper tfTableModelPredictModelMapper;

	private transient ThreadLocal <TimestampUtil.TimestampToCalendar> calendar;

	public DeepARModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
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

		Timestamp[] predictTimes = TimeSeriesMapper.getPredictTimes(historyTimes, predictNum);

		int window = historyVals.length;

		FloatTensor[] tensors = new FloatTensor[window];

		// fill the first z with zero
		tensors[0] = Tensor.cat(
			new FloatTensor[] {
				new FloatTensor(new float[] {0.0f}),
				DeepARFeaturesGenerator.generateFromFrequency(calendar.get(), unit, historyTimes[0])
			},
			-1, null
		);

		// others
		for (int i = 1; i < window; ++i) {
			tensors[i] = Tensor.cat(
				new FloatTensor[] {
					new FloatTensor(new float[] {(float) historyVals[i - 1]}),
					DeepARFeaturesGenerator.generateFromFrequency(calendar.get(), unit, historyTimes[i])
				},
				-1, null
			);
		}

		FloatTensor batch = Tensor.stack(tensors, 0, null);

		// initialize mu
		float mu = (float) historyVals[window - 1];

		// calculate v
		FloatTensor v = new FloatTensor(new float[] {0.0f, 0.0f});
		int nonZero = 0;
		for (int i = 0; i < window; ++i) {
			float cell = batch.getFloat(i, 0);

			if (cell != 0) {
				nonZero += 1;
			}

			v.setFloat(v.getFloat(0) + cell, 0);
		}

		if (mu != 0) {
			nonZero += 1;
			v.setFloat(v.getFloat(0) + mu, 0);
		}

		if (nonZero == 0) {
			double[] result = new double[predictNum];
			Row[] sigmas = new Row[predictNum];
			Arrays.fill(result, 0.0);
			Arrays.fill(sigmas, Row.of(0));
			return Tuple2.of(
				result,
				new MTable(
					Arrays.asList(sigmas),
					new String[] {"sigma"},
					new TypeInformation <?>[] {Types.DOUBLE}
				).toString()
			);
		}

		v.setFloat(v.getFloat(0) / nonZero + 1.0f, 0);

		// normalize with v
		for (int i = 0; i < window; ++i) {
			batch.setFloat(batch.getFloat(i, 0) / v.getFloat(0), i, 0);
		}

		mu = mu / v.getFloat(0);

		// result initialize.
		double[] result = new double[predictNum];
		Row[] sigmas = new Row[predictNum];
		Arrays.fill(result, 0.0);
		for (int i = 0; i < predictNum; ++i) {
			sigmas[i] = Row.of(0.0);
		}

		// prediction
		for (int j = 0; j < predictNum; ++j) {
			batch = Tensor.cat(
				new FloatTensor[] {
					batch,
					Tensor.stack(
						new FloatTensor[] {
							Tensor.cat(
								new FloatTensor[] {
									new FloatTensor(new float[] {mu}),
									DeepARFeaturesGenerator.generateFromFrequency(calendar.get(), unit,
										predictTimes[j])
								},
								-1, null)},
						0, null
					)},
				0, null
			);

			FloatTensor pred;

			try {
				pred = (FloatTensor) tfTableModelPredictModelMapper.map(Row.of(batch)).getField(0);
			} catch (Exception e) {
				return Tuple2.of(null, null);
			}

			mu = pred.getFloat(window + j, 0);
			float sigma = pred.getFloat(window + j, 1);

			result[j] = mu * v.getFloat(0) + v.getFloat(1);
			sigmas[j].setField(0, sigma * v.getFloat(0));
		}

		return Tuple2.of(
			result,
			new MTable(
				Arrays.asList(sigmas),
				new String[] {"sigma"},
				new TypeInformation <?>[] {Types.DOUBLE}
			).toString()
		);
	}

	@Override
	protected Tuple2 <Vector[], String> predictMultiVar(
		Timestamp[] historyTimes, Vector[] historyVals, int predictNum) {

		Timestamp[] predictTimes = TimeSeriesMapper.getPredictTimes(historyTimes, predictNum);

		int window = historyVals.length;

		int series = 0;

		DenseVector[] vectors = new DenseVector[historyVals.length];

		for (int i = 0; i < window; ++i) {
			vectors[i] = VectorUtil.getDenseVector(historyVals[i]);

			if (vectors[i] == null) {
				throw new IllegalArgumentException("history values should not be null.");
			}

			series = vectors[i].size();
		}

		FloatTensor[][] tensors = new FloatTensor[series][window];

		for (int i = 0; i < series; ++i) {
			tensors[i][0] = Tensor.cat(
				new FloatTensor[] {
					new FloatTensor(new float[] {0.0f}),
					DeepARFeaturesGenerator.generateFromFrequency(calendar.get(), unit, historyTimes[0])
				},
				-1, null
			);

			for (int j = 1; j < window; ++j) {
				tensors[i][j] = Tensor.cat(
					new FloatTensor[] {
						new FloatTensor(new float[] {(float) vectors[j - 1].get(i)}),
						DeepARFeaturesGenerator.generateFromFrequency(calendar.get(), unit, historyTimes[j])
					},
					-1, null
				);
			}
		}

		FloatTensor[] batch = new FloatTensor[series];

		for (int i = 0; i < series; ++i) {
			batch[i] = Tensor.stack(tensors[i], 0, null);
		}

		Vector[] result = new Vector[predictNum];
		Row[] sigmas = new Row[predictNum];

		for (int i = 0; i < predictNum; ++i) {
			result[i] = new DenseVector(series);
			sigmas[i] = Row.of(new DenseVector(series));
		}

		for (int i = 0; i < series; ++i) {
			float mu = (float) historyVals[window - 1].get(i);

			FloatTensor v = new FloatTensor(new float[] {0.0f, 0.0f});
			int nonZero = 0;
			for (int j = 0; j < window; ++j) {
				float cell = batch[i].getFloat(j, 0);

				if (cell != 0) {
					nonZero += 1;
				}

				v.setFloat(v.getFloat(0) + cell, 0);
			}

			if (mu != 0) {
				nonZero += 1;
				v.setFloat(v.getFloat(0) + mu, 0);
			}

			if (nonZero == 0) {
				continue;
			}

			v.setFloat(v.getFloat(0) / nonZero + 1.0f, 0);

			for (int j = 0; j < window; ++j) {
				batch[i].setFloat(batch[i].getFloat(j, 0) / v.getFloat(0), j, 0);
			}

			mu = mu / v.getFloat(0);

			for (int j = 0; j < predictNum; ++j) {
				batch[i] = Tensor.cat(
					new FloatTensor[] {
						batch[i],
						Tensor.stack(
							new FloatTensor[] {
								Tensor.cat(
									new FloatTensor[] {
										new FloatTensor(new float[] {mu}),
										DeepARFeaturesGenerator.generateFromFrequency(calendar.get(), unit,
											predictTimes[j])
									},
									-1, null)},
							0, null
						)},
					0, null
				);

				FloatTensor pred;

				try {
					pred = (FloatTensor) tfTableModelPredictModelMapper
						.map(Row.of(batch[i]))
						.getField(0);
				} catch (Exception e) {
					return Tuple2.of(null, null);
				}

				mu = pred.getFloat(window + j, 0);
				float sigma = pred.getFloat(window + j, 1);

				result[j].set(i, mu * v.getFloat(0) + v.getFloat(1));
				((Vector) (sigmas[j].getField(0))).set(i, sigma * v.getFloat(0));
			}
		}

		return Tuple2.of(
			result,
			new MTable(
				Arrays.asList(sigmas),
				new String[] {"sigma"},
				new TypeInformation <?>[] {VectorTypes.DENSE_VECTOR}
			).toString()
		);
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
		DeepARModelData modelData = new DeepARModelDataConverter().load(modelRows);

		unit = modelData.meta.get(HasTimeFrequency.TIME_FREQUENCY);
		tfTableModelPredictModelMapper.loadModel(modelData.deepModel);

		calendar = ThreadLocal.withInitial(TimestampToCalendar::new);
	}
}
