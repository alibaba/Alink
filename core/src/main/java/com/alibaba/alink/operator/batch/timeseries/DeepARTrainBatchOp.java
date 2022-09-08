package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamCond;
import com.alibaba.alink.common.annotation.ParamCond.CondType;
import com.alibaba.alink.common.annotation.ParamMutexRule;
import com.alibaba.alink.common.annotation.ParamMutexRule.ActionType;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.operator.common.timeseries.DeepARFeaturesGenerator;
import com.alibaba.alink.operator.common.timeseries.DeepARModelDataConverter;
import com.alibaba.alink.operator.common.timeseries.DeepARModelDataConverter.DeepARModelData;
import com.alibaba.alink.operator.common.timeseries.TimestampUtil.TimestampToCalendar;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.params.timeseries.DeepARPreProcessParams;
import com.alibaba.alink.params.timeseries.DeepARTrainParams;
import com.alibaba.alink.params.timeseries.HasTimeFrequency;
import com.alibaba.alink.params.timeseries.HasTimeFrequency.TimeFrequency;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.MODEL))
@ParamSelectColumnSpec(name = "timeCol", allowedTypeCollections = TypeCollections.TIMESTAMP_TYPES)
@ParamSelectColumnSpec(name = "selectedCol")
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamMutexRule(
	name = "vectorCol", type = ActionType.DISABLE,
	cond = @ParamCond(
		name = "selectedCol",
		type = CondType.WHEN_NOT_NULL
	)
)
@ParamMutexRule(
	name = "selectedCol", type = ActionType.DISABLE,
	cond = @ParamCond(
		name = "vectorCol",
		type = CondType.WHEN_NOT_NULL
	)
)
@NameCn("DeepAR训练")
public class DeepARTrainBatchOp extends BatchOperator <DeepARTrainBatchOp>
	implements DeepARTrainParams <DeepARTrainBatchOp> {

	public DeepARTrainBatchOp() {
		this(new Params());
	}

	public DeepARTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public DeepARTrainBatchOp linkFrom(BatchOperator <?>... inputs) {

		BatchOperator <?> input = checkAndGetFirst(inputs);

		BatchOperator <?> preprocessed = new DeepARPreProcessBatchOp(getParams().clone())
			.setOutputCols("tensor", "v", "y")
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(input);

		Map <String, Object> modelConfig = new HashMap <>();
		modelConfig.put("window", getWindow());
		modelConfig.put("stride", getStride());

		Map <String, String> userParams = new HashMap <>();
		userParams.put("tensorCol", "tensor");
		userParams.put("labelCol", "y");
		userParams.put("batch_size", String.valueOf(getBatchSize()));
		userParams.put("num_epochs", String.valueOf(getNumEpochs()));
		userParams.put("model_config", JsonConverter.toJson(modelConfig));

		TFTableModelTrainBatchOp tfTableModelTrainBatchOp = new TFTableModelTrainBatchOp(getParams().clone())
			.setSelectedCols("tensor", "y")
			.setUserFiles(new String[] {"res:///tf_algos/deepar_entry.py"})
			.setMainScriptFile("res:///tf_algos/deepar_entry.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(preprocessed);

		final Params params = getParams();

		setOutput(
			tfTableModelTrainBatchOp
				.getDataSet()
				.reduceGroup(new RichGroupReduceFunction <Row, Row>() {
					private transient TimeFrequency frequency;

					@Override
					public void open(Configuration parameters) throws Exception {
						frequency = getRuntimeContext()
							.getBroadcastVariableWithInitializer(
								"frequency",
								new BroadcastVariableInitializer <TimeFrequency, TimeFrequency>() {
									@Override
									public TimeFrequency initializeBroadcastVariable(Iterable <TimeFrequency> data) {
										return data.iterator().next();
									}
								}
							);
					}

					@Override
					public void reduce(Iterable <Row> values, Collector <Row> out) throws Exception {
						List <Row> all = new ArrayList <>();

						for (Row val : values) {
							all.add(val);
						}

						new DeepARModelDataConverter().save(
							new DeepARModelData(params.clone().set(HasTimeFrequency.TIME_FREQUENCY, frequency), all),
							out
						);
					}
				})
				.withBroadcastSet(
					preprocessed
						.getSideOutput(0)
						.getDataSet().map(
							new MapFunction <Row, TimeFrequency>() {
								@Override
								public TimeFrequency map(Row value) throws Exception {
									return (TimeFrequency) value.getField(0);
								}
							}),
					"frequency"
				),
			new DeepARModelDataConverter().getModelSchema()
		);

		return this;
	}

	private static class DeepARPreProcessBatchOp extends BatchOperator <DeepARPreProcessBatchOp>
		implements DeepARPreProcessParams <DeepARPreProcessBatchOp> {

		public DeepARPreProcessBatchOp() {
			this(new Params());
		}

		public DeepARPreProcessBatchOp(Params params) {
			super(params);
		}

		@Override
		public DeepARPreProcessBatchOp linkFrom(BatchOperator <?>... inputs) {
			checkMinOpSize(1, inputs);

			BatchOperator <?> input = inputs[0];

			final String colName;

			if (getParams().contains(VECTOR_COL)) {
				colName = getVectorCol();
			} else {
				colName = getSelectedCol();
			}

			AkPreconditions.checkNotNull(colName);

			final String timeCol = getTimeCol();

			input = Preprocessing.select(input, timeCol, colName);

			final int colIndex = TableUtil.findColIndexWithAssertAndHint(input.getColNames(), colName);
			final int timeColIndex = TableUtil.findColIndexWithAssertAndHint(input.getColNames(), timeCol);

			DataSet <TimeFrequency> frequency;

			if (inputs.length > 1) {
				frequency = inputs[1]
					.getDataSet()
					.map(new MapFunction <Row, TimeFrequency>() {
						@Override
						public TimeFrequency map(Row value) throws Exception {
							return Params.fromJson(String.valueOf(value.getField(0))).get(
								HasTimeFrequency.TIME_FREQUENCY);
						}
					});
			} else {
				DataSet <Row> inputDataSet = input.getDataSet();

				DataSet <Tuple1 <Long>> cnt = DataSetUtils
					.countElementsPerPartition(inputDataSet)
					. <Tuple1 <Long>>project(1)
					.sum(0);

				frequency = input
					.getDataSet()
					.mapPartition(new MapPartitionFunction <Row, Tuple2 <Timestamp, Timestamp>>() {
						@Override
						public void mapPartition(Iterable <Row> values,
												 Collector <Tuple2 <Timestamp, Timestamp>> out) {
							Timestamp min = null, max = null;

							for (Row val : values) {
								Timestamp timestamp = (Timestamp) val.getField(timeColIndex);

								if (timestamp == null) {
									continue;
								}

								if (min == null) {
									min = timestamp;
									max = timestamp;
								} else {
									min = min.compareTo(timestamp) < 0 ? min : timestamp;
									max = max.compareTo(timestamp) > 0 ? max : timestamp;
								}
							}

							if (min != null) {
								out.collect(Tuple2.of(min, max));
							}
						}
					})
					.reduce(new ReduceFunction <Tuple2 <Timestamp, Timestamp>>() {
						@Override
						public Tuple2 <Timestamp, Timestamp> reduce(
							Tuple2 <Timestamp, Timestamp> value1, Tuple2 <Timestamp, Timestamp> value2) {

							return Tuple2.of(
								value1.f0.compareTo(value2.f0) < 0 ? value1.f0 : value2.f0,
								value1.f1.compareTo(value2.f1) > 0 ? value1.f1 : value2.f1
							);
						}
					})
					.reduceGroup(new RichGroupReduceFunction <Tuple2 <Timestamp, Timestamp>, TimeFrequency>() {
						private transient long cnt;

						@Override
						public void open(Configuration parameters) throws Exception {

							cnt = getRuntimeContext().getBroadcastVariableWithInitializer(
								"cnt",
								new BroadcastVariableInitializer <Tuple1 <Long>, Long>() {
									@Override
									public Long initializeBroadcastVariable(Iterable <Tuple1 <Long>> data) {
										return data.iterator().next().f0;
									}
								}
							);
						}

						@Override
						public void reduce(
							Iterable <Tuple2 <Timestamp, Timestamp>> values, Collector <TimeFrequency> out) {
							if (cnt == 0) {
								out.collect(TimeFrequency.MONTHLY);
								return;
							}

							Iterator <Tuple2 <Timestamp, Timestamp>> iterator = values.iterator();

							if (!iterator.hasNext()) {
								out.collect(TimeFrequency.MONTHLY);
								return;
							}

							Tuple2 <Timestamp, Timestamp> minMax = iterator.next();

							out.collect(DeepARFeaturesGenerator.generateFrequency(minMax.f0, minMax.f1, cnt));
						}
					})
					.withBroadcastSet(cnt, "cnt");
			}

			Tuple2 <DataSet <Tuple2 <Integer, Row>>, DataSet <Tuple2 <Integer, Long>>> sorted =
				SortUtils.pSort(input.getDataSet(), timeColIndex);

			String[] outputColNames = getOutputCols();

			AkPreconditions.checkState(outputColNames != null
				&& (outputColNames.length == 2 || outputColNames.length == 3));

			final boolean genY = outputColNames.length == 3;

			TypeInformation <?>[] outputColTypes = genY ?
				new TypeInformation <?>[] {AlinkTypes.FLOAT_TENSOR, AlinkTypes.FLOAT_TENSOR,
					AlinkTypes.FLOAT_TENSOR}
				:
					new TypeInformation <?>[] {AlinkTypes.FLOAT_TENSOR, AlinkTypes.FLOAT_TENSOR};

			final int window = getWindow();
			final int stride = getStride();

			setOutput(
				sorted.f0
					.partitionByHash(0)
					.mapPartition(
						new RichMapPartitionFunction <Tuple2 <Integer, Row>, Row>() {
							private final TimestampToCalendar calendar = new TimestampToCalendar();
							private transient TimeFrequency frequency;

							@Override
							public void open(Configuration parameters) throws Exception {
								frequency = getRuntimeContext()
									.getBroadcastVariableWithInitializer(
										"frequency",
										new BroadcastVariableInitializer <TimeFrequency, TimeFrequency>() {
											@Override
											public TimeFrequency initializeBroadcastVariable(
												Iterable <TimeFrequency> data) {
												return data.iterator().next();
											}
										}
									);
							}

							@Override
							public void mapPartition(Iterable <Tuple2 <Integer, Row>> values,
													 Collector <Row> out) {
								long series = 0;

								// series id
								final ArrayList <Tuple3 <Integer, FloatTensor, FloatTensor>> tensors
									= new ArrayList <>();

								for (Tuple2 <Integer, Row> val : values) {
									DenseVector vector = VectorUtil.getDenseVector(val.f1.getField(colIndex));

									if (vector == null) {
										tensors.add(Tuple3.of(val.f0, null, null));
										continue;
									}

									series = vector.size();

									Tuple3 <Integer, FloatTensor, FloatTensor> item = Tuple3.of(
										val.f0,
										FloatTensor.of(TensorUtil.getTensor(vector)).reshape(new Shape(series, 1)),
										DeepARFeaturesGenerator.generateFromFrequency(calendar, frequency,
											(Timestamp) val.f1.getField(timeColIndex))
									);

									tensors.add(item);
								}

								tensors.sort(Comparator.comparing(o -> o.f0));

								final int size = tensors.size();

								FloatTensor[] floatTensors = new FloatTensor[window];
								FloatTensor label = new FloatTensor(new float[window]);

								final int numInput = window - stride;
								final int total = size - window;

								for (int i = 0; i < series; ++i) {
									for (int start = 0; start < total; start += stride) {
										final int end = start + window;
										FloatTensor v = new FloatTensor(new float[] {0.0f, 0.0f});
										float nonZeroCnt = 0.0f;

										for (int j1 = start, j2 = 0; j1 < end; ++j1, ++j2) {

											FloatTensor tensor = new FloatTensor(new Shape(1));

											if (j1 != start) {

												final Tuple3 <Integer, FloatTensor, FloatTensor> item =
													tensors.get(j1 - 1);

												if (j2 <= numInput) {
													final float var = item.f1.getFloat(i, 0);

													if (var != 0.0f) {
														nonZeroCnt += 1.0f;
													}

													v.setFloat(v.getFloat(0) + var, 0);
												}

												tensor.setFloat(item.f1.getFloat(i, 0), 0);
											} else {
												tensor.setFloat(0.0f, 0);
											}

											floatTensors[j2] = Tensor.cat(
												new FloatTensor[] {tensor, tensors.get(j1).f2},
												-1, null
											);

											if (genY) {
												label.setFloat(tensors.get(j1).f1.getFloat(i, 0), j2);
											}
										}

										if (nonZeroCnt == 0.0f) {
											v.setFloat(0.0f, 0);
										} else {
											v.setFloat(v.getFloat(0) / nonZeroCnt + 1);

											for (int j = 0; j < window; ++j) {
												floatTensors[j].setFloat(
													floatTensors[j].getFloat(0) / v.getFloat(0), 0
												);

												if (genY) {
													label.setFloat(label.getFloat(j) / v.getFloat(0), j);
												}
											}
										}

										if (genY) {
											out.collect(Row.of(
												Tensor.stack(floatTensors, 0, null), v, label
											));
										} else {
											out.collect(Row.of(Tensor.stack(floatTensors, 0, null), v));
										}
									}
								}
							}
						}
					)
					.withBroadcastSet(frequency, "frequency"),
				outputColNames,
				outputColTypes
			);

			setSideOutputTables(new Table[] {
				DataSetConversionUtil
					.toTable(
					getMLEnvironmentId(),
					frequency
						.map(new MapFunction <TimeFrequency, Row>() {
							@Override
							public Row map(TimeFrequency value) {
								return Row.of(value);
							}
						}),
					new String[] {"frequency"},
					new TypeInformation <?>[] {TypeInformation.of(TimeFrequency.class)}
				)
			});

			return this;
		}
	}
}
