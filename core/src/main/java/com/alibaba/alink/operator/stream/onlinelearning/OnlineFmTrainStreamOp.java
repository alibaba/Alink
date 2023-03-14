package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.exceptions.AkIllegalModelException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.FmDataFormat;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.LogitLoss;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.LossFunction;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;
import com.alibaba.alink.operator.common.fm.FmModelData;
import com.alibaba.alink.operator.common.fm.FmModelDataConverter;
import com.alibaba.alink.operator.common.modelstream.ModelStreamUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlTrainStreamOp.PrepareBatchSample;
import com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp;
import com.alibaba.alink.params.onlinelearning.OnlineFmTrainParams;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.optim.FmOptimizer.calcY;

/**
 * FM algorithm receive train data streams, using training samples to update model, and output model after every time
 * interval.
 */

@Internal
@NameCn("在线FM训练")
@NameEn("Online Factorization Machine training")
public final class OnlineFmTrainStreamOp extends StreamOperator <OnlineFmTrainStreamOp>
	implements OnlineFmTrainParams <OnlineFmTrainStreamOp> {

	private static final long serialVersionUID = -1717242899554835631L;
	DataBridge dataBridge;
	private final String modelSchemeStr;

	public OnlineFmTrainStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public OnlineFmTrainStreamOp(BatchOperator <?> model, Params params) {
		super(params);
		if (model != null) {
			dataBridge = DirectReader.collect(model);
			modelSchemeStr = TableUtil.schema2SchemaStr(model.getSchema());
		} else {
			throw new AkIllegalModelException("Online algo: initial model is null. Please set the initial model.");
		}
	}

	@Override
	public OnlineFmTrainStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamExecutionEnvironment streamEnv = MLEnvironmentFactory.get(getMLEnvironmentId())
			.getStreamExecutionEnvironment();
		int parallelism = streamEnv.getParallelism();
		streamEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		streamEnv.getCheckpointConfig().setForceCheckpointing(true);
		checkOpSize(1, inputs);
		Params params = getParams();
		String vecColName = getVectorCol();
		String[] featureCols = getFeatureCols();
		int vectorTrainIdx = vecColName != null ? TableUtil.findColIndexWithAssertAndHint(inputs[0].getColNames(),
			vecColName) : -1;
		int labelIdx = TableUtil.findColIndexWithAssertAndHint(inputs[0].getColNames(), getLabelCol());
		int[] featureIdx = null;
		if (vectorTrainIdx == -1) {
			featureIdx = new int[featureCols.length];
			for (int i = 0; i < featureCols.length; ++i) {
				featureIdx[i] = TableUtil.findColIndexWithAssertAndHint(inputs[0].getColNames(), featureCols[i]);
			}
		}
		final TypeInformation <?> labelType = inputs[0].getColTypes()[labelIdx];

		final int timeInterval = getTimeInterval();
		DataStream <Tuple2 <Long, Object>> modelSaveHandler = streamEnv.fromElements(Row.of(1)).flatMap(
			new FlatMapFunction <Row, Tuple2 <Long, Object>>() {
				@Override
				public void flatMap(Row row, Collector <Tuple2 <Long, Object>> collector) throws Exception {
					for (long i = 0; i < Long.MAX_VALUE; ++i) {
						Thread.sleep(timeInterval * 1000L);
						collector.collect(Tuple2.of(0L, 0L));
					}
				}
			});

		/* Prepares mini batches for training process. */
		DataStream <Row[]> batchData = inputs[0].getDataStream().rebalance()
			.flatMap(
				new PrepareBatchSample(Math.max(1, getMiniBatchSize() / parallelism)));

		/* Prepares rebase model stream. */
		DataStream <Tuple2 <Long, Object>> modelStream = null;
		if (ModelStreamUtils.useModelStreamFile(params)) {
			StreamOperator <?> modelStreamOp =
				new ModelStreamFileSourceStreamOp().setFilePath(getModelStreamFilePath())
					.setScanInterval(getModelStreamScanInterval()).setStartTime(getModelStreamStartTime()).setSchemaStr(
						modelSchemeStr).setMLEnvironmentId(inputs[0].getMLEnvironmentId());
			modelStream = modelStreamOp.getDataStream().map(new MapFunction <Row, Tuple2 <Long, Object>>() {
				@Override
				public Tuple2 <Long, Object> map(Row row) {
					return Tuple2.of(-1L, row);
				}
			});
		}

		IterativeStream.ConnectedIterativeStreams <Row[], Tuple2 <Long, Object>> iteration = batchData.iterate(
			Long.MAX_VALUE).withFeedbackType(TypeInformation.of(new TypeHint <Tuple2 <Long, Object>>() {}));

		DataStream iterativeBody = iteration
			.flatMap(new ModelUpdater(dataBridge, params, vectorTrainIdx, featureIdx, labelIdx, modelSchemeStr));

		DataStream <Tuple2 <Long, Object>> result = iterativeBody.filter(new FilterFunction <Tuple2 <Long, Object>>() {
			private static final long serialVersionUID = -5436758453355074895L;

			@Override
			public boolean filter(Tuple2 <Long, Object> t2) {
				// if t2.f0 >= 0: data is gradient and feedback it.
				return (t2.f0 >= 0L);
			}
		}).keyBy(0).countWindowAll(parallelism).reduce(new ReduceFunction <Tuple2 <Long, Object>>() {
			@Override
			public Tuple2 <Long, Object> reduce(Tuple2 <Long, Object> o, Tuple2 <Long, Object> t1) {
				Map <Integer, Tuple2 <Double, double[]>> grad1 = (Map <Integer, Tuple2 <Double, double[]>>) o.f1;
				Map <Integer, Tuple2 <Double, double[]>> grad2 = (Map <Integer, Tuple2 <Double, double[]>>) t1.f1;
				for (int idx : grad2.keySet()) {
					if (grad1.containsKey(idx)) {
						grad1.get(idx).f0 += grad2.get(idx).f0;
						double[] factor = grad1.get(idx).f1;
						for (int i = 0; i < factor.length; ++i) {
							factor[i] += grad2.get(idx).f1[i];
						}
					} else {
						grad1.put(idx, grad2.get(idx));
					}
				}
				return o;
			}
		}).map(new MapFunction <Tuple2 <Long, Object>, Tuple2 <Long, Object>>() {
			@Override
			public Tuple2 <Long, Object> map(Tuple2 <Long, Object> gradient) {
				Map <Integer, Tuple2 <Double, double[]>> grad = (Map <Integer, Tuple2 <Double, double[]>>) gradient.f1;

				Map <Integer, double[]> gradAverage = new HashMap <>();
				for (int idx : grad.keySet()) {
					double[] gradValues = grad.get(idx).f1;
					for (int i = 0; i < gradValues.length; ++i) {
						gradValues[i] /= grad.get(idx).f0;
					}
					gradAverage.put(idx, gradValues);
				}
				return Tuple2.of(gradient.f0, gradAverage);
			}
		}).setParallelism(parallelism);

		result = (modelStream == null ? result : result.union(modelStream)).broadcast();

		DataStream <Row> output = iterativeBody.filter(new FilterFunction <Tuple2 <Long, Object>>() {
			private static final long serialVersionUID = 4204787383191799107L;

			@Override
			public boolean filter(Tuple2 <Long, Object> t2) {
				/* if t2.f0 small than 0L: data is model, then output */
				return t2.f0 < 0L;
			}
		}).flatMap(new WriteModel());

		iteration.closeWith(result.union(modelSaveHandler));

		TableSchema schema = new FmModelDataConverter(labelType).getModelSchema();

		TypeInformation <?>[] types = new TypeInformation[schema.getFieldNames().length + 2];
		String[] names = new String[schema.getFieldNames().length + 2];
		names[0] = ModelStreamUtils.MODEL_STREAM_TIMESTAMP_COLUMN_NAME;
		names[1] = ModelStreamUtils.MODEL_STREAM_COUNT_COLUMN_NAME;
		types[0] = ModelStreamUtils.MODEL_STREAM_TIMESTAMP_COLUMN_TYPE;
		types[1] = ModelStreamUtils.MODEL_STREAM_COUNT_COLUMN_TYPE;
		for (int i = 0; i < schema.getFieldNames().length; ++i) {
			types[i + 2] = schema.getFieldTypes()[i];
			names[i + 2] = schema.getFieldNames()[i];
		}

		this.setOutput(output, names, types);
		return this;
	}

	public static class ModelUpdater extends RichCoFlatMapFunction <Row[], Tuple2 <Long, Object>, Tuple2 <Long,
		Object>>
		implements CheckpointedFunction {
		private static final long serialVersionUID = 7338858137860097282L;
		private final DataBridge dataBridge;
		transient private FmModelData modelData;
		double[] regular;
		int[] dim;
		private Object[] labelValues;
		transient private LossFunction lossFunc;
		transient private FmDataFormat nParam;
		transient private FmDataFormat zParam;
		/* Tuple3 : factors, nParams, labelValues */
		private transient ListState <Tuple4 <FmDataFormat, FmDataFormat, FmDataFormat, Object[]>> modelState;
		private long gradVersion = 0L;
		private final int vectorTrainIdx;
		private final int labelIdx;
		private final int[] featureIdx;
		private boolean isUpdatedModel = true;
		private List <Row[]> localBatchDataBuffer;
		private List <Map <Integer, double[]>> gradientBuffer;
		private final int batchSize;
		private int maxNumBatches;
		// Map<gradIndex, Tuple2<weight, gradients>>
		private final Map <Integer, Tuple2 <Double, double[]>> sparseGradient = new HashMap <>();

		private final double l1;
		private final double l2;
		private final double alpha;
		private final double beta;

		private final Map <Timestamp, List <Row>> buffers = new HashMap <>();
		private final String modelSchemaStr;

		public ModelUpdater(DataBridge dataBridge, Params params, int vectorIdx,
							int[] featureIdx, int labelIdx, String modelSchemaStr) {
			this.dataBridge = dataBridge;
			this.labelValues = null;
			this.batchSize = params.get(OnlineFmTrainParams.MINI_BATCH_SIZE);
			this.vectorTrainIdx = vectorIdx;
			this.labelIdx = labelIdx;
			this.featureIdx = featureIdx;
			this.regular = new double[3];
			this.regular[0] = params.get(OnlineFmTrainParams.LAMBDA_0);
			this.regular[1] = params.get(OnlineFmTrainParams.LAMBDA_1);
			this.regular[2] = params.get(OnlineFmTrainParams.LAMBDA_2);
			this.dim = new int[3];
			this.dim[0] = params.get(OnlineFmTrainParams.WITH_INTERCEPT) ? 1 : 0;
			this.dim[1] = params.get(OnlineFmTrainParams.WITH_LINEAR_ITEM) ? 1 : 0;
			this.dim[2] = params.get(OnlineFmTrainParams.NUM_FACTOR);
			this.l1 = params.get(OnlineFmTrainParams.L_1);
			this.l2 = params.get(OnlineFmTrainParams.L_2);
			this.alpha = params.get(OnlineFmTrainParams.ALPHA);
			this.beta = params.get(OnlineFmTrainParams.BETA);
			this.lossFunc = new LogitLoss();
			this.modelSchemaStr = modelSchemaStr;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.maxNumBatches = Math.max(1, 100000 * getRuntimeContext().getNumberOfParallelSubtasks() / batchSize);
			lossFunc = new LogitLoss();
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			modelState.clear();
			modelState.add(Tuple4.of(modelData.fmModel, nParam, zParam, labelValues));
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {

			modelState = context.getOperatorStateStore().getListState(
				new ListStateDescriptor <>("StreamingOnlineModelState", TypeInformation.of(
					new TypeHint <Tuple4 <FmDataFormat, FmDataFormat, FmDataFormat, Object[]>>() {})));

			if (context.isRestored()) {
				// read model data from checkpoint.
				for (Tuple4 <FmDataFormat, FmDataFormat, FmDataFormat, Object[]> state : modelState.get()) {
					this.modelData.fmModel = state.f0;
					this.nParam = state.f1;
					this.zParam = state.f2;
					this.labelValues = state.f3;
				}
			} else {
				// read model data from init model.
				List <Row> modelRows = DirectReader.directRead(dataBridge);
				this.modelData = new FmModelDataConverter().load(modelRows);
				double[][] factors = modelData.fmModel.factors;
				this.nParam = new FmDataFormat(factors.length, factors[0].length, modelData.dim, 0.0);
				this.zParam = new FmDataFormat(factors.length, factors[0].length, modelData.dim, 0.0);
				this.labelValues = modelData.labelValues;
			}
		}

		@Override
		public void flatMap1(Row[] rows, Collector <Tuple2 <Long, Object>> out) throws Exception {
			if (localBatchDataBuffer == null) {
				localBatchDataBuffer = new ArrayList <>();
				gradientBuffer = new ArrayList <>();
			}
			localBatchDataBuffer.add(rows);
			if (isUpdatedModel) {
				sparseGradient.clear();
				for (Row[] currentRows : localBatchDataBuffer) {
					for (Row row : currentRows) {
						Vector vec;
						if (vectorTrainIdx == -1) {
							vec = new DenseVector(featureIdx.length);
							for (int i = 0; i < featureIdx.length; ++i) {
								vec.set(i, Double.parseDouble(row.getField(featureIdx[i]).toString()));
							}
						} else {
							vec = VectorUtil.getVector(row.getField(vectorTrainIdx));
						}
						double yTruth = (row.getField(labelIdx).equals(labelValues[0]) ? 1.0 : 0.0);
						Tuple2 <Double, double[]> yVx = calcY(vec, modelData.fmModel, dim);
						double dldy = lossFunc.dldy(yTruth, yVx.f0);

						int[] indices;
						double[] values;
						if (vec instanceof SparseVector) {
							indices = ((SparseVector) vec).getIndices();
							values = ((SparseVector) vec).getValues();
						} else {
							indices = new int[vec.size()];
							for (int i = 0; i < vec.size(); ++i) {
								indices[i] = i;
							}
							values = ((DenseVector) vec).getData();
						}

						if (dim[0] > 0) {
							double biasGrad = dldy + regular[0] * modelData.fmModel.bias;
							if (sparseGradient.containsKey(-1)) {
								sparseGradient.get(-1).f0 += 1.0;
								sparseGradient.get(-1).f1[0] += biasGrad;
							} else {
								sparseGradient.put(-1, Tuple2.of(1.0, new double[] {biasGrad}));
							}
						}
						double[][] factors = modelData.fmModel.factors;
						for (int i = 0; i < indices.length; ++i) {
							int idx = indices[i];
							if (sparseGradient.containsKey(idx)) {
								double[] grad = sparseGradient.get(idx).f1;
								sparseGradient.get(idx).f0 += 1.0;
								if (dim[1] > 0) {
									grad[dim[2]] += dldy * values[i] + regular[1] * factors[idx][dim[2]];
								}
								if (dim[2] > 0) {
									for (int j = 0; j < dim[2]; j++) {
										double vixi = values[i] * factors[idx][j];
										double d = values[i] * (yVx.f1[j] - vixi);
										grad[j] += dldy * d + regular[2] * factors[idx][j];
									}
								}
							} else {
								double[] grad = new double[dim[2] + dim[1]];
								if (dim[1] > 0) {
									grad[dim[2]] = dldy * values[i] + regular[1] * factors[idx][dim[2]];
								}
								if (dim[2] > 0) {
									for (int j = 0; j < dim[2]; j++) {
										double vixi = values[i] * factors[idx][j];
										double d = values[i] * (yVx.f1[j] - vixi);
										grad[j] = dldy * d + regular[2] * factors[idx][j];
									}
								}
								sparseGradient.put(idx, Tuple2.of(1.0, grad));
							}
						}
					}
				}
				localBatchDataBuffer.clear();
				isUpdatedModel = false;
				out.collect(Tuple2.of(gradVersion++, sparseGradient));
			} else {
				if (localBatchDataBuffer.size() > maxNumBatches) {
					localBatchDataBuffer.subList(0, Math.min(maxNumBatches - 1, maxNumBatches * 9 / 10)).clear();
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("remove batches happened. ");
					}
				}
			}
		}

		@Override
		public void flatMap2(Tuple2 <Long, Object> value, Collector <Tuple2 <Long, Object>> out) {
			if (value.f1 instanceof Map) {
				/* Updates model. */
				gradientBuffer.add((Map) value.f1);
				if (!isUpdatedModel) {
					updateModel(gradientBuffer.remove(0));
					isUpdatedModel = true;
				}
			} else if (value.f1 instanceof Row) {
				Row inRow = (Row) value.f1;
				Timestamp timestamp = (Timestamp) inRow.getField(0);
				long count = (long) inRow.getField(1);
				Row row = ModelStreamUtils.genRowWithoutIdentifier(inRow, 0, 1);

				if (buffers.containsKey(timestamp)) {
					buffers.get(timestamp).add(row);
				} else {
					List <Row> buffer = new ArrayList <>(0);
					buffer.add(row);
					buffers.put(timestamp, buffer);
				}
				if (buffers.get(timestamp).size() == (int) count) {
					try {
						FmModelDataConverter fmModelDataConverter = new FmModelDataConverter(
							FmModelDataConverter.extractLabelType(TableUtil.schemaStr2Schema(modelSchemaStr)));
						modelData.fmModel = fmModelDataConverter.load(buffers.remove(timestamp)).fmModel;
						double[][] factors = modelData.fmModel.factors;
						this.nParam = new FmDataFormat(factors.length, factors[0].length, modelData.dim, 0.0);
						this.zParam = new FmDataFormat(factors.length, factors[0].length, modelData.dim, 0.0);
					} catch (Exception e) {
						System.err.println("test Model stream updating failed. Please check your model stream.");
					}
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("rebase fm model.");
					}
				}
			} else if (value.f1 instanceof Long) {
				Params meta = new Params()
					.set(ModelParamName.VECTOR_COL_NAME, modelData.vectorColName)
					.set(ModelParamName.LABEL_COL_NAME, modelData.labelColName)
					.set(ModelParamName.TASK, Task.BINARY_CLASSIFICATION)
					.set(ModelParamName.VECTOR_SIZE, modelData.vectorSize)
					.set(ModelParamName.FEATURE_COL_NAMES, modelData.featureColNames)
					.set(ModelParamName.LABEL_VALUES, labelValues)
					.set(ModelParamName.DIM, modelData.dim);
				long currentTime = System.currentTimeMillis();
				out.collect(Tuple2.of(-currentTime, Tuple3.of((long) modelData.vectorSize + 2, null, meta.toJson())));

				for (int i = 0; i < modelData.fmModel.factors.length; ++i) {
					double[] factor = modelData.fmModel.factors[i];
					out.collect(Tuple2.of(-currentTime,
						Tuple3.of((long) modelData.vectorSize + 2, (long) i, JsonConverter.toJson(factor))));
				}
				out.collect(Tuple2.of(-currentTime,
					Tuple3.of((long) modelData.vectorSize + 2, -1L,
						JsonConverter.toJson(new double[] {modelData.fmModel.bias}))));
			} else {
				throw new AkUnclassifiedErrorException("feedback data type err, must be a Map or DenseVector.");
			}
		}

		private void updateModel(Map <Integer, double[]> grad) {
			for (int idx : grad.keySet()) {
				// update fmModel
				if (idx == -1) {
					assert (grad.get(idx).length == 1);
					double biasGrad = grad.get(idx)[0];
					double sigma = (Math.sqrt(nParam.bias + biasGrad * biasGrad) - Math.sqrt(nParam.bias)) / alpha;
					zParam.bias += biasGrad - sigma * modelData.fmModel.bias;
					nParam.bias += biasGrad * biasGrad;
					if (Math.abs(zParam.bias) <= l1) {
						modelData.fmModel.bias = 0.0;
					} else {
						modelData.fmModel.bias = ((zParam.bias < 0 ? -1 : 1) * l1 - zParam.bias) / ((beta + Math.sqrt(
							nParam.bias)) / alpha + l2);
					}
				} else {
					double[] factor = grad.get(idx);
					if (dim[1] > 0) {
						updateModelVal(idx, dim[2], factor[dim[2]]);
					}
					if (dim[2] > 0) {
						for (int j = 0; j < dim[2]; j++) {
							updateModelVal(idx, j, factor[j]);
						}
					}
				}
			}
		}

		private void updateModelVal(int idx, int j, double gradVal) {
			double sigma = (Math.sqrt(nParam.factors[idx][j] + gradVal * gradVal) - Math.sqrt(nParam.factors[idx][j]))
				/ alpha;
			zParam.factors[idx][j] += gradVal - sigma * modelData.fmModel.factors[idx][j];
			nParam.factors[idx][j] += gradVal * gradVal;
			if (Math.abs(zParam.factors[idx][j]) <= l1) {
				modelData.fmModel.factors[idx][j] = 0.0;
			} else {
				modelData.fmModel.factors[idx][j] =
					((zParam.factors[idx][j] < 0 ? -1 : 1) * l1 - zParam.factors[idx][j]) / ((beta + Math.sqrt(
						nParam.factors[idx][j])) / alpha + l2);
			}
		}
	}

	public static class WriteModel extends RichFlatMapFunction <Tuple2 <Long, Object>, Row> {
		private static final long serialVersionUID = 3487644568763785149L;

		@Override
		public void flatMap(Tuple2 <Long, Object> value, Collector <Row> out) throws Exception {
			Timestamp curModelTimestamp = new Timestamp(-value.f0);

			Tuple3 <Long, Long, String> element = (Tuple3 <Long, Long, String>) value.f1;
			out.collect(Row.of(curModelTimestamp, element.f0, element.f1, element.f2, null));
		}
	}
}

