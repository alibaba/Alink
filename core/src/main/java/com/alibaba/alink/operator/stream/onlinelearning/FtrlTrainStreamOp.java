package com.alibaba.alink.operator.stream.onlinelearning;

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
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LabelTypeEnum.StringTypeEnum;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.modelstream.ModelStreamUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp;
import com.alibaba.alink.params.onlinelearning.FtrlTrainParams;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stream train operation of ftrl Algorithm.
 */
@NameCn("Ftrl在线训练")
@NameEn("Follow the regularized leader model training")
public class FtrlTrainStreamOp extends StreamOperator <FtrlTrainStreamOp>
	implements FtrlTrainParams <FtrlTrainStreamOp> {

	private static final long serialVersionUID = 3688413917992858013L;
	private final DataBridge dataBridge;
	private final String modelSchemeStr;

	public FtrlTrainStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public FtrlTrainStreamOp(BatchOperator <?> model, Params params) {
		super(params);
		if (model != null) {
			dataBridge = DirectReader.collect(model);
			modelSchemeStr = TableUtil.schema2SchemaStr(model.getSchema());
		} else {
			throw new AkIllegalModelException("Online algo: initial model is null. Please set the initial model.");
		}
	}

	@Override
	public FtrlTrainStreamOp linkFrom(StreamOperator <?>... inputs) {
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
		DataStream <Tuple2 <Long, Object>> modelSaveHandler = streamEnv.fromElements(Row.of(1))
			.flatMap(new FlatMapFunction <Row, Tuple2 <Long, Object>>() {
				@Override
				public void flatMap(Row row, Collector <Tuple2 <Long, Object>> collector) throws Exception {
					for (long i = 0; i < Long.MAX_VALUE; ++i) {
						Thread.sleep(timeInterval * 1000L);
						collector.collect(Tuple2.of(0L, 0L));
					}
				}
			});

		/* Prepares mini batches for training process. */
		DataStream <Row[]> batchData = inputs[0].getDataStream().rebalance().flatMap(
			new PrepareBatchSample(Math.max(1, getMiniBatchSize() / parallelism)));

		/* Prepares rebase model stream. */
		DataStream <Tuple2 <Long, Object>> modelStream = null;
		if (ModelStreamUtils.useModelStreamFile(params)) {
			StreamOperator <?> modelStreamOp =
				new ModelStreamFileSourceStreamOp().setFilePath(getModelStreamFilePath())
					.setScanInterval(getModelStreamScanInterval()).setStartTime(getModelStreamStartTime()).setSchemaStr(
						modelSchemeStr).setMLEnvironmentId(inputs[0].getMLEnvironmentId());
			modelStream = modelStreamOp.getDataStream().flatMap(new ParseRebaseModel(modelSchemeStr));
		}

		IterativeStream.ConnectedIterativeStreams <Row[], Tuple2 <Long, Object>> iteration = batchData.iterate(
			Long.MAX_VALUE).withFeedbackType(TypeInformation.of(new TypeHint <Tuple2 <Long, Object>>() {}));

		DataStream iterativeBody = iteration.flatMap(
			new ModelUpdater(dataBridge, params, featureIdx, vectorTrainIdx, labelIdx));

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
				Map <Integer, double[]> grad1 = (Map <Integer, double[]>) o.f1;
				Map <Integer, double[]> grad2 = (Map <Integer, double[]>) t1.f1;
				for (int idx : grad2.keySet()) {
					if (grad1.containsKey(idx)) {
						grad1.get(idx)[0] += grad2.get(idx)[0];
						grad1.get(idx)[1] += grad2.get(idx)[1];
					} else {
						grad1.put(idx, grad2.get(idx));
					}
				}
				return o;
			}
		}).map(new MapFunction <Tuple2 <Long, Object>, Tuple2 <Long, Object>>() {
			@Override
			public Tuple2 <Long, Object> map(Tuple2 <Long, Object> gradient) {
				Map <Integer, double[]> grad = (Map <Integer, double[]>) gradient.f1;
				int[] indices = new int[grad.size()];
				double[] values = new double[grad.size()];
				int iter = 0;
				for (Integer i : grad.keySet()) {
					indices[iter] = i;
					double[] gradVal = grad.get(i);
					values[iter++] = gradVal[0] / gradVal[1];
				}
				return Tuple2.of(gradient.f0, new SparseVector(-1, indices, values));
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
		}).flatMap(new WriteModel(labelType, vecColName, featureCols, getWithIntercept()));

		iteration.closeWith(result.union(modelSaveHandler));

		TableSchema schema = new LinearModelDataConverter(labelType).getModelSchema();

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

	private static class ParseRebaseModel implements FlatMapFunction <Row, Tuple2 <Long, Object>> {
		private final Map <Timestamp, List <Row>> buffers = new HashMap <>();
		private final String modelSchemaStr;

		public ParseRebaseModel(String modelSchemaStr) {
			this.modelSchemaStr = modelSchemaStr;
		}

		@Override
		public void flatMap(Row inRow, Collector <Tuple2 <Long, Object>> collector) throws Exception {
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
					LinearModelDataConverter linearModelDataConverter = new LinearModelDataConverter(
						LinearModelDataConverter.extractLabelType(TableUtil.schemaStr2Schema(modelSchemaStr)));
					DenseVector coefficient = linearModelDataConverter.load(buffers.get(timestamp)).coefVector;
					collector.collect(Tuple2.of(-1L, coefficient));
					buffers.get(timestamp).clear();
				} catch (Exception e) {
					System.err.println("Model stream updating failed. Please check your model stream.");
				}
			}
		}
	}

	public static class PrepareBatchSample extends RichFlatMapFunction <Row, Row[]> {
		private static final long serialVersionUID = 3738888745125082777L;
		private final int batchSize;
		private final Row[] bufferedData;
		private int idx = 0;

		public PrepareBatchSample(int batchSize) {
			this.batchSize = batchSize;
			bufferedData = new Row[batchSize];
		}

		@Override
		public void flatMap(Row row, Collector <Row[]> collector) throws Exception {
			bufferedData[idx++] = row;
			if (idx == batchSize) {
				collector.collect(bufferedData);
				idx = 0;
			}
		}
	}

	public static class ModelUpdater extends RichCoFlatMapFunction <Row[], Tuple2 <Long, Object>, Tuple2 <Long,
		Object>>
		implements CheckpointedFunction {
		private static final long serialVersionUID = 7338858137860097282L;

		private final DataBridge dataBridge;
		transient private DenseVector coefficientVector;

		private Object[] labelValues;
		private long gradientVersion = 0L;

		private final double alpha;
		private final double beta;
		private final double l1;
		private final double l2;
		private double[] nParam;
		private double[] zParam;
		private final boolean hasInterceptItem;
		private final int vectorTrainIdx;
		private final int labelIdx;
		private final int[] featureIdx;
		private boolean isUpdatedModel = true;
		/* Tuple4 : coefficient, nParam, zParam, labelValues */
		private transient ListState <Tuple4 <double[], double[], double[], Object[]>> modelState;
		private List <Row[]> localBatchDataBuffer;
		private List <SparseVector> gradientBuffer;
		private final int batchSize;
		private int maxNumBatches;
		// Map<gradIndex, [gradValue, weight]>
		private final Map <Integer, double[]> sparseGradient = new HashMap <>();

		public ModelUpdater(DataBridge dataBridge, Params params, int[] featureIdx, int vectorIdx, int labelIdx) {
			this.dataBridge = dataBridge;
			this.labelValues = null;
			this.alpha = params.get(FtrlTrainParams.ALPHA);
			this.beta = params.get(FtrlTrainParams.BETA);
			this.l1 = params.get(FtrlTrainParams.L_1);
			this.l2 = params.get(FtrlTrainParams.L_2);
			this.batchSize = params.get(FtrlTrainParams.MINI_BATCH_SIZE);
			this.hasInterceptItem = params.get(FtrlTrainParams.WITH_INTERCEPT);
			this.vectorTrainIdx = vectorIdx;
			this.labelIdx = labelIdx;
			this.featureIdx = featureIdx;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.maxNumBatches = Math.max(1, 100000 * getRuntimeContext().getNumberOfParallelSubtasks() / batchSize);
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			Tuple4 <double[], double[], double[], Object[]> state
				= Tuple4.of(coefficientVector.getData(), nParam, zParam, labelValues);
			modelState.clear();
			modelState.add(state);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {

			modelState = context.getOperatorStateStore().getListState(
				new ListStateDescriptor <>("StreamingOnlineModelState",
					TypeInformation.of(new TypeHint <Tuple4 <double[], double[], double[], Object[]>>() {})));

			if (context.isRestored()) {
				// read model data from checkpoint.
				for (Tuple4 <double[], double[], double[], Object[]> state : modelState.get()) {
					this.coefficientVector = new DenseVector(state.f0);
					this.nParam = state.f1;
					this.zParam = state.f2;
					this.labelValues = state.f3;
				}
			} else {
				// read model data from init model.
				List <Row> modelRows = DirectReader.directRead(dataBridge);
				LinearModelData model = new LinearModelDataConverter().load(modelRows);
				this.coefficientVector = model.coefVector;
				this.nParam = new double[this.coefficientVector.size()];
				this.zParam = new double[this.coefficientVector.size()];
				this.labelValues = model.labelValues;
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
						if (hasInterceptItem) {
							vec = vec.prefix(1.0);
						}
						double label = row.getField(labelIdx).equals(labelValues[0]) ? 1.0 : 0.0;
						double p = coefficientVector.dot(vec);
						p = 1 / (1 + Math.exp(-p));
						if (vec instanceof DenseVector) {
							DenseVector denseVec = (DenseVector) vec;
							for (int i = 0; i < coefficientVector.size(); ++i) {
								if (sparseGradient.containsKey(i)) {
									sparseGradient.get(i)[0] += (p - label) * denseVec.getData()[i];
									sparseGradient.get(i)[1] += 1.0;
								} else {
									sparseGradient.put(i, new double[] {(p - label) * denseVec.getData()[i], 1.0});
								}
							}
						} else {
							SparseVector parseVec = (SparseVector) vec;
							for (int i = 0; i < parseVec.getIndices().length; ++i) {
								int idx = parseVec.getIndices()[i];
								if (sparseGradient.containsKey(idx)) {
									sparseGradient.get(idx)[0] += (p - label) * parseVec.getValues()[i];
									sparseGradient.get(idx)[1] += 1.0;
								} else {
									sparseGradient.put(idx, new double[] {(p - label) * parseVec.getValues()[i], 1.0});
								}
							}
						}
					}
				}
				localBatchDataBuffer.clear();
				isUpdatedModel = false;
				out.collect(Tuple2.of(gradientVersion++, sparseGradient));
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
			if (value.f1 instanceof SparseVector) {
				/* Updates model. */
				gradientBuffer.add((SparseVector) value.f1);
				if (!isUpdatedModel) {
					updateModel(coefficientVector, gradientBuffer.remove(0));
					isUpdatedModel = true;
				}
			} else if (value.f1 instanceof DenseVector) {
				/* Rebase model. */
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("rebase linear model.");
				}
				this.coefficientVector = (DenseVector) value.f1;
				nParam = new double[this.coefficientVector.size()];
				zParam = new double[this.coefficientVector.size()];
			} else if (value.f1 instanceof Long) {
				/* Collects model to down stream. */
				out.collect(Tuple2.of(-1L, Tuple2.of(labelValues, coefficientVector)));
			} else {
				throw new AkUnclassifiedErrorException("feedback data type err, must be a Map, Long, or DenseVector.");
			}
		}

		private void updateModel(DenseVector coefficient, SparseVector grad) {
			int[] indices = grad.getIndices();
			double[] values = grad.getValues();
			for (int i = 0; i < indices.length; ++i) {
				int idx = indices[i];
				double sigma = (Math.sqrt(nParam[idx] + values[i] * values[i]) - Math.sqrt(nParam[idx])) / alpha;
				zParam[idx] += values[i] - sigma * coefficient.getData()[idx];
				nParam[idx] += values[i] * values[i];

				if (Math.abs(zParam[idx]) <= l1) {
					coefficient.set(idx, 0.0);
				} else {
					coefficient.set(idx, ((zParam[idx] < 0 ? -1 : 1) * l1 - zParam[idx]) / ((beta + Math.sqrt(
						nParam[idx])) / alpha + l2));
				}
			}
		}
	}

	public static class WriteModel extends RichFlatMapFunction <Tuple2 <Long, Object>, Row> {
		private static final long serialVersionUID = 828728999893377750L;
		private final StringTypeEnum type;
		private final String vectorColName;
		private final String[] featureCols;
		private final boolean hasInterceptItem;

		public WriteModel(TypeInformation <?> type, String vectorColName, String[] featureCols,
						  boolean hasInterceptItem) {
			this.type = StringTypeEnum.valueOf(type.toString().toUpperCase());
			this.vectorColName = vectorColName;
			this.featureCols = featureCols;
			this.hasInterceptItem = hasInterceptItem;
		}

		@Override
		public void flatMap(Tuple2 <Long, Object> value, Collector <Row> out) throws Exception {
			Tuple2 <Object, DenseVector> t2 = (Tuple2 <Object, DenseVector>) value.f1;
			LinearModelData modelData = new LinearModelData();
			modelData.coefVector = t2.f1;
			modelData.hasInterceptItem = this.hasInterceptItem;
			modelData.vectorColName = this.vectorColName;
			modelData.modelName = "Logistic Regression";
			modelData.featureNames = this.featureCols;
			modelData.labelValues = (Object[]) t2.f0;
			modelData.vectorSize = hasInterceptItem ? modelData.coefVector.size() - 1 : modelData.coefVector.size();
			modelData.linearModelType = LinearModelType.LR;

			RowCollector listCollector = new RowCollector();
			new LinearModelDataConverter().save(modelData, listCollector);
			List <Row> rows = listCollector.getRows();
			long time = System.currentTimeMillis();
			for (Row r : rows) {
				int rowSize = r.getArity();
				Row row = new Row(rowSize + 2);
				row.setField(0, new Timestamp(time));
				row.setField(1, (long) rows.size());

				for (int j = 0; j < rowSize; ++j) {
					if (j == 2 && r.getField(j) != null) {
						if (type.equals(StringTypeEnum.BIGINT) || type.equals(StringTypeEnum.LONG)) {
							row.setField(2 + j, Double.valueOf(r.getField(j).toString()).longValue());
						} else if (type.equals(StringTypeEnum.INT) || type.equals(StringTypeEnum.INTEGER)) {
							row.setField(2 + j, Double.valueOf(r.getField(j).toString()).intValue());
						} else if (type.equals(StringTypeEnum.DOUBLE)) {
							row.setField(2 + j, Double.valueOf(r.getField(j).toString()));
						} else if (type.equals(StringTypeEnum.FLOAT)) {
							row.setField(2 + j, Double.valueOf(r.getField(j).toString()).floatValue());
						} else {
							row.setField(2 + j, r.getField(j));
						}
					} else {
						row.setField(2 + j, r.getField(j));
					}
				}
				out.collect(row);
			}
		}
	}
}