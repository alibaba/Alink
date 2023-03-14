package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkUnimplementedOperationException;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.MapperChain;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.modelstream.ModelStreamUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.onlinelearning.kernel.FmOnlineLearningKernel;
import com.alibaba.alink.operator.stream.onlinelearning.kernel.LinearOnlineLearningKernel;
import com.alibaba.alink.operator.stream.onlinelearning.kernel.OnlineLearningKernel;
import com.alibaba.alink.operator.stream.onlinelearning.kernel.SoftmaxOnlineLearningKernel;
import com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp;
import com.alibaba.alink.operator.stream.source.NumSeqSourceStreamOp;
import com.alibaba.alink.params.onlinelearning.OnlineLearningTrainParams;
import com.alibaba.alink.pipeline.ModelExporterUtils.StageNode;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.PipelineStageBase;
import com.alibaba.alink.pipeline.classification.FmClassificationModel;
import com.alibaba.alink.pipeline.classification.LinearSvmModel;
import com.alibaba.alink.pipeline.classification.LogisticRegressionModel;
import com.alibaba.alink.pipeline.classification.SoftmaxModel;
import com.alibaba.alink.pipeline.regression.FmRegressionModel;
import com.alibaba.alink.pipeline.regression.LinearRegressionModel;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.pipeline.ModelExporterUtils.deserializePipelineStagesFromMeta;
import static com.alibaba.alink.pipeline.ModelExporterUtils.loadMapperListFromStages;
import static com.alibaba.alink.pipeline.ModelExporterUtils.loadStagesFromPipelineModel;

/**
 * Online learning of pipeline model.
 * <p>
 * This operator update the last stage of the the pipeline model with online learning algorithm. The other stages will
 * be updated by rebase action. The rebase action is set by param modelStreamFilePath, if this param is set, operator
 * will scan this path and rebase pipeline model with this model stream.
 */
@NameCn("在线学习")
@NameEn("Online learning")
public class OnlineLearningStreamOp extends StreamOperator <OnlineLearningStreamOp>
	implements OnlineLearningTrainParams <OnlineLearningStreamOp> {

	private static final long serialVersionUID = 3688413917992858013L;
	private final DataBridge dataBridge;
	private final String modelSchemeStr;

	public OnlineLearningStreamOp(PipelineModel model) {
		this(model.save(), new Params());
	}

	public OnlineLearningStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public OnlineLearningStreamOp(BatchOperator <?> model, Params params) {
		super(params);
		if (model != null) {
			dataBridge = DirectReader.collect(model);
			modelSchemeStr = TableUtil.schema2SchemaStr(model.getSchema());
		} else {
			throw new AkIllegalArgumentException("Online algo: initial model is null. Please set the initial model.");
		}
	}

	@Override
	public OnlineLearningStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamExecutionEnvironment streamEnv = MLEnvironmentFactory.get(getMLEnvironmentId())
			.getStreamExecutionEnvironment();
		checkOpSize(1, inputs);
		final String dataSchemeStr = TableUtil.schema2SchemaStr(inputs[0].getSchema());
		int parallelism = streamEnv.getParallelism();
		streamEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		streamEnv.getCheckpointConfig().setForceCheckpointing(true);
		streamEnv.setBufferTimeout(20);
		Params params = getParams();

		final int timeInterval = getTimeInterval();
		DataStream <Tuple2 <Long, Object>> modelSaveHandler = new NumSeqSourceStreamOp(0, 0)
			.setMLEnvironmentId(getMLEnvironmentId())
			.getDataStream().flatMap(new FlatMapFunction <Row, Tuple2 <Long, Object>>() {
				@Override
				public void flatMap(Row row, Collector <Tuple2 <Long, Object>> collector) throws Exception {
					for (long i = 0; i < Long.MAX_VALUE; ++i) {
						Thread.sleep(timeInterval * 1000L);
						collector.collect(Tuple2.of(0L, 0L));
					}
				}
			});

		/* Prepares mini batches for training process. */
		DataStream <Row> batchData = inputs[0].getDataStream().rebalance();

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

		IterativeStream.ConnectedIterativeStreams <Row, Tuple2 <Long, Object>> iteration = batchData.iterate(
			Long.MAX_VALUE).withFeedbackType(TypeInformation.of(new TypeHint <Tuple2 <Long, Object>>() {}));

		DataStream iterativeBody = iteration.flatMap(
			new ModelUpdater(dataBridge, params, modelSchemeStr, dataSchemeStr));

		DataStream <Tuple2 <Long, Object>> result = iterativeBody.filter(new FilterFunction <Tuple2 <Long, Object>>() {
				private static final long serialVersionUID = -5436758453355074895L;

				@Override
				public boolean filter(Tuple2 <Long, Object> t2) {
					// if t2.f0 >= 0: data is gradient and feedback it.
					return (t2.f0 >= 0L);
				}
			}).keyBy(0).countWindowAll(parallelism)
			.reduce(new ReduceGradient())
			.map(new MapFunction <Tuple2 <Long, Object>, Tuple2 <Long, Object>>() {
				@Override
				public Tuple2 <Long, Object> map(Tuple2 <Long, Object> gradient) {
					return gradient;
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
		}).map(new MapFunction <Tuple2 <Long, Object>, Row>() {
			@Override
			public Row map(Tuple2 <Long, Object> t2) {
				return (Row) t2.f1;
			}
		});

		iteration.closeWith(result.union(modelSaveHandler));

		TableSchema schema = TableUtil.schemaStr2Schema(modelSchemeStr);
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

	/**
	 * This CoFlatMap updates model with training data and outputs models with fixed frequency. In flatMap1 function,
	 * the training data is buffered and when model is updated, then using buffered training data to calculate
	 * gradient.
	 * In flatMap2 function: If coming data is reduced gradient(Map), then update model with the gradient. If coming
	 * data is model data (Row), then rebase the model data. If coming data is output signal(Long), then output model
	 * data.
	 */
	public static class ModelUpdater extends RichCoFlatMapFunction <Row, Tuple2 <Long, Object>, Tuple2 <Long,
		Object>>
		implements CheckpointedFunction {
		private static final long serialVersionUID = 7338858137860097282L;

		private final DataBridge dataBridge;
		private long gradientVersion = 0L;
		private int vectorTrainIdx;
		private int labelIdx;
		private int[] featureIdx;
		private boolean isUpdatedModel = true;
		private List <Row> localBatchDataBuffer;
		private List <Object> gradientBuffer;
		private int maxNumBatches;
		private final String modelSchemaStr;
		private final String dataSchemaStr;
		private final Map <Timestamp, List <Row>> buffers = new HashMap <>();
		private List <Row> pipeModelRows;
		private long linearModelId;
		private int[] rowFieldMapping;
		private int idIdx;
		private final Params params;
		private MapperChain mapperChain;
		private OnlineLearningKernel kernel;
		private boolean isOutputModel = true;

		private transient ListState <OnlineLearningKernel> modelState;

		public ModelUpdater(DataBridge dataBridge, Params params,
							String modelSchemaStr, String dataSchemaStr) {
			this.dataBridge = dataBridge;
			this.params = params;
			this.modelSchemaStr = modelSchemaStr;
			this.dataSchemaStr = dataSchemaStr;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.maxNumBatches = Math.max(1, 100000 * getRuntimeContext().getNumberOfParallelSubtasks());
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			modelState.clear();
			modelState.add(kernel);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {

			modelState = context.getOperatorStateStore().getListState(
				new ListStateDescriptor <>("StreamingOnlineModelState",
					TypeInformation.of(OnlineLearningKernel.class)));

			if (context.isRestored()) {
				// read model data from checkpoint.
				for (OnlineLearningKernel state : modelState.get()) {
					this.kernel = state;
				}
			} else if (!context.isRestored()) {
				// read model data from init model.
				pipeModelRows = DirectReader.directRead(dataBridge);
				loadPipelineModel(TableUtil.schemaStr2Schema(modelSchemaStr),
					TableUtil.schemaStr2Schema(dataSchemaStr));
			}
		}

		@Override
		public void flatMap1(Row rows, Collector <Tuple2 <Long, Object>> out) throws Exception {
			if (localBatchDataBuffer == null) {
				localBatchDataBuffer = new ArrayList <>();
				gradientBuffer = new ArrayList <>();
			}
			localBatchDataBuffer.add(rows);
			if (isUpdatedModel) {
				kernel.getGradient().clear();
				for (Row element : localBatchDataBuffer) {
					Row row = mapperChain.map(element);
					Vector vec;
					if (vectorTrainIdx == -1) {
						vec = new DenseVector(featureIdx.length);
						for (int i = 0; i < featureIdx.length; ++i) {
							vec.set(i, Double.parseDouble(row.getField(featureIdx[i]).toString()));
						}
					} else {
						vec = VectorUtil.getVector(row.getField(vectorTrainIdx));
					}
					Object label = row.getField(labelIdx);
					kernel.calcGradient(vec, label);
				}

				localBatchDataBuffer.clear();
				isUpdatedModel = false;
				out.collect(Tuple2.of(gradientVersion++, kernel.getGradient()));
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
				gradientBuffer.add(value.f1);
				if (!isUpdatedModel) {
					kernel.updateModel(gradientBuffer.remove(0));
					isUpdatedModel = true;
					isOutputModel = true;
				}
			} else if (value.f1 instanceof Row) {
				/* Rebase model. */
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
						pipeModelRows = buffers.get(timestamp);
						loadPipelineModel(TableUtil.schemaStr2Schema(modelSchemaStr),
							TableUtil.schemaStr2Schema(dataSchemaStr));
					} catch (Exception e) {
						System.err.println("test Model stream updating failed. Please check your model stream.");
					}
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("rebase pipeline model.");
					}
				}
			} else if (value.f1 instanceof Long) {
				if (isOutputModel) {
					/* Collects model to down stream. */
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("output model begin...");
					}
					List <Row> outputModelRows = new ArrayList <>(pipeModelRows.size());
					for (Row row : pipeModelRows) {
						if ((long) row.getField(idIdx) != linearModelId) {
							outputModelRows.add(row);
						}
					}
					List <Row> lastModelRows = kernel.serializeModel();
					Timestamp time = new Timestamp(System.currentTimeMillis());
					long count = lastModelRows.size() + outputModelRows.size();

					int rowSize = outputModelRows.get(0).getArity();

					for (Row row : outputModelRows) {
						Row outRow = new Row(row.getArity() + 2);
						outRow.setField(0, time);
						outRow.setField(1, count);
						for (int i = 0; i < rowSize; ++i) {
							outRow.setField(i + 2, row.getField(i));
						}
						out.collect(Tuple2.of(-1L, outRow));
					}
					for (Row row : lastModelRows) {
						Row outRow = new Row(rowSize + 2);
						outRow.setField(0, time);
						outRow.setField(1, count);
						outRow.setField(2, linearModelId);

						for (int i = 0; i < rowFieldMapping.length; ++i) {
							outRow.setField(rowFieldMapping[i] + 3, row.getField(i));
						}
						out.collect(Tuple2.of(-1L, outRow));
					}
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("output model OK.");
					}
					isOutputModel = false;
				}
			} else {
				throw new AkIllegalDataException("feedback data type err, must be a Map, Row or Long.");
			}
		}

		private void loadPipelineModel(
			TableSchema modelSchema, TableSchema dataSchema) {
			List <Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>>> stages =
				loadStagesFromPipelineModel(pipeModelRows, modelSchema);
			idIdx = TableUtil.findColIndexWithAssert(modelSchema, "id");
			linearModelId = stages.size();
			PipelineStageBase lastStage = stages.get(stages.size() - 1).f0;
			for (Row row : pipeModelRows) {
				if ((long) row.getField(idIdx) == -1L) {
					StageNode[] stageNodes = deserializePipelineStagesFromMeta(row, modelSchema);
					rowFieldMapping = stageNodes[stageNodes.length - 1].schemaIndices;
					break;
				}
			}
			MapperChain mapperChain = loadMapperListFromStages(stages, dataSchema);
			mapperChain.open();
			Mapper[] mappers = new Mapper[mapperChain.getMappers().length - 1];
			for (int i = 0; i < mapperChain.getMappers().length - 1; ++i) {
				mappers[i] = mapperChain.getMappers()[i];
			}
			if (lastStage instanceof LogisticRegressionModel) {
				this.kernel = new LinearOnlineLearningKernel(params, LinearModelType.LR);
			} else if (lastStage instanceof FmClassificationModel) {
				this.kernel = new FmOnlineLearningKernel(params, true);
			} else if (lastStage instanceof FmRegressionModel) {
				this.kernel = new FmOnlineLearningKernel(params, false);
			} else if (lastStage instanceof SoftmaxModel) {
				this.kernel = new SoftmaxOnlineLearningKernel(params);
			} else if (lastStage instanceof LinearRegressionModel) {
				this.kernel = new LinearOnlineLearningKernel(params, LinearModelType.LinearReg);
			} else if (lastStage instanceof LinearSvmModel) {
				this.kernel = new LinearOnlineLearningKernel(params, LinearModelType.SVM);
			} else {
				throw new AkUnimplementedOperationException(
					"Not support this stage yet, online learning only support LR, FMClassification, FMRegression, SVM,"
						+ " LinearReg, Softmax.");
			}

			this.mapperChain = new MapperChain(mappers);
			TableSchema inputDataSchema = mappers[mappers.length - 1].getOutputSchema();
			kernel.deserializeModel(stages.get(stages.size() - 1).f2);

			featureIdx = kernel.getFeatureIndices(inputDataSchema);
			labelIdx = kernel.getLabelIdx(inputDataSchema);
			vectorTrainIdx = kernel.getVectorIdx(inputDataSchema);
		}
	}

	public static class ReduceGradient implements ReduceFunction <Tuple2 <Long, Object>> {
		@Override
		public Tuple2 <Long, Object> reduce(Tuple2 <Long, Object> o, Tuple2 <Long, Object> t1) {
			Map <Integer, double[]> grad1 = (Map <Integer, double[]>) o.f1;
			Map <Integer, double[]> grad2 = (Map <Integer, double[]>) t1.f1;
			for (int idx : grad2.keySet()) {
				if (grad1.containsKey(idx)) {
					for (int i = 0; i < grad1.get(idx).length; ++i) {
						grad1.get(idx)[i] += grad2.get(idx)[i];
					}
				} else {
					grad1.put(idx, grad2.get(idx));
				}
			}
			return o;
		}
	}
}