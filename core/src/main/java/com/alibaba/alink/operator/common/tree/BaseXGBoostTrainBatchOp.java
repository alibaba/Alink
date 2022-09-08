package com.alibaba.alink.operator.common.tree;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.FeatureColsVectorColMutexRule;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComQueue;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.XGboostException;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.outlier.OutlierUtil;
import com.alibaba.alink.operator.common.tree.xgboost.Booster;
import com.alibaba.alink.operator.common.tree.xgboost.Tracker;
import com.alibaba.alink.operator.common.tree.xgboost.XGBoost;
import com.alibaba.alink.operator.common.tree.xgboost.plugin.XGBoostClassLoaderFactory;
import com.alibaba.alink.params.xgboost.HasObjective.Objective;
import com.alibaba.alink.params.xgboost.XGBoostDebugParams;
import com.alibaba.alink.params.xgboost.XGBoostDebugParams.RunningMode;
import com.alibaba.alink.params.xgboost.XGBoostInputParams;
import com.alibaba.alink.params.xgboost.XGBoostLearningTaskParams;
import com.alibaba.alink.params.xgboost.XGBoostTrainParams;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.MODEL))
@ParamSelectColumnSpec(name = "featureCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@FeatureColsVectorColMutexRule
public abstract class BaseXGBoostTrainBatchOp<T extends BaseXGBoostTrainBatchOp <T>> extends BatchOperator <T> {

	private final static Logger LOG = LoggerFactory.getLogger(BaseXGBoostTrainBatchOp.class);

	public BaseXGBoostTrainBatchOp() {
		this(new Params());
	}

	public BaseXGBoostTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> input = checkAndGetFirst(inputs);

		final Params params = getParams().clone();
		final Objective objective = params.get(XGBoostLearningTaskParams.OBJECTIVE);

		final XGBoostClassLoaderFactory xgBoostClassLoaderFactory = new XGBoostClassLoaderFactory(
			params.get(XGBoostTrainParams.PLUGIN_VERSION)
		);

		final boolean needDistinctLabel =
			objective.equals(Objective.BINARY_LOGISTIC)
				|| objective.equals(Objective.BINARY_LOGITRAW)
				|| objective.equals(Objective.BINARY_HINGE)
				|| objective.equals(Objective.MULTI_SOFTMAX)
				|| objective.equals(Objective.MULTI_SOFTPROB);

		final int labelColIndex = TableUtil.findColIndex(
			input.getColNames(),
			params.get(XGBoostInputParams.LABEL_COL)
		);

		TypeInformation <?> labelType = Types.DOUBLE;

		if (objective.equals(Objective.BINARY_LOGISTIC)
			|| objective.equals(Objective.BINARY_HINGE)
			|| objective.equals(Objective.MULTI_SOFTMAX)
			|| objective.equals(Objective.MULTI_SOFTPROB)) {

			labelType = TableUtil.findColType(
				input.getSchema(),
				params.get(XGBoostInputParams.LABEL_COL)
			);
		}

		DataSet <Object[]> labels = Preprocessing.generateLabels(input, params, !needDistinctLabel);

		input = Preprocessing.castLabel(input, params, labels, !needDistinctLabel);

		TypeInformation <?> castLabelType = TableUtil.findColType(
			input.getSchema(),
			params.get(XGBoostInputParams.LABEL_COL)
		);

		DataSet <Row> trainInput;

		if (params.contains(XGBoostInputParams.VECTOR_COL)) {
			trainInput = Preprocessing
				.select(input, params.get(XGBoostInputParams.VECTOR_COL), params.get(XGBoostInputParams.LABEL_COL))
				.getDataSet();
		} else {
			trainInput = input
				.getDataSet()
				.map(
					new SelectSampleCol(
						TableUtil.findColIndicesWithAssertAndHint(
							input.getColNames(),
							OutlierUtil.uniformFeatureColsDefaultAsAll(
								TableUtil.getNumericCols(input.getSchema()),
								params.get(XGBoostInputParams.FEATURE_COLS)
							)
						),
						labelColIndex
					)
				)
				.returns(new RowTypeInfo(AlinkTypes.DENSE_VECTOR, castLabelType));
		}

		DataSet <Tuple1 <Integer>> vectorSize = trainInput
			.map(new MapFunction <Row, Tuple1 <Integer>>() {
				@Override
				public Tuple1 <Integer> map(Row value) throws Exception {
					Vector vector = VectorUtil.getVector(value.getField(0));

					if (vector instanceof SparseVector) {
						if (vector.size() >= 0) {
							return Tuple1.of(vector.size());
						} else {
							int[] indices = ((SparseVector) vector).getIndices();

							return Tuple1.of(
								indices == null || indices.length == 0 ? 0 : indices[indices.length - 1] + 1);
						}
					} else {
						return Tuple1.of(vector.size());
					}
				}
			})
			.name("Extract vector size")
			.max(0);

		RunningMode runningMode = params.get(XGBoostDebugParams.RUNNING_MODE);

		DataSet <Row> model;

		final TableSchema modelSchema = new XGBoostModelDataConverter(labelType).getModelSchema();

		switch (runningMode) {
			case ICQ:
				model = new ComQueue()
					.initWithPartitionedData("trainData", trainInput)
					.initWithBroadcastData("labels", labels)
					.initWithBroadcastData("vectorSize", vectorSize)
					.add(new InitTracker(xgBoostClassLoaderFactory))
					.add(new Bcast <Tuple2 <String, String>>("workerEnvs", 0, Types.TUPLE(Types.STRING, Types.STRING)))
					.add(new XGBoostTrain(
						params, 0, 1, xgBoostClassLoaderFactory
					))
					.add(new Gather <Boolean>("status", 0, Types.BOOLEAN))
					.add(new RecycleTracker())
					.closeWith(new GenModel())
					.exec();
				break;
			case TRIVIAL:
				final long trackerHandle = IterTaskObjKeeper.getNewHandle();

				final Row emptyModelRow = new RowTypeInfo(modelSchema.getFieldTypes())
					.createSerializer(new ExecutionConfig())
					.createInstance();

				DataSet <Tuple2 <String, String>> workerEnvs = MLEnvironmentFactory
					.get(getMLEnvironmentId())
					.getExecutionEnvironment()
					.fromElements(0)
					.partitionByHash(new KeySelector <Integer, Integer>() {
						@Override
						public Integer getKey(Integer value) {
							return value;
						}
					})
					.mapPartition(new InitTrackMapPartition(xgBoostClassLoaderFactory, trackerHandle))
					.name("Init tracker");

				DataSet <Tuple2 <Boolean, Row>> modelAndStatus = trainInput
					.mapPartition(new XGBoostTrainMapPartition(
						params, 0, 1, emptyModelRow, xgBoostClassLoaderFactory
					))
					.withBroadcastSet(labels, "labels")
					.withBroadcastSet(vectorSize, "vectorSize")
					.withBroadcastSet(workerEnvs, "workerEnvs")
					.name("XGBoost train");

				DataSet <byte[]> recycleResult = modelAndStatus
					.filter(new FilterFunction <Tuple2 <Boolean, Row>>() {
						@Override
						public boolean filter(Tuple2 <Boolean, Row> value) throws Exception {
							return !value.f0;
						}
					})
					.mapPartition(new RecycleTrackerMapPartition(trackerHandle))
					.name("Recycle tracker");

				model = modelAndStatus
					.filter(new FilterFunction <Tuple2 <Boolean, Row>>() {
						@Override
						public boolean filter(Tuple2 <Boolean, Row> value) throws Exception {
							return value.f0;
						}
					})
					.map(new MapFunction <Tuple2 <Boolean, Row>, Row>() {
						@Override
						public Row map(Tuple2 <Boolean, Row> value) throws Exception {
							return value.f1;
						}
					})
					.withBroadcastSet(recycleResult, "recycleResult")
					.name("Gen model");
				break;
			default:
				throw new AkIllegalArgumentException("Illegal running mode: " + runningMode);
		}

		setOutput(model, modelSchema);

		return (T) this;
	}

	public static class InitTracker extends ComputeFunction {
		private final XGBoostClassLoaderFactory xgBoostClassLoaderFactory;

		public InitTracker(
			XGBoostClassLoaderFactory xgBoostClassLoaderFactory) {

			this.xgBoostClassLoaderFactory = xgBoostClassLoaderFactory;
		}

		@Override
		public void calc(ComContext context) {
			if (context.getTaskId() == 0) {
				XGBoost xgBoost = XGBoostClassLoaderFactory
					.create(xgBoostClassLoaderFactory)
					.create();

				Tracker tracker;

				try {
					tracker = xgBoost.initTracker(context.getNumTask());
				} catch (XGboostException e) {
					throw new AkIllegalStateException("XGboost error.", e);
				}

				if (!tracker.start(0L)) {
					throw new AkIllegalStateException("Tracker cannot be started");
				}

				context.putObj("tracker", tracker);

				context.putObj("workerEnvs", tracker.getWorkerEnvs());
			}
		}
	}

	public static class XGBoostTrain extends ComputeFunction {
		private final Params params;
		private final int vectorColIndex;
		private final int labelColIndex;
		private final Objective objective;
		private final XGBoostClassLoaderFactory xgBoostClassLoaderFactory;

		public XGBoostTrain(Params params, int vectorColIndex,
							int labelColIndex, XGBoostClassLoaderFactory xgBoostClassLoaderFactory) {

			this.params = params;
			this.vectorColIndex = vectorColIndex;
			this.labelColIndex = labelColIndex;
			this.objective = params.get(XGBoostLearningTaskParams.OBJECTIVE);
			this.xgBoostClassLoaderFactory = xgBoostClassLoaderFactory;
		}

		@Override
		public void calc(ComContext context) {
			final int vectorSize = context. <List <Tuple1 <Integer>>>getObj("vectorSize").get(0).getField(0);

			XGBoost xgBoost = XGBoostClassLoaderFactory.create(xgBoostClassLoaderFactory).create();

			List <Tuple2 <String, String>> workerEnvsList = new ArrayList <>(
				context. <List <Tuple2 <String, String>>>getObj("workerEnvs")
			);
			workerEnvsList.add(Tuple2.of("DMLC_TASK_ID", String.valueOf(context.getTaskId())));

			try {
				xgBoost.init(workerEnvsList);
			} catch (XGboostException e) {
				throw new AkIllegalStateException("XGBoost init error.", e);
			}

			try (TemporaryClassLoaderContext temporaryClassLoaderContext
					 = TemporaryClassLoaderContext.of(xgBoostClassLoaderFactory.create())) {

				Booster booster = train(
					context. <Iterable <Row>>getObj("trainData").iterator(),
					params,
					labelColIndex,
					vectorColIndex,
					vectorSize,
					xgBoost
				);

				if (context.getTaskId() == 0) {

					List <Row> model = generateModel(
						objective, params.clone(), context.getObj("labels"), vectorSize, booster
					);

					context.putObj("model", model);
				}
			} catch (XGboostException e) {
				throw new AkIllegalStateException("XGBoost error.", e);
			} finally {
				try {
					xgBoost.shutdown();
				} catch (XGboostException e) {
					// pass
					LOG.warn("Shutdown rabit error.", e);
				}
			}

			context.putObj("status", Collections.singletonList(true));
		}
	}

	public static Booster train(
		Iterator <Row> iterator,
		Params params,
		int labelColIndex,
		int vectorColIndex,
		int vectorSize,
		XGBoost xgBoost) throws XGboostException {

		return xgBoost.train(
			iterator,
			row -> row,
			row -> {
				Vector vector = VectorUtil.getVector(row.getField(vectorColIndex));

				if (vector instanceof SparseVector && vector.size() < 0) {
					((SparseVector) vector).setSize(vectorSize);
				}

				return Tuple2.of(
					vector,
					new float[] {((Number) AkPreconditions.checkNotNull(row.getField(labelColIndex))).floatValue()}
				);
			},
			params);
	}

	public static List <Row> generateModel(
		Objective objective, Params params, List <Object[]> labels, int vectorSize, Booster booster)
		throws XGboostException {

		final int subModelSize = 1024;
		final byte[] serialized = booster.toByteArray();
		final int len = serialized.length;
		final int subModelLen = len % subModelSize == 0 ? len / subModelSize
			: len / subModelSize + 1;
		final Encoder base64Encoder = Base64.getEncoder();

		XGBoostModelDataConverter xgBoostModelDataConverter = new XGBoostModelDataConverter();

		xgBoostModelDataConverter.meta = params
			.set(XGBoostModelDataConverter.XGBOOST_VECTOR_SIZE, vectorSize);

		xgBoostModelDataConverter.modelData = () -> new Iterator <String>() {
			int counter = 0;

			@Override
			public boolean hasNext() {
				return counter < subModelLen;
			}

			@Override
			public String next() {
				return base64Encoder.encodeToString(
					ArrayUtils.subarray(
						serialized, counter * subModelSize, (counter++ + 1) * subModelSize
					)
				);
			}
		};

		if (objective.equals(Objective.BINARY_LOGISTIC)
			|| objective.equals(Objective.BINARY_HINGE)
			|| objective.equals(Objective.MULTI_SOFTMAX)
			|| objective.equals(Objective.MULTI_SOFTPROB)) {

			xgBoostModelDataConverter.labels = labels.get(0);
		}

		List <Row> model = new ArrayList <>();

		xgBoostModelDataConverter.save(xgBoostModelDataConverter, new Collector <Row>() {
			@Override
			public void collect(Row record) {
				model.add(record);
			}

			@Override
			public void close() {
				// pass
			}
		});
		return model;
	}

	public static class RecycleTracker extends ComputeFunction {
		@Override
		public void calc(ComContext context) {
			if (context.getTaskId() == 0) {
				Tracker tracker = context.getObj("tracker");
				tracker.stop();
			}
		}
	}

	public static class GenModel extends CompleteResultFunction {
		@Override
		public List <Row> calc(ComContext context) {
			return context.getObj("model");
		}
	}

	public static class InitTrackMapPartition extends RichMapPartitionFunction <Integer, Tuple2 <String, String>> {
		private final XGBoostClassLoaderFactory xgBoostClassLoaderFactory;
		private final long trackerHandle;

		public InitTrackMapPartition(XGBoostClassLoaderFactory xgBoostClassLoaderFactory, long trackerHandle) {
			this.xgBoostClassLoaderFactory = xgBoostClassLoaderFactory;
			this.trackerHandle = trackerHandle;
		}

		@Override
		public void mapPartition(Iterable <Integer> values, Collector <Tuple2 <String, String>> out)
			throws Exception {

			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {

				XGBoost xgBoost = XGBoostClassLoaderFactory
					.create(xgBoostClassLoaderFactory)
					.create();

				Tracker tracker = xgBoost.initTracker(getRuntimeContext().getNumberOfParallelSubtasks());

				if (!tracker.start(0L)) {
					throw new AkIllegalStateException("Tracker cannot be started");
				}

				IterTaskObjKeeper.put(trackerHandle, 0, tracker);

				for (Tuple2 <String, String> workerEnv : tracker.getWorkerEnvs()) {
					out.collect(workerEnv);
				}

			}
		}
	}

	public static class XGBoostTrainMapPartition extends RichMapPartitionFunction <Row, Tuple2 <Boolean, Row>> {

		private final Params params;
		private final int vectorColIndex;
		private final int labelColIndex;
		private final Objective objective;
		private final Row emptyModelRow;
		private final XGBoostClassLoaderFactory xgBoostClassLoaderFactory;

		private transient int vectorSize;
		private transient List <Tuple2 <String, String>> workerEnvsList;
		private transient List <Object[]> labels;

		public XGBoostTrainMapPartition(
			Params params, int vectorColIndex,
			int labelColIndex, Row emptyModelRow,
			XGBoostClassLoaderFactory xgBoostClassLoaderFactory) {

			this.params = params;
			this.vectorColIndex = vectorColIndex;
			this.labelColIndex = labelColIndex;
			this.objective = params.get(XGBoostLearningTaskParams.OBJECTIVE);
			this.emptyModelRow = emptyModelRow;
			this.xgBoostClassLoaderFactory = xgBoostClassLoaderFactory;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			vectorSize = getRuntimeContext()
				.getBroadcastVariableWithInitializer(
					"vectorSize",
					new BroadcastVariableInitializer <Tuple1 <Integer>, Integer>() {
						@Override
						public Integer initializeBroadcastVariable(Iterable <Tuple1 <Integer>> data) {
							return data.iterator().next().f0;
						}
					}
				);

			workerEnvsList = getRuntimeContext()
				.getBroadcastVariableWithInitializer(
					"workerEnvs",
					new BroadcastVariableInitializer <Tuple2 <String, String>, List <Tuple2 <String, String>>>() {
						@Override
						public List <Tuple2 <String, String>> initializeBroadcastVariable(
							Iterable <Tuple2 <String, String>> data) {

							List <Tuple2 <String, String>> workEnvsList = new ArrayList <>();

							for (Tuple2 <String, String> env : data) {
								workEnvsList.add(env);
							}

							return workEnvsList;
						}
					}
				);

			labels = getRuntimeContext()
				.getBroadcastVariableWithInitializer(
					"labels",
					new BroadcastVariableInitializer <Object[], List <Object[]>>() {
						@Override
						public List <Object[]> initializeBroadcastVariable(Iterable <Object[]> data) {
							List <Object[]> labels = new ArrayList <>();

							for (Object[] label : data) {
								labels.add(label);
							}

							return labels;
						}
					}
				);
		}

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Boolean, Row>> out) throws Exception {
			final int taskId = getRuntimeContext().getIndexOfThisSubtask();

			XGBoost xgBoost = XGBoostClassLoaderFactory.create(xgBoostClassLoaderFactory).create();

			List <Tuple2 <String, String>> localWorkerEnvsList = new ArrayList <>(workerEnvsList);
			localWorkerEnvsList.add(Tuple2.of("DMLC_TASK_ID", String.valueOf(taskId)));

			try {
				xgBoost.init(localWorkerEnvsList);
			} catch (XGboostException e) {
				throw new AkIllegalStateException("XGBoost init error.", e);
			}

			try (TemporaryClassLoaderContext temporaryClassLoaderContext
					 = TemporaryClassLoaderContext.of(xgBoostClassLoaderFactory.create())) {

				Booster booster = train(
					values.iterator(),
					params,
					labelColIndex,
					vectorColIndex,
					vectorSize,
					xgBoost
				);

				if (taskId == 0) {

					List <Row> model = generateModel(
						objective, params.clone(), labels, vectorSize, booster
					);

					for (Row row : model) {
						out.collect(Tuple2.of(true, row));
					}
				}
			} catch (XGboostException e) {
				throw new AkIllegalStateException("XGBoost error", e);
			} finally {
				try {
					xgBoost.shutdown();
				} catch (XGboostException e) {
					// pass
					LOG.warn("Shutdown rabit error.", e);
				}
			}

			out.collect(Tuple2.of(false, emptyModelRow));
		}
	}

	public static class RecycleTrackerMapPartition extends RichMapPartitionFunction <Tuple2 <Boolean, Row>, byte[]> {
		private final long trackerHandle;

		public RecycleTrackerMapPartition(long trackerHandle) {this.trackerHandle = trackerHandle;}

		@Override
		public void mapPartition(Iterable <Tuple2 <Boolean, Row>> values, Collector <byte[]> out)
			throws Exception {

			values.iterator().next();

			Tracker tracker = IterTaskObjKeeper.get(trackerHandle, 0);

			if (tracker != null) {
				tracker.stop();
			}

			IterTaskObjKeeper.clear(trackerHandle);
		}
	}

	public static class SelectSampleCol implements MapFunction <Row, Row> {
		private final int[] featureColIndices;
		private final int labelColIndex;

		public SelectSampleCol(int[] featureColIndices, int labelColIndex) {
			this.featureColIndices = featureColIndices;
			this.labelColIndex = labelColIndex;
		}

		@Override
		public Row map(Row value) {
			return Row.of(
				OutlierUtil.rowToDenseVector(value, featureColIndices, featureColIndices.length),
				value.getField(labelColIndex)
			);
		}
	}
}
