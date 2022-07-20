package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithFirstInputSpec;
import com.alibaba.alink.common.exceptions.AkFlinkExecutionErrorException;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelDataConverter;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansPredictModelData;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansTrainModelData;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansUtil;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil.AllDataMerge;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.clustering.StreamingKMeansParams;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Streaming version of Kmeans. It supports online learning and inference of kmeans model.
 */
@InputPorts(values = {
	@PortSpec(value = PortType.MODEL, opType = OpType.BATCH),
	@PortSpec(value = PortType.DATA),
	@PortSpec(value = PortType.DATA, isOptional = true)
})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT),
	@PortSpec(value = PortType.MODEL, desc = PortDesc.KMEANS_MODEL),
})
@ReservedColsWithFirstInputSpec
@NameCn("流式K均值聚类")
public final class StreamingKMeansStreamOp extends StreamOperator <StreamingKMeansStreamOp>
	implements StreamingKMeansParams <StreamingKMeansStreamOp> {
	private static final long serialVersionUID = -7631814863449716946L;
	private BatchOperator batchModel;

	public StreamingKMeansStreamOp(BatchOperator batchModel) {
		super(new Params());
		this.batchModel = batchModel;
	}

	public StreamingKMeansStreamOp(BatchOperator batchModel, Params params) {
		super(params);
		this.batchModel = batchModel;
	}

	public static KMeansTrainModelData initModel(DataBridge modelDataBridge) {
		List <Row> modelRows = DirectReader.directRead(modelDataBridge);
		KMeansPredictModelData predictModelData = new KMeansModelDataConverter().load(modelRows);
		return KMeansUtil.transformPredictDataToTrainData(predictModelData);
	}

	enum PredType {
		PRED,
		PRED_CLUS,
		PRED_DIST,
		PRED_CLUS_DIST;

		static PredType fromInputs(Params params) {
			if (!params.contains(StreamingKMeansParams.PREDICTION_CLUSTER_COL) && !params.contains(
				StreamingKMeansParams.PREDICTION_DISTANCE_COL)) {
				return PRED;
			} else if (!params.contains(StreamingKMeansParams.PREDICTION_CLUSTER_COL)) {
				return PRED_DIST;
			} else if (!params.contains(StreamingKMeansParams.PREDICTION_DISTANCE_COL)) {
				return PRED_CLUS;
			} else {
				return PRED_CLUS_DIST;
			}
		}
	}

	/**
	 * Update model with stream in1, predict for stream in2
	 */
	@Override
	public StreamingKMeansStreamOp linkFrom(StreamOperator <?>... inputs) {
		checkMinOpSize(1, inputs);

		StreamOperator <?> in1 = inputs[0];
		StreamOperator <?> in2 = inputs[0];
		if (inputs.length > 1) {
			in2 = inputs[1];
		}

		if (!this.getParams().contains(HasPredictionCol.PREDICTION_COL)) {
			this.setPredictionCol("cluster_id");
		}
		/**
		 * time interval for updating the model, in seconds
		 */
		final long timeInterval = getParams().get(TIME_INTERVAL);
		final long halfLife = getParams().get(HALF_LIFE);
		final double decayFactor = Math.pow(0.5, (double) timeInterval / (double) halfLife);

		DataStream <Row> trainingData = in1.getDataStream();
		DataStream <Row> predictData = in2.getDataStream();

		PredType predType = PredType.fromInputs(getParams());

		OutputColsHelper outputColsHelper = null;
		switch (predType) {
			case PRED: {
				outputColsHelper = new OutputColsHelper(in2.getSchema(),
					new String[] {getPredictionCol()}, new TypeInformation[] {Types.LONG}, this.getReservedCols());
				break;
			}
			case PRED_CLUS: {
				outputColsHelper = new OutputColsHelper(in2.getSchema(),
					new String[] {getPredictionCol(), getPredictionClusterCol()},
					new TypeInformation[] {Types.LONG, AlinkTypes.DENSE_VECTOR},
					this.getReservedCols());
				break;

			}
			case PRED_DIST: {
				outputColsHelper = new OutputColsHelper(in2.getSchema(),
					new String[] {getPredictionCol(), getPredictionDistanceCol()},
					new TypeInformation[] {Types.LONG, Types.DOUBLE},
					this.getReservedCols());
				break;
			}
			case PRED_CLUS_DIST: {
				outputColsHelper = new OutputColsHelper(in2.getSchema(),
					new String[] {this.getPredictionCol(), getPredictionClusterCol(), getPredictionDistanceCol()},
					new TypeInformation[] {Types.LONG, AlinkTypes.DENSE_VECTOR, Types.DOUBLE},
					this.getReservedCols());
			}
		}

		// for direct read
		DataBridge modelDataBridge = DirectReader.collect(batchModel);

		// incremental train on every window of data
		DataStream <Tuple3 <DenseVector[], int[], Long>> updateData = trainingData
			.flatMap(new CollectUpdateData(modelDataBridge, in1.getColNames(), timeInterval))
			.name("local_aggregate");

		int taskNum = updateData.getParallelism();

		DataStream <KMeansTrainModelData> streamModel = updateData
			.flatMap(new AllDataMerge(taskNum))
			.name("global_aggregate")
			.setParallelism(1)
			.map(new UpdateModelOp(modelDataBridge, decayFactor))
			.name("update_model")
			.setParallelism(1);

		// predict
		DataStream <Row> predictResult = predictData
			.connect(streamModel.broadcast())
			.flatMap(new PredictOp(modelDataBridge, in2.getColNames(), outputColsHelper, predType))
			.name("kmeans_prediction");

		this.setOutput(predictResult, outputColsHelper.getResultSchema());
		this.setSideOutputTables(outputModel(streamModel, getMLEnvironmentId()));

		return this;
	}

	private static class UpdateModelOp extends RichMapFunction <Tuple2 <DenseVector[], int[]>, KMeansTrainModelData>
		implements CheckpointedFunction {
		private static final long serialVersionUID = 4161086998242051550L;
		private static final Logger LOG = LoggerFactory.getLogger(UpdateModelOp.class);
		transient private KMeansTrainModelData modelData;
		private DataBridge modelDataBridge;
		private double decayFactor;
		private transient ListState <Tuple2 <String, List <String>>> modelState;

		@Override
		public void open(Configuration params) {
			if (getRuntimeContext().getNumberOfParallelSubtasks() > 1) {
				throw new AkIllegalStateException("The parallelism of UpdateModelOp should be one.");
			}
		}

		UpdateModelOp(DataBridge modelDataBridge, double decayFactor) {
			this.modelDataBridge = modelDataBridge;
			this.decayFactor = decayFactor;
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			LOG.info("StreamingKMeans: initializeState");

			ListStateDescriptor <Tuple2 <String, List <String>>> descriptor =
				new ListStateDescriptor("StreamingKMeansModelState",
					TypeInformation.of(new TypeHint <Tuple2 <String, List <String>>>() {}));

			modelState = context.getOperatorStateStore().getListState(descriptor);

			if (context.isRestored()) {
				for (Tuple2 <String, List <String>> state : modelState.get()) {
					LOG.info("Loading state ...");
					this.modelData = KMeansUtil.loadModelForTrain(Params.fromJson(state.f0), state.f1);
					LOG.info("Loading state ... OK");
				}
			} else {
				this.modelData = initModel(modelDataBridge);
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			LOG.info("StreamingKMeans: snapshotState at checkpoint");
			Tuple2 <Params, Iterable <String>> serialized = new KMeansModelDataConverter().serializeModel(
				this.modelData);
			List <String> clusters = new ArrayList <>();
			serialized.f1.forEach(clusters::add);
			Tuple2 <String, List <String>> state = Tuple2.of(serialized.f0.toJson(), clusters);
			modelState.clear();
			modelState.add(state);
		}

		@Override
		public KMeansTrainModelData map(Tuple2 <DenseVector[], int[]> tuple2) throws Exception {
			//System.out.println(tuple2);
			//System.out.println(modelData.centroids.get(0).weight + ", " + modelData.centroids.get(1).weight);
			updateModel(tuple2.f0, tuple2.f1, decayFactor);
			//System.out.println(modelData.centroids.get(0).weight + ", " + modelData.centroids.get(1).weight);
			return modelData;
		}

		/**
		 * Update the model given: -# the original model (centroids with weights) -# a batch of new samples -#
		 * decayFactor
		 */
		void updateModel(DenseVector[] sum, int[] clusterCount, double decayFactor) {
			// update cluster centers and weights
			for (int i = 0; i < modelData.centroids.size(); i++) {
				double oldWeight = modelData.getClusterWeight(i) * decayFactor;
				double newWeight = oldWeight + (double) clusterCount[i];
				modelData.setClusterWeight(i, modelData.getClusterWeight(i) + clusterCount[i]);

				double[] vec = modelData.getClusterVector(i).getData();
				if (clusterCount[i] > 0) {
					double frac = oldWeight / newWeight;
					for (int j = 0; j < sum[i].size(); j++) {
						vec[j] = vec[j] * frac + sum[i].get(j) / newWeight;
					}
				}
			}
		}
	}

	/**
	 * Merge data from different windows.
	 */
	public static class AllDataMerge implements
		FlatMapFunction <Tuple3 <DenseVector[], int[], Long>, Tuple2 <DenseVector[], int[]>> {
		private static final long serialVersionUID = 6157375461626497956L;
		private static final Logger LOG = LoggerFactory.getLogger(AllDataMerge.class);
		//WindowHashcode, count, delta
		private HashMap <Long, Tuple3 <Long, DenseVector[], int[]>> map = new HashMap <>();
		private int taskNum;

		AllDataMerge(int taskNum) {
			this.taskNum = taskNum;
		}

		@Override
		public void flatMap(Tuple3 <DenseVector[], int[], Long> tuple,
							Collector <Tuple2 <DenseVector[], int[]>> collector) {
			if (map.containsKey(tuple.f2)) {
				Tuple3 <Long, DenseVector[], int[]> origin = map.get(tuple.f2);
				origin.f0++;
				for (int i = 0; i < tuple.f0.length; i++) {
					origin.f1[i].plusEqual(tuple.f0[i]);
					origin.f2[i] += tuple.f1[i];
				}
				if (origin.f0 == taskNum) {
					collector.collect(Tuple2.of(origin.f1, origin.f2));
					map.remove(tuple.f2);
				}
			} else {
				if (1 == taskNum) {
					collector.collect(Tuple2.of(tuple.f0, tuple.f1));
				} else {
					map.put(tuple.f2, Tuple3.of(1L, tuple.f0, tuple.f1));
				}
			}
			LOG.info("MapHashSet: " + map.keySet());
		}
	}

	static Table[] outputModel(DataStream <KMeansTrainModelData> dataStream, long enviromentId) {
		DataStream <Row> outputModel = dataStream.flatMap(new pipeModel());

		TableSchema schema = new KMeansModelDataConverter().getModelSchema();

		TypeInformation[] types = new TypeInformation[schema.getFieldTypes().length + 2];
		String[] names = new String[schema.getFieldTypes().length + 2];
		names[0] = ModelStreamUtils.MODEL_STREAM_TIMESTAMP_COLUMN_NAME;
		names[1] = ModelStreamUtils.MODEL_STREAM_COUNT_COLUMN_NAME;
		types[0] = ModelStreamUtils.MODEL_STREAM_TIMESTAMP_COLUMN_TYPE;
		types[1] = ModelStreamUtils.MODEL_STREAM_COUNT_COLUMN_TYPE;

		for (int i = 0; i < schema.getFieldTypes().length; ++i) {
			types[i + 2] = schema.getFieldTypes()[i];
			names[i + 2] = schema.getFieldNames()[i];
		}

		return new Table[] {DataStreamConversionUtil.toTable(enviromentId, outputModel, new TableSchema(names,
			types))};
	}

	public static class pipeModel implements FlatMapFunction <KMeansTrainModelData, Row> {
		private static final long serialVersionUID = -6252541197996341634L;

		@Override
		public void flatMap(KMeansTrainModelData linearModel, Collector <Row> collector) throws Exception {
			RowCollector listCollector = new RowCollector();
			new KMeansModelDataConverter().save(linearModel, listCollector);
			List <Row> rows = listCollector.getRows();
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			for (Row r : rows) {
				int rowSize = r.getArity();
				Row row = new Row(rowSize + 2);
				row.setField(0, timestamp);
				row.setField(1, (long) rows.size());

				for (int j = 0; j < rowSize; ++j) {
					row.setField(2 + j, r.getField(j));
				}
				collector.collect(row);
			}
		}
	}

	private static class CollectUpdateData extends
		RichFlatMapFunction <Row, Tuple3 <DenseVector[], int[], Long>> {
		private static final long serialVersionUID = -4082462361419657027L;
		private static final Logger LOG = LoggerFactory.getLogger(CollectUpdateData.class);
		private DataBridge modelDataBridge;
		private String[] trainColNames;
		private long startTime, timeInterval, windowHashCode;
		private transient DenseVector[] sum;
		private transient int[] clusterCount;
		private transient KMeansTrainModelData modelData;
		private transient int[] colIdx;
		private transient ContinuousDistance distance;

		@Override
		public void open(Configuration params) {
			this.modelData = initModel(modelDataBridge);
			this.startTime = System.currentTimeMillis();
			this.windowHashCode = 1L;
			sum = new DenseVector[this.modelData.params.k];
			for (int i = 0; i < modelData.params.k; i++) {
				sum[i] = DenseVector.zeros(modelData.params.vectorSize);
			}
			clusterCount = new int[modelData.params.k];
			distance = modelData.params.distanceType.getFastDistance();

			this.colIdx = KMeansUtil.getKmeansPredictColIdxs(this.modelData.params, trainColNames);
		}

		CollectUpdateData(DataBridge modelDataBridge, String[] trainColNames, long timeInterval) {
			this.modelDataBridge = modelDataBridge;
			this.trainColNames = trainColNames;
			this.timeInterval = timeInterval;
		}

		@Override
		public void flatMap(Row row, Collector <Tuple3 <DenseVector[], int[], Long>> out) throws Exception {
			long curTime = System.currentTimeMillis();
			if (curTime - startTime > timeInterval * windowHashCode * 1000L) {
				LOG.info("TaskId: " + this.getRuntimeContext().getIndexOfThisSubtask() + ", TriggerHashCode: "
					+ windowHashCode);
				out.collect(Tuple3.of(sum, clusterCount, windowHashCode++));
				sum = new DenseVector[this.modelData.params.k];
				for (int i = 0; i < modelData.params.k; i++) {
					sum[i] = DenseVector.zeros(modelData.params.vectorSize);
				}
				clusterCount = new int[modelData.params.k];
			} else {
				Vector record = KMeansUtil.getKMeansPredictVector(colIdx, row);

				Tuple2 <Integer, Double> tuple = KMeansUtil.getClosestClusterIndex(modelData, record, distance);
				clusterCount[tuple.f0]++;
				sum[tuple.f0].plusEqual(record);
			}
		}
	}

	public static class PredictOp extends RichCoFlatMapFunction <Row, KMeansTrainModelData, Row> {
		private static final long serialVersionUID = 1824350851125007591L;
		private DataBridge modelDataBridge;
		private String[] trainColNames;
		private OutputColsHelper outputColsHelper;
		private transient ContinuousDistance distance;
		private transient KMeansTrainModelData modelData;
		private transient int[] colIdx;
		private PredType predType;

		@Override
		public void open(Configuration params) {
			this.modelData = initModel(modelDataBridge);
			this.distance = this.modelData.params.distanceType.getFastDistance();
			this.colIdx = KMeansUtil.getKmeansPredictColIdxs(this.modelData.params,
				trainColNames);
		}

		PredictOp(DataBridge modelDataBridge, String[] testColNames, OutputColsHelper outputColsHelper,
				  PredType predType) {
			this.outputColsHelper = outputColsHelper;
			this.trainColNames = testColNames;
			this.modelDataBridge = modelDataBridge;
			this.predType = predType;
		}

		@Override
		public void flatMap1(Row row, Collector <Row> out) {
			Vector record = KMeansUtil.getKMeansPredictVector(colIdx, row);

			Tuple2 <Integer, Double> tuple2 = KMeansUtil.getClosestClusterIndex(modelData, record, distance);
			long clusterId = modelData.getClusterId(tuple2.f0);
			DenseVector vec = modelData.getClusterVector(tuple2.f0);
			switch (predType) {
				case PRED: {
					out.collect(outputColsHelper.getResultRow(row, Row.of(clusterId)));
					break;
				}
				case PRED_CLUS: {
					out.collect(outputColsHelper.getResultRow(row, Row.of(clusterId, vec)));

					break;

				}
				case PRED_DIST: {
					out.collect(outputColsHelper.getResultRow(row, Row.of(clusterId, tuple2.f1)));

					break;
				}
				case PRED_CLUS_DIST: {
					out.collect(outputColsHelper.getResultRow(row, Row.of(clusterId, vec, tuple2.f1)));
				}
			}
		}

		@Override
		public void flatMap2(KMeansTrainModelData value, Collector <Row> out) {
			this.modelData = value;
		}
	}
}
