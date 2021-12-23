package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
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
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.onlinelearning.OnlineTrainParams;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Online algorithm receive train data streams, using the training samples to update model element by element, and
 * output
 * model after every time interval.
 */

@Internal
public abstract class BaseOnlineTrainStreamOp<T extends BaseOnlineTrainStreamOp <T>> extends StreamOperator <T> {

	private static final long serialVersionUID = 5419987051076832208L;
	DataBridge dataBridge = null;
	private OnlineLearningKernel kernel;

	public BaseOnlineTrainStreamOp(BatchOperator model) throws Exception {
		super(new Params());
		if (model != null) {
			dataBridge = DirectReader.collect(model);
		} else {
			throw new IllegalArgumentException("Online algo: initial model is null. Please set a valid initial model"
				+ ".");
		}
	}

	public BaseOnlineTrainStreamOp(BatchOperator model, Params params) throws Exception {
		super(params);
		if (model != null) {
			dataBridge = DirectReader.collect(model);
		} else {
			throw new IllegalArgumentException("Online algo: initial model is null. Please set a valid initial model"
				+ ".");
		}
	}

	public void setLearningKernel(OnlineLearningKernel subFunc) {
		this.kernel = subFunc;
	}

	private static int[] getSplitInfo(int featureSize, boolean hasInterceptItem, int parallelism) {
		int coefSize = (hasInterceptItem) ? featureSize + 1 : featureSize;
		int subSize = coefSize / parallelism;
		int[] poses = new int[parallelism + 1];
		int offset = coefSize % parallelism;
		for (int i = 0; i < offset; ++i) {
			poses[i + 1] = poses[i] + subSize + 1;
		}
		for (int i = offset; i < parallelism; ++i) {
			poses[i + 1] = poses[i] + subSize;
		}
		return poses;
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		StreamExecutionEnvironment
			streamEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment();
		int parallelism = streamEnv.getParallelism();
		streamEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		streamEnv.getCheckpointConfig().setForceCheckpointing(true);

		checkOpSize(1, inputs);

		boolean hasInterceptItem = getParams().get(OnlineTrainParams.WITH_INTERCEPT);
		String vecColName = getParams().get(OnlineTrainParams.VECTOR_COL);
		int vectorTrainIdx = vecColName != null ?
			TableUtil.findColIndexWithAssertAndHint(inputs[0].getColNames(), vecColName) : -1;
		int labelIdx = TableUtil.findColIndexWithAssertAndHint(inputs[0].getColNames(),
			getParams().get(OnlineTrainParams.LABEL_COL));
		String[] featureCols = getParams().get(OnlineTrainParams.FEATURE_COLS);
		int[] featureIdx = null;
		int featureColLength = -1;
		if (vectorTrainIdx == -1) {
			featureIdx = new int[featureCols.length];
			for (int i = 0; i < featureCols.length; ++i) {
				featureIdx[i] = TableUtil.findColIndexWithAssertAndHint(inputs[0].getColNames(), featureCols[i]);
			}
			featureColLength = featureCols.length;
		}
		final TypeInformation labelType = inputs[0].getColTypes()[labelIdx];

		int featureSize = vectorTrainIdx != -1 ? getParams().get(OnlineTrainParams.VECTOR_SIZE) : featureColLength;
		final int[] splitInfo = getSplitInfo(featureSize, hasInterceptItem, parallelism);

		DataStream <Tuple3 <Long, Vector, Object>> initData = inputs[0].getDataStream().map(
			new ParseSample(hasInterceptItem, vectorTrainIdx, featureIdx, labelIdx));

		// train data format = Tuple3<sampleId, SparseVector(subSample), label>
		// feedback format = Tuple4<sampleId, sum_wx>
		IterativeStream.ConnectedIterativeStreams <Tuple3 <Long, Vector, Object>,
			Tuple2 <Long, Object>>
			iteration = initData.iterate(Long.MAX_VALUE)
			.withFeedbackType(TypeInformation
				.of(new TypeHint <Tuple2 <Long, Object>>() {
				}));

		DataStream iterativeBody = iteration.flatMap(new AppendWx())
			.flatMap(new SplitVector(splitInfo, hasInterceptItem, featureSize))
			.partitionCustom(new SubVectorPartitioner(), 1)
			.flatMap(new CalcTask(dataBridge, splitInfo, getParams(), kernel, featureSize))
			.keyBy(0)
			.flatMap(new ReduceTask(parallelism, splitInfo, kernel))
			.partitionCustom(new WxPartitioner(), 0);

		DataStream <Tuple2 <Long, Object>>
			result = iterativeBody.filter(
			new FilterFunction <Tuple2 <Long, Object>>() {
				private static final long serialVersionUID = -5436758453355074895L;

				@Override
				public boolean filter(Tuple2 <Long, Object> t2)
					throws Exception {
					// if t2.f0 >= 0 then feedback
					return (t2.f0 >= 0);
				}
			});

		DataStream <Row> output = iterativeBody.filter(
			new FilterFunction <Tuple2 <Long, Object>>() {
				private static final long serialVersionUID = 4204787383191799107L;

				@Override
				public boolean filter(Tuple2 <Long, Object> t2)
					throws Exception {
					/* if t2.f0 small than 0, then output */
					return t2.f0 < 0;
				}
			}).flatMap(new WriteModel(labelType, vecColName, featureCols, hasInterceptItem));

		iteration.closeWith(result);

		TableSchema schema = new LinearModelDataConverter(labelType).getModelSchema();

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

		this.setOutput(output, names, types);
		return (T) this;
	}

	public static class WriteModel
		extends RichFlatMapFunction <Tuple2 <Long, Object>, Row> {
		private static final long serialVersionUID = 828728999893377750L;
		private StringTypeEnum type;
		private String vectorColName;
		private String[] featureCols;
		private boolean hasInterceptItem;

		public WriteModel(TypeInformation type, String vectorColName, String[] featureCols, boolean hasInterceptItem) {
			this.type = StringTypeEnum.valueOf(type.toString().toUpperCase());
			this.vectorColName = vectorColName;
			this.featureCols = featureCols;
			this.hasInterceptItem = hasInterceptItem;
		}

		@Override
		public void flatMap(Tuple2 <Long, Object> value, Collector <Row> out)
			throws Exception {
			Tuple2 <Object, double[]> t2 = (Tuple2 <Object, double[]>) value.f1;
			LinearModelData modelData = new LinearModelData();
			modelData.coefVector = new DenseVector(t2.f1);
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
			long time =  System.currentTimeMillis();
			for (Row r : rows) {
				int rowSize = r.getArity();
				Row row = new Row(rowSize + 2);
				row.setField(0, new Timestamp(time));
				row.setField(1, rows.size() + 0L);

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

	public static class ParseSample
		extends RichMapFunction <Row, Tuple3 <Long, Vector, Object>> {
		private static final long serialVersionUID = 3738888745125082777L;
		private long counter;
		private boolean hasInterceptItem;
		private int parallelism;
		private int vectorTrainIdx;
		private int labelIdx;
		private int[] featureIdx;

		public ParseSample(boolean hasInterceptItem,
						   int vectorTrainIdx, int[] featureIdx, int labelIdx) {
			this.hasInterceptItem = hasInterceptItem;
			this.vectorTrainIdx = vectorTrainIdx;
			this.labelIdx = labelIdx;
			this.featureIdx = featureIdx;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
			counter = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public Tuple3 <Long, Vector, Object> map(Row row) throws Exception {
			long sampleId = counter;
			counter += parallelism;
			Vector vec;
			if (vectorTrainIdx == -1) {
				vec = new DenseVector(featureIdx.length);
				for (int i = 0; i < featureIdx.length; ++i) {
					vec.set(i, Double.valueOf(row.getField(featureIdx[i]).toString()));
				}
			} else {
				vec = VectorUtil.getVector(row.getField(vectorTrainIdx));
			}

			if (hasInterceptItem) {
				vec = vec.prefix(1.0);
			}
			Thread.sleep(1);
			return Tuple3.of(sampleId, vec, row.getField(labelIdx));
		}
	}

	public static class AppendWx extends RichCoFlatMapFunction <Tuple3 <Long, Vector, Object>,
		Tuple2 <Long, Object>,
		Tuple4 <Long, Vector, Object, double[]>> {
		private static final long serialVersionUID = 7338858137860097282L;
		Map <Long, Tuple3 <Vector, Object, Long>> sampleBuffer = new HashMap <>();
		private long cnt = 0L;

		@Override
		public void flatMap1(Tuple3 <Long, Vector, Object> value,
							 Collector <Tuple4 <Long, Vector, Object, double[]>> out)
			throws Exception {
			if (cnt > 0 && (value.f0 / getRuntimeContext().getNumberOfParallelSubtasks()) % cnt != 0) {
				return;
			}
			sampleBuffer.put(value.f0, Tuple3.of(value.f1, value.f2, System.currentTimeMillis()));
			out.collect(Tuple4.of(value.f0, value.f1, value.f2, new double[] {0.0}));
		}

		@Override
		public void flatMap2(Tuple2 <Long, Object> value, Collector <Tuple4 <Long, Vector, Object, double[]>> out)
			throws Exception {
			Tuple3 <Vector, Object, Long> sample = sampleBuffer.get(value.f0);
			long timeInterval = System.currentTimeMillis() - sample.f2;
			if (timeInterval > 500L) {
				if (cnt != 2L) {
					cnt = 2L;
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("50% samples are abandoned. current sampleId " + value.f0);
					}
				}
			} else if (timeInterval > 1000L) {
				if (cnt != 4L) {
					cnt = 4L;
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("75% samples are abandoned. current sampleId " + value.f0);
					}
				}
			} else if (timeInterval > 10000L) {
				if (cnt != 8L) {
					cnt = 8L;
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("87.5% samples are abandoned. current sampleId " + value.f0);
					}
				}
			} else {
				if (cnt != 0L) {
					cnt = 0L;
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("no sample is abandoned. current sampleId " + value.f0);
					}
				}
			}
			out.collect(Tuple4.of(-(value.f0 + 1), sample.f0, sample.f1, (double[]) value.f1));
		}
	}

	public static class SplitVector
		extends RichFlatMapFunction <Tuple4 <Long, Vector, Object, double[]>,
		Tuple6 <Long, Integer, Integer, Vector, Object, double[]>> {
		private static final long serialVersionUID = -8716205207637225677L;
		private int coefSize;
		private boolean hasInterceptItem;
		private int vectorSize;
		private int parallelism;
		private int[] splitInfo;
		private int[] nnz;

		public SplitVector(int[] splitInfo, boolean hasInterceptItem, int vectorSize) {
			this.hasInterceptItem = hasInterceptItem;
			this.vectorSize = vectorSize;

			this.splitInfo = splitInfo;
			this.nnz = new int[splitInfo.length - 1];
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
			coefSize = (hasInterceptItem) ? vectorSize + 1 : vectorSize;
		}

		@Override
		public void flatMap(Tuple4 <Long, Vector, Object, double[]> t4,
							Collector <Tuple6 <Long, Integer, Integer, Vector, Object, double[]>> collector)
			throws Exception {
			Vector vec = t4.f1;

			if (vec instanceof SparseVector) {
				int[] indices = ((SparseVector) vec).getIndices();
				double[] values = ((SparseVector) vec).getValues();
				int pos = 1;
				int subNum = 0;
				boolean hasElement = false;

				for (int i = 0; i < indices.length; ++i) {
					if (indices[i] < splitInfo[pos]) {
						nnz[pos - 1]++;
						hasElement = true;
					} else {
						pos++;
						i--;
						if (hasElement) {
							subNum++;
							hasElement = false;
						}
					}
				}
				if (nnz[pos - 1] != 0) {
					subNum++;
				}
				pos = 0;
				for (int i = 0; i < nnz.length; ++i) {
					if (nnz[i] != 0) {
						int[] tmpIndices = new int[nnz[i]];
						double[] tmpValues = new double[nnz[i]];
						System.arraycopy(indices, pos, tmpIndices, 0, nnz[i]);
						System.arraycopy(values, pos, tmpValues, 0, nnz[i]);
						Vector tmpVec = new SparseVector(coefSize, tmpIndices, tmpValues);
						collector.collect(Tuple6.of(t4.f0, i, subNum, tmpVec, t4.f2, t4.f3));
						pos += nnz[i];
						nnz[i] = 0;
					}
				}
			} else {
				double[] data = ((DenseVector) vec).getData();
				for (int i = 0; i < splitInfo.length - 1; ++i) {
					DenseVector dvec = new DenseVector(splitInfo[i + 1] - splitInfo[i]);
					for (int j = splitInfo[i]; j < splitInfo[i + 1]; ++j) {
						dvec.set(j - splitInfo[i], data[j]);
					}
					collector.collect(Tuple6.of(t4.f0, i, parallelism, dvec, t4.f2, t4.f3));
				}
			}
		}
	}

	public static class CalcTask extends RichFlatMapFunction <Tuple6 <Long, Integer, Integer, Vector, Object,
		double[]>,
		Tuple2 <Long, Object>> implements CheckpointedFunction {
		private static final long serialVersionUID = 1613267176484234752L;
		private DataBridge dataBridge;
		transient private double[] coef;
		transient private Map <Long, Tuple3 <Vector, Object, Long>> subVectors;

		private int startIdx;
		private int endIdx;
		private int[] poses;
		long startTime = System.currentTimeMillis();
		int modelSaveTimeInterval;
		private Object[] labelValues;
		private long modelId = 0;
		private OnlineLearningKernel kernel = null;
		private Params params;
		private int vectorSize;
		private boolean hasIntercept;
		private transient ListState <Tuple2 <double[], Object[]>> modelState;

		public CalcTask(DataBridge dataBridge, int[] poses, Params params, OnlineLearningKernel kernel, int vectorSize) {
			this.dataBridge = dataBridge;
			this.poses = poses;
			this.modelSaveTimeInterval = params.get(OnlineTrainParams.TIME_INTERVAL) * 1000;
			this.labelValues = null;
			this.kernel = kernel;
			this.params = params;
			this.vectorSize = vectorSize;
			this.hasIntercept = params.get(OnlineTrainParams.WITH_INTERCEPT);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			subVectors = new HashMap <>();
			startTime = System.currentTimeMillis();
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			Tuple2 <double[], Object[]> state = Tuple2.of(coef, labelValues);
			modelState.clear();
			modelState.add(state);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {

			ListStateDescriptor <Tuple2 <double[], Object[]>> descriptor =
				new ListStateDescriptor("StreamingOnlineFtrlModelState",
					TypeInformation.of(new TypeHint <Tuple2 <double[], Object[]>>() {
					}));

			modelState = context.getOperatorStateStore().getListState(descriptor);

			int numWorkers = getRuntimeContext().getNumberOfParallelSubtasks();
			int workerId = getRuntimeContext().getIndexOfThisSubtask();
			startIdx = poses[workerId];
			endIdx = poses[workerId + 1];
			if (context.isRestored()) {
				for (Tuple2 <double[], Object[]> state : modelState.get()) {
					this.coef = state.f0;
					this.labelValues = state.f1;
					int localSize = coef.length;
					kernel.setModelParams(params, localSize, labelValues);
				}
			} else {
				// read init model
				List <Row> modelRows = DirectReader.directRead(dataBridge);
				LinearModelData model = new LinearModelDataConverter().load(modelRows);
				labelValues = model.labelValues;
				int weightSize = vectorSize + (hasIntercept ? 1 : 0);

				int localSize = weightSize / numWorkers;
				localSize += (workerId < weightSize % numWorkers) ? 1 : 0;
				kernel.setModelParams(params, localSize, labelValues);

				coef = new double[localSize];
				endIdx = Math.min(endIdx, model.coefVector.size());
				for (int i = startIdx; i < endIdx; ++i) {
					coef[i - startIdx] = model.coefVector.get(i);
				}
			}
		}

		@Override
		public void flatMap(
			Tuple6 <Long, Integer, Integer, Vector, Object, double[]> value,
			Collector <Tuple2 <Long, Object>> out) throws Exception {

			Long timeStamps = System.currentTimeMillis();
			if (value.f0 >= 0) {
				Vector vec = value.f3;
				double[] rval = kernel.calcLocalWx(coef, vec, startIdx);
				subVectors.put(value.f0, Tuple3.of(vec, value.f4, timeStamps));
				out.collect(Tuple2.of(value.f0, Tuple2.of(value.f2, rval)));
			} else {
				Tuple3 <Vector, Object, Long> t3 = subVectors.get(-(value.f0 + 1));
				long timeInterval = timeStamps - t3.f2;
				Vector vec = t3.f0;
				double[] r = value.f5;
				kernel.updateModel(coef, vec, r, timeInterval, startIdx, value.f4);
				subVectors.remove(-(value.f0 + 1));
				if (System.currentTimeMillis() - startTime > modelSaveTimeInterval) {
					startTime = System.currentTimeMillis();
					modelId++;
					out.collect(Tuple2.of(-modelId,
						Tuple3.of(getRuntimeContext().getIndexOfThisSubtask(), labelValues, coef)));
				}
			}
		}
	}

	public static class ReduceTask extends
		RichFlatMapFunction <Tuple2 <Long, Object>, Tuple2 <Long, Object>> {
		private static final long serialVersionUID = 1072071076831105639L;
		private int parallelism;
		private int[] poses;
		transient private Map <Long, Tuple2 <Integer, double[]>> buffer;
		OnlineLearningKernel kernel;
		private Map <Long, List <Tuple2 <Integer, double[]>>> models;

		public ReduceTask(int parallelism, int[] poses, OnlineLearningKernel kernel) {
			this.parallelism = parallelism;
			this.poses = poses;
			this.kernel = kernel;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			buffer = new HashMap <>(0);
			models = new HashMap <>(0);
		}

		@Override
		public void flatMap(Tuple2 <Long, Object> value,
							Collector <Tuple2 <Long, Object>> out)
			throws Exception {
			if (value.f0 < 0) {
				long modelId = value.f0;
				Tuple3 <Integer, Object, double[]> t3 = (Tuple3 <Integer, Object, double[]>) value.f1;
				List <Tuple2 <Integer, double[]>> model = models.get(modelId);
				if (model == null) {
					model = new ArrayList <>();
					model.add(Tuple2.of(t3.f0, t3.f2));
					models.put(modelId, model);
				} else {
					model.add(Tuple2.of(t3.f0, t3.f2));
				}
				if (model.size() == parallelism) {
					double[] coef = new double[poses[parallelism]];
					for (Tuple2 <Integer, double[]> subModel : model) {
						int pos = poses[subModel.f0];
						for (int i = 0; i < subModel.f1.length; ++i) {
							coef[pos + i] = subModel.f1[i];
						}
					}
					out.collect(Tuple2.of(value.f0, Tuple2.of(t3.f1, coef)));
					models.remove(modelId);
				}
			} else {
				Tuple2 <Integer, double[]> val = buffer.get(value.f0);
				Tuple2 <Integer, double[]> t2 = (Tuple2 <Integer, double[]>) value.f1;
				if (val == null) {
					val = Tuple2.of(1, t2.f1);
					buffer.put(value.f0, val);
				} else {
					val.f0++;
					for (int i = 0; i < val.f1.length; ++i) {
						val.f1[i] += t2.f1[i];
					}
				}

				if (val.f0 == t2.f0) {
					out.collect(Tuple2.of(value.f0, kernel.getFeedbackVar(val.f1)));
					buffer.remove(value.f0);
				}
			}
		}
	}

	private static class SubVectorPartitioner implements Partitioner <Integer> {

		private static final long serialVersionUID = 3154122892861557361L;

		@Override
		public int partition(Integer key, int numPartitions) {
			return key % numPartitions;
		}
	}

	private static class WxPartitioner implements Partitioner <Long> {

		private static final long serialVersionUID = -6637943571982178520L;

		@Override
		public int partition(Long sampleId, int numPartition) {
			if (sampleId < 0L) {
				return 0;
			} else {
				return (int) (sampleId % numPartition);
			}
		}
	}

	/**
	 * this class define the kernel learning function api of online learning method.
	 */
	public abstract static class OnlineLearningKernel implements Serializable {
		private static final long serialVersionUID = 8668156188944429667L;

		/**
		 * calc wx with sub vector.
		 *
		 * @param coef
		 * @param vec
		 * @param startIdx
		 * @return
		 */
		public abstract double[] calcLocalWx(double[] coef, Vector vec, int startIdx);

		/**
		 * get feedback variable.
		 *
		 * @param wx
		 * @return
		 */
		public abstract double[] getFeedbackVar(double[] wx);

		/**
		 * set model variables.
		 *
		 * @param params
		 * @param localModelSize
		 * @param labelValues
		 */
		public abstract void setModelParams(Params params, int localModelSize, Object[] labelValues);

		/**
		 * update model with feedback variables.
		 *
		 * @param coef
		 * @param vec
		 * @param wx
		 * @param timeInterval
		 * @param startIdx
		 * @param labelValue
		 */
		public abstract void updateModel(double[] coef, Vector vec, double[] wx,
										 long timeInterval, int startIdx, Object labelValue);
	}
}
