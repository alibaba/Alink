package com.alibaba.alink.operator.common.tree;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExecutorUtils;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.tree.paralleltree.TreeInitObj;
import com.alibaba.alink.operator.common.tree.paralleltree.TreeObj;
import com.alibaba.alink.operator.common.tree.paralleltree.TreeSplit;
import com.alibaba.alink.operator.common.tree.paralleltree.TreeStat;
import com.alibaba.alink.operator.common.tree.seriestree.DecisionTree;
import com.alibaba.alink.operator.common.tree.seriestree.DenseData;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
import com.alibaba.alink.params.shared.tree.HasSeed;
import com.alibaba.alink.params.shared.tree.HasTreePartition;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.alibaba.alink.operator.common.tree.TreeModelDataConverter.IMPORTANCE_FIRST_COL;
import static com.alibaba.alink.operator.common.tree.TreeModelDataConverter.IMPORTANCE_SECOND_COL;

/**
 * Base class for fitting random forest and decision tree model. The random forest use the bagging to prevent the
 * overfitting.
 *
 * <p>In the operator, we implement three type of decision tree to
 * increase diversity of the forest.
 * <ul>
 * <tr>id3</tr>
 * <tr>cart</tr>
 * <tr>c4.5</tr>
 * </ul>
 * and the criteria is
 * <ul>
 * <tr>information</tr>
 * <tr>gini</tr>
 * <tr>information ratio</tr>
 * <tr>mse</tr>
 * </ul>
 *
 * @param <T>
 * @see <a href="https://en.wikipedia.org/wiki/Random_forest">Random_forest</a>
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {
	@PortSpec(PortType.MODEL),
	@PortSpec(value = PortType.DATA, desc = PortDesc.FEATURE_IMPORTANCE)
})
@ParamSelectColumnSpec(
	name = "featureCols",
	allowedTypeCollections = TypeCollections.TREE_FEATURE_TYPES
)
@ParamSelectColumnSpec(
	name = "categoricalCols",
	allowedTypeCollections = TypeCollections.TREE_FEATURE_TYPES
)
@ParamSelectColumnSpec(
	name = "weightCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES
)
@ParamSelectColumnSpec(name = "labelCol")
public abstract class BaseRandomForestTrainBatchOp<T extends BaseRandomForestTrainBatchOp <T>>
	extends BatchOperator <T> {

	private static final long serialVersionUID = 5757403088524138175L;
	protected DataSet <Object[]> labels;
	protected BatchOperator <?> stringIndexerModel;

	protected BaseRandomForestTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		rewriteTreeType(getParams());

		rewriteLabelType(in.getSchema(), getParams());

		getParams().set(ModelParamName.FEATURE_TYPES,
			FlinkTypeConverter.getTypeString(
				TableUtil.findColTypesWithAssertAndHint(in.getSchema(),
					getParams().get(RandomForestTrainParams.FEATURE_COLS))
			)
		);

		in = Preprocessing.select(in, TreeUtil.trainColNames(getParams()));

		set(
			RandomForestTrainParams.CATEGORICAL_COLS,
			TableUtil.getCategoricalCols(
				in.getSchema(),
				getParams().get(RandomForestTrainParams.FEATURE_COLS),
				getParams().contains(RandomForestTrainParams.CATEGORICAL_COLS) ?
					getParams().get(RandomForestTrainParams.CATEGORICAL_COLS) : null
			)
		);

		labels = Preprocessing.generateLabels(
			in, getParams(), Criteria.isRegression(getParams().get(TreeUtil.TREE_TYPE))
		);

		in = Preprocessing.castLabel(
			in, getParams(), labels, Criteria.isRegression(getParams().get(TreeUtil.TREE_TYPE))
		);

		stringIndexerModel = Preprocessing.generateStringIndexerModel(in, getParams());

		in = Preprocessing.castWeightCol(
			Preprocessing.castContinuousCols(
				Preprocessing.castCategoricalCols(
					in, stringIndexerModel, getParams()
				),
				getParams()
			),
			getParams()
		);

		DataSet <Row> model;

		if (getParams().get(RandomForestTrainParams.CREATE_TREE_MODE).toUpperCase().equals("PARALLEL")) {
			model = parallelTrain(in);
		} else {
			model = seriesTrain(in);
		}

		Table importanceTable = DataSetConversionUtil.toTable(
			getMLEnvironmentId(),
			model.reduceGroup(
				new TreeModelDataConverter.FeatureImportanceReducer()
			),
			new String[] {getParams().get(IMPORTANCE_FIRST_COL), getParams().get(IMPORTANCE_SECOND_COL)},
			new TypeInformation[] {Types.STRING, Types.DOUBLE}
		);

		this.setSideOutputTables(new Table[] {importanceTable});

		setOutput(model, new TreeModelDataConverter(
				FlinkTypeConverter.getFlinkType(getParams().get(ModelParamName.LABEL_TYPE_NAME))
			).getModelSchema()
		);

		return (T) this;
	}

	public static void rewriteLabelType(TableSchema inputSchema, Params params) {
		if (Criteria.isRegression(params.get(TreeUtil.TREE_TYPE))) {
			params.set(ModelParamName.LABEL_TYPE, FlinkTypeConverter.getTypeString(Types.DOUBLE));
		} else {
			params.set(
				ModelParamName.LABEL_TYPE,
				FlinkTypeConverter.getTypeString(
					TableUtil.findColTypeWithAssertAndHint(
						inputSchema,
						params.get(RandomForestTrainParams.LABEL_COL))
				)
			);
		}
	}

	private DataSet <Row> parallelTrain(BatchOperator <?> in) {
		BatchOperator <?> quantileModel = Preprocessing.generateQuantileDiscretizerModel(in, getParams());

		DataSet <Row> trainingDataSet = Preprocessing
			.castToQuantile(in, quantileModel, getParams())
			.getDataSet()
			// check null value in training dataset and throw exception when there are null values.
			.map(new CheckNullValue(in.getColNames()));

		final Params meta = getParams().clone();

		return new IterativeComQueue()
			.setMaxIter(Integer.MAX_VALUE)
			.initWithPartitionedData("treeInput", trainingDataSet)
			.initWithBroadcastData("quantileModel", quantileModel.getDataSet())
			.initWithBroadcastData("stringIndexerModel", stringIndexerModel.getDataSet())
			.initWithBroadcastData("labels", labels)
			.add(new TreeInitObj(meta))
			.add(new TreeStat())
			.add(new AllReduce("allReduce", "allReduceCnt"))
			.add(new TreeSplit())
			.setCompareCriterionOfNode0(new Criterion())
			.closeWith(new SerializeModelCompleteResultFunction(meta))
			.exec();
	}

	private static class SerializeModelCompleteResultFunction extends CompleteResultFunction {
		private static final long serialVersionUID = -774526299754876291L;
		private final Params meta;

		SerializeModelCompleteResultFunction(Params meta) {
			this.meta = meta;
		}

		@Override
		public List <Row> calc(ComContext context) {
			if (context.getTaskId() != 0) {
				return null;
			}

			TreeObj treeObj = context.getObj("treeObj");
			List <Row> stringIndexerModel = context.getObj("stringIndexerModel");
			List <Object[]> labelsList = context.getObj("labels");
			List <Row> model = TreeModelDataConverter.saveModelWithData(
				treeObj.getRoots(), meta, stringIndexerModel,
				labelsList == null || labelsList.isEmpty() ? null : labelsList.get(0)
			);
			return model;
		}
	}

	private static class Criterion extends CompareCriterionFunction {
		private static final long serialVersionUID = -8249556754233088562L;

		@Override
		public boolean calc(ComContext context) {
			TreeObj treeObj = context.getObj("treeObj");

			return treeObj.terminationCriterion();
		}
	}

	private static class CheckNullValue implements MapFunction <Row, Row> {
		private static final long serialVersionUID = -2809221584231401798L;
		private String[] cols;

		public CheckNullValue(String[] cols) {
			this.cols = cols;
		}

		@Override
		public Row map(Row value) throws Exception {
			for (int i = 0; i < value.getArity(); ++i) {
				if (value.getField(i) == null) {
					throw new AkIllegalOperatorParameterException(
						"There should not be null value in training dataset. col: "
							+ cols[i] + ", "
							+ "Maybe you can use {@code Imputer} to fill the missing values");
				}
			}
			return value;
		}
	}

	private DataSet <Row> seriesTrain(BatchOperator <?> in) {
		DataSet <Row> trainDataSet = in.getDataSet();

		DataSet <Long> cnt = DataSetUtils
			.countElementsPerPartition(trainDataSet)
			.sum(1)
			.map(new MapFunction <Tuple2 <Integer, Long>, Long>() {
				private static final long serialVersionUID = 2167540828697787410L;

				@Override
				public Long map(Tuple2 <Integer, Long> value) throws Exception {
					return value.f1;
				}
			});

		DataSet <Integer> labelSize = labels.map(new MapFunction <Object[], Integer>() {
			private static final long serialVersionUID = 993622654277953634L;

			@Override
			public Integer map(Object[] objects) throws Exception {
				return objects.length;
			}
		});

		// Flink pass a copy of the no iterative part into the iteration translation,
		// and then, recursively build the data flow for the step function.
		// We must create the relationship between the input and the loop first.

		//IterativeDataSet <Tuple2 <Integer, String>> loop =
		//	MLEnvironmentFactory
		//		.get(getMLEnvironmentId())
		//		.getExecutionEnvironment()
		//		.fromElements(Tuple2.of(-1, ""))
		//		.iterate(getParams().get(RandomForestTrainParams.NUM_TREES));

		IterativeDataSet <Tuple2 <Integer, String>> loop =
			trainDataSet
				.mapPartition(new MapPartitionFunction <Row, Tuple2 <Integer, String>>() {
					private static final long serialVersionUID = -1747675717825084026L;

					@Override
					public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, String>> out) {
						//pass
					}
				})
				.iterate(getParams().get(RandomForestTrainParams.NUM_TREES));

		DataSet <Tuple2 <Integer, Row>> sampled = trainDataSet
			.mapPartition(new SampleDataLimit(
					get(HasSeed.SEED),
					get(RandomForestTrainParams.SUBSAMPLING_RATIO),
					get(RandomForestTrainParams.NUM_TREES)
				)
			)
			.withBroadcastSet(loop, "loop")
			.withBroadcastSet(cnt, "totalCnt");

		DataSet <Tuple2 <Integer, String>> pModel = sampled
			.partitionCustom(new AvgPartition(), 0)
			.mapPartition(new SeriesTrainFunction(getParams()))
			.withBroadcastSet(stringIndexerModel.getDataSet(), "stringIndexerModel")
			.withBroadcastSet(labelSize, "labelSize")
			.withBroadcastSet(cnt, "totalCnt");

		pModel = loop.closeWith(
			pModel,
			pModel.filter(new FilterFunction <Tuple2 <Integer, String>>() {
				private static final long serialVersionUID = 7877735883319723407L;

				@Override
				public boolean filter(Tuple2 <Integer, String> value) throws Exception {
					return value.f0 < 0;
				}
			}));

		return pModel
			.reduceGroup(new SerializeModel(getParams()))
			.withBroadcastSet(stringIndexerModel.getDataSet(), "stringIndexerModel")
			.withBroadcastSet(labels, "labels");
	}

	public static void rewriteTreeType(Params params) {
		int numTrees = 0;
		StringBuilder stringBuilder = new StringBuilder();

		if (params.contains(RandomForestTrainParams.NUM_TREES_OF_INFO_GAIN)) {
			int numTreeOfInfoGain = params.get(RandomForestTrainParams.NUM_TREES_OF_INFO_GAIN);
			numTrees += numTreeOfInfoGain;
		}

		stringBuilder.append(numTrees);

		if (params.contains(RandomForestTrainParams.NUM_TREES_OF_GINI)) {
			int numTreeOfGini = params.get(RandomForestTrainParams.NUM_TREES_OF_GINI);
			numTrees += numTreeOfGini;
		}

		stringBuilder.append(",").append(numTrees);

		if (params.contains(RandomForestTrainParams.NUM_TREES_OF_INFO_GAIN_RATIO)) {
			int numTreeOfInfoGain = params.get(RandomForestTrainParams.NUM_TREES_OF_INFO_GAIN_RATIO);
			numTrees += numTreeOfInfoGain;
		}

		if (numTrees > 0) {
			params.set(RandomForestTrainParams.NUM_TREES, numTrees);
			params.set(TreeUtil.TREE_TYPE, TreeUtil.TreeType.PARTITION);
			params.set(HasTreePartition.TREE_PARTITION, stringBuilder.toString());
		}
	}

	private static class SeriesTrainFunction
		extends RichMapPartitionFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, String>> {

		private static final Logger LOG = LoggerFactory.getLogger(SeriesTrainFunction.class);
		private static final long serialVersionUID = 8664682425332787016L;
		private static final int NUM_THREADS_POOL = 4;
		private Map <String, Integer> categoricalColsSize;
		private final Params params;

		private transient int cnt;
		private transient List <Tuple2 <Integer, Node>> trees;
		private transient DenseData data;
		private transient ExecutorService executorService;

		public SeriesTrainFunction(Params params) {
			this.params = params;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			categoricalColsSize = getRuntimeContext()
				.getBroadcastVariableWithInitializer(
					"stringIndexerModel",
					new BroadcastVariableInitializer <Row, Map <String, Integer>>() {
						@Override
						public Map <String, Integer> initializeBroadcastVariable(Iterable <Row> iterable) {
							List <Row> stringIndexerSerialized = new ArrayList <>();
							for (Row row : iterable) {
								stringIndexerSerialized.add(row);
							}
							return TreeUtil.extractCategoricalColsSize(
								stringIndexerSerialized, params.get(RandomForestTrainParams.CATEGORICAL_COLS));
						}
					});

			if (!Criteria.isRegression(params.get(TreeUtil.TREE_TYPE))) {
				categoricalColsSize.put(
					params.get(RandomForestTrainParams.LABEL_COL),
					getRuntimeContext()
						.getBroadcastVariableWithInitializer(
							"labelSize",
							new BroadcastVariableInitializer <Integer, Integer>() {
								@Override
								public Integer initializeBroadcastVariable(Iterable <Integer> iterable) {
									return iterable.iterator().next();
								}
							}
						)
				);
			}

			long cnt = getRuntimeContext()
				.getBroadcastVariableWithInitializer("totalCnt", new BroadcastVariableInitializer <Long, Long>() {
						@Override
						public Long initializeBroadcastVariable(Iterable <Long> data) {
							for (Long cnt : data) {
								return cnt;
							}

							throw new AkIllegalOperatorParameterException(
								"Can not find total sample count of sample in training dataset if factor > 1.0"
							);
						}
					}
				);

			this.cnt = params.get(RandomForestTrainParams.SUBSAMPLING_RATIO) > 1.0 ?
				Double.valueOf(Math.min(cnt, params.get(RandomForestTrainParams.SUBSAMPLING_RATIO))).intValue() :
				Double.valueOf(params.get(RandomForestTrainParams.SUBSAMPLING_RATIO) * cnt).intValue();

			executorService = new ThreadPoolExecutor(NUM_THREADS_POOL, NUM_THREADS_POOL,
				0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue <>(NUM_THREADS_POOL),
				new BasicThreadFactory
					.Builder()
					.namingPattern("random-forest-%d")
					.daemon(true)
					.build(),
				new ThreadPoolExecutor.AbortPolicy()
			);
		}

		@Override
		public void close() throws Exception {
			if (executorService != null) {
				ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executorService);
			}
		}

		private static class IterableWrapper implements Iterable <Row> {
			private final Iterable <Tuple2 <Integer, Row>> iterable;
			private transient IteratorWrapper iterator;

			public IterableWrapper(Iterable <Tuple2 <Integer, Row>> iterable) {
				this.iterable = iterable;
			}

			public int getTreeId() {
				return iterator.getTreeId();
			}

			@Override
			public Iterator <Row> iterator() {
				if (iterator == null) {
					iterator = new IteratorWrapper(iterable.iterator());
				}
				return iterator;
			}
		}

		private static class IteratorWrapper implements Iterator <Row> {
			private final Iterator <Tuple2 <Integer, Row>> iterator;
			private int treeId = -1;

			public IteratorWrapper(Iterator <Tuple2 <Integer, Row>> iterator) {
				this.iterator = iterator;
			}

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			public int getTreeId() {
				return treeId;
			}

			@Override
			public Row next() {
				Tuple2 <Integer, Row> n = iterator.next();
				treeId = n.f0;
				return n.f1;
			}
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, Row>> values, Collector <Tuple2 <Integer, String>> out) {
			LOG.info("start the random forests training");
			int parallel = getRuntimeContext().getNumberOfParallelSubtasks();
			int superStep = getIterationRuntimeContext().getSuperstepNumber();
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			final Params localParams = params.clone();

			if (trees == null) {
				trees = new ArrayList <>();
			}

			// create dense data.
			if (this.data == null) {
				data = new DenseData(
					cnt,
					TreeUtil.getFeatureMeta(localParams.get(RandomForestTrainParams.FEATURE_COLS),
						categoricalColsSize),
					TreeUtil.getLabelMeta(
						localParams.get(RandomForestTrainParams.LABEL_COL),
						localParams.get(RandomForestTrainParams.FEATURE_COLS).length,
						categoricalColsSize
					)
				);
			}

			IterableWrapper wrapper = new IterableWrapper(values);

			// read instance to data.
			data.readFromInstances(wrapper);

			//Non-empty partition.
			if (wrapper.getTreeId() >= 0) {
				LOG.info("start the random forests training {}", wrapper.getTreeId());

				// rewrite gain for this tree.
				localParams.set(Criteria.Gain.GAIN, getGainFromParams(localParams, wrapper.getTreeId()));

				// rewrite seed.
				localParams.set(HasSeed.SEED, localParams.get(HasSeed.SEED) + superStep * (taskId + 1));

				// fit the decision tree.
				Node root = new DecisionTree(data, localParams, executorService).fit();

				trees.add(Tuple2.of(wrapper.getTreeId(), root));

				LOG.info("end the random forests training {}", wrapper.getTreeId());
			}

			if (superStep * parallel >= localParams.get(RandomForestTrainParams.NUM_TREES)) {
				for (Tuple2 <Integer, Node> tree : trees) {
					// serialize the tree.
					for (String serialized : TreeModelDataConverter.serializeTree(tree.f1)) {
						out.collect(Tuple2.of(tree.f0, serialized));
					}
				}
			} else {
				out.collect(Tuple2.of(-1, ""));
			}

			LOG.info("end the random forests training");
		}
	}

	public static class AvgPartition implements Partitioner <Integer> {

		private static final long serialVersionUID = -8338959787279940010L;

		@Override
		public int partition(Integer key, int numPartitions) {
			return key % numPartitions;
		}
	}

	private static class SerializeModel extends RichGroupReduceFunction <Tuple2 <Integer, String>, Row> {
		private static final long serialVersionUID = -314826879276130037L;
		private Params params;
		private transient List <Row> stringIndexerModelSerialized;
		private transient Object[] labels;

		public SerializeModel(Params params) {
			this.params = params;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			stringIndexerModelSerialized = getRuntimeContext().getBroadcastVariable("stringIndexerModel");
			labels = getRuntimeContext().getBroadcastVariableWithInitializer("labels",
				new BroadcastVariableInitializer <Object[], Object[]>() {
					@Override
					public Object[] initializeBroadcastVariable(Iterable <Object[]> data) {
						Iterator <Object[]> iter = data.iterator();

						if (iter.hasNext()) {
							return iter.next();
						} else {
							return null;
						}
					}
				});
		}

		@Override
		public void reduce(Iterable <Tuple2 <Integer, String>> values, Collector <Row> out) throws Exception {
			TreeModelDataConverter.saveModelWithData(
				StreamSupport.stream(values.spliterator(), false)
					.collect(Collectors.groupingBy(x -> x.f0, Collectors.mapping(x -> x.f1, Collectors.toList())))
					.entrySet()
					.stream()
					.sorted((x, y) -> x.getKey().compareTo(y.getKey()))
					.map(x -> TreeModelDataConverter.deserializeTree(x.getValue()))
					.collect(Collectors.toList()),
				params,
				stringIndexerModelSerialized,
				labels
			).forEach(out::collect);
		}
	}

	private static Criteria.Gain getGainFromParams(Params params, int treeId) {
		TreeUtil.TreeType treeType = params.get(TreeUtil.TREE_TYPE);

		switch (treeType) {
			case AVG:
				return getAvgGain(params.get(RandomForestTrainParams.NUM_TREES), treeId);
			case PARTITION:
				return getIntervalGain(params.get(HasTreePartition.TREE_PARTITION), treeId);
			case MSE:
				return Criteria.Gain.MSE;
			case GINI:
				return Criteria.Gain.GINI;
			case INFOGAIN:
				return Criteria.Gain.INFOGAIN;
			case INFOGAINRATIO:
				return Criteria.Gain.INFOGAINRATIO;
			default:
				throw new AkIllegalOperatorParameterException(
					"Could not parse the gain type from params. type: " + treeType);
		}
	}

	private static Criteria.Gain getIntervalGain(String treeType, int id) {
		String[] intervalStrs = treeType.split(",");

		AkPreconditions.checkState(intervalStrs.length == 2, "Error format of treeType: " + treeType);

		return getIntervalGain(
			Integer.parseInt(intervalStrs[0]),
			Integer.parseInt(intervalStrs[1]),
			id);
	}

	private static Criteria.Gain getIntervalGain(int startGini, int startInfoGainRatio, int id) {
		if (id < startGini) {
			return Criteria.Gain.INFOGAIN;
		} else if (id < startInfoGainRatio) {
			return Criteria.Gain.GINI;
		} else {
			return Criteria.Gain.INFOGAINRATIO;
		}
	}

	private static Criteria.Gain getAvgGain(int treeNum, int id) {
		int div = treeNum / 3;
		int mod = treeNum % 3;

		int startGini = mod < 1 ? div : div + 1;
		int startInfoGainRatio = mod < 2 ? startGini + div : startGini + div + 1;
		return getIntervalGain(startGini, startInfoGainRatio, id);
	}

	public static class SampleDataLimit extends RichMapPartitionFunction <Row, Tuple2 <Integer, Row>> {
		private static final long serialVersionUID = -8114271430777216933L;
		private final long seed;
		private double factor;
		private final int treeNum;

		public SampleDataLimit(long seed, double factor, int treeNum) {
			this.seed = seed;
			this.factor = factor;
			this.treeNum = treeNum;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			if (factor > 1.0) {
				factor = Math.min(factor / getRuntimeContext()
					.getBroadcastVariableWithInitializer("totalCnt", new BroadcastVariableInitializer <Long, Double>
						() {
						@Override
						public Double initializeBroadcastVariable(Iterable <Long> data) {
							for (Long cnt : data) {
								return cnt.doubleValue();
							}

							throw new AkIllegalOperatorParameterException(
								"Can not find total sample count of sample in training dataset if factor > 1.0"
							);
						}
					}), 1.0);
			}
		}

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, Row>> out) {

			int parallel = getRuntimeContext().getNumberOfParallelSubtasks();
			int superStep = getIterationRuntimeContext().getSuperstepNumber();
			int taskId = getRuntimeContext().getIndexOfThisSubtask();

			Random rand = new Random(this.seed + superStep * (taskId + 1));

			for (Row row : values) {
				for (int i = (superStep - 1) * parallel; i < treeNum && i < superStep * parallel; ++i) {
					if (rand.nextDouble() < factor) {
						out.collect(new Tuple2 <>(i, row));
					}
				}
			}
		}
	}
}
