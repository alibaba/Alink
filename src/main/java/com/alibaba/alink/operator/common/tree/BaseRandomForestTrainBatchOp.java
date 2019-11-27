package com.alibaba.alink.operator.common.tree;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.tree.paralleltree.TreeInitObj;
import com.alibaba.alink.operator.common.tree.paralleltree.TreeObj;
import com.alibaba.alink.operator.common.tree.paralleltree.TreeSplit;
import com.alibaba.alink.operator.common.tree.paralleltree.TreeStat;
import com.alibaba.alink.operator.common.tree.seriestree.DecisionTree;
import com.alibaba.alink.operator.common.tree.seriestree.DenseData;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.tree.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Base class for fitting random forest and decision tree model.
 * The random forest use the bagging to prevent the overfitting.
 *
 * <p>In the operator, we implement three type of decision tree to
 * increase diversity of the forest.
 * <ul>
 *     <tr>id3</tr>
 *     <tr>cart</tr>
 *     <tr>c4.5</tr>
 * </ul>
 * and the criteria is
 * <ul>
 *     <tr>information</tr>
 *     <tr>gini</tr>
 *     <tr>information ratio</tr>
 *     <tr>mse</tr>
 * </ul>
 *
 * @see <a href="https://en.wikipedia.org/wiki/Random_forest">Random_forest</a>
 *
 * @param <T>
 */
public abstract class BaseRandomForestTrainBatchOp<T extends BaseRandomForestTrainBatchOp<T>>
	extends BatchOperator<T> {
	protected DataSet<Object[]> labels;
	protected BatchOperator<?> stringIndexerModel;

	protected BaseRandomForestTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public T linkFrom(BatchOperator<?>... inputs) {
		BatchOperator<?> in = checkAndGetFirst(inputs);

		if (Criteria.isRegression(getParams().get(HasTreeType.TREE_TYPE))) {
			getParams().set(ModelParamName.LABEL_TYPE, FlinkTypeConverter.getTypeString(Types.DOUBLE));
		} else {
			getParams().set(
				ModelParamName.LABEL_TYPE,
				FlinkTypeConverter.getTypeString(
					TableUtil.findColType(in.getSchema(), getParams().get(HasLabelCol.LABEL_COL))
				)
			);
		}

		getParams().set(ModelParamName.FEATURE_TYPES,
			FlinkTypeConverter.getTypeString(
				TableUtil.findColTypes(in.getSchema(), getParams().get(HasFeatureCols.FEATURE_COLS))
			)
		);

		in = in.select(TreeUtil.trainColNames(getParams()));

		set(
			HasCategoricalCols.CATEGORICAL_COLS,
			TableUtil.getCategoricalCols(
				in.getSchema(),
				getParams().get(HasFeatureCols.FEATURE_COLS),
				getParams().contains(HasCategoricalCols.CATEGORICAL_COLS) ?
				getParams().get(HasCategoricalCols.CATEGORICAL_COLS) : null
			)
		);

		labels = Preprocessing.generateLabels(
			in, getParams(), Criteria.isRegression(getParams().get(HasTreeType.TREE_TYPE))
		);

		in = Preprocessing.castLabel(
			in, getParams(), labels, Criteria.isRegression(getParams().get(HasTreeType.TREE_TYPE))
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

		DataSet<Row> model;

		if (getParams().get(HasCreateTreeMode.CREATE_TREE_MODE).toUpperCase().equals("PARALLEL")) {
			model = parallelTrain(in);
		} else {
			model = seriesTrain(in);
		}

		setOutput(model, new TreeModelDataConverter(
				FlinkTypeConverter.getFlinkType(getParams().get(ModelParamName.LABEL_TYPE_NAME))
			).getModelSchema()
		);

		return (T) this;
	}

	private DataSet<Row> parallelTrain(BatchOperator<?> in) {
		BatchOperator<?> quantileModel = Preprocessing.generateQuantileDiscretizerModel(in, getParams());

		DataSet<Row> trainingDataSet = Preprocessing
			.castToQuantile(in, quantileModel, getParams())
			.getDataSet()
			// check null value in training dataset and throw exception when there are null values.
			.map(new CheckNullValue(in.getColNames()));

		final Params meta = getParams().clone();

		return new IterativeComQueue().setMaxIter(Integer.MAX_VALUE)
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
		private final Params meta;

		SerializeModelCompleteResultFunction(Params meta) {
			this.meta = meta;
		}

		@Override
		public List<Row> calc(ComContext context) {
			if (context.getTaskId() != 0) {
				return null;
			}

			TreeObj treeObj = context.getObj("treeObj");
			List<Row> stringIndexerModel = context.getObj("stringIndexerModel");
			List<Object[]> labelsList = context.getObj("labels");
			List<Row> model = TreeModelDataConverter.saveModelWithData(
				treeObj.getRoots(), meta, stringIndexerModel,
				labelsList == null || labelsList.isEmpty() ? null : labelsList.get(0)
			);
			return model;
		}
	}

	private static class Criterion extends CompareCriterionFunction {
		@Override
		public boolean calc(ComContext context) {
			TreeObj treeObj = context.getObj("treeObj");

			return treeObj.terminationCriterion();
		}
	}

	private static class CheckNullValue implements MapFunction<Row, Row> {
		private String[] cols;

		public CheckNullValue(String[] cols) {
			this.cols = cols;
		}

		@Override
		public Row map(Row value) throws Exception {
			for (int i = 0; i < value.getArity(); ++i) {
				if (value.getField(i) == null) {
					throw new IllegalArgumentException("There should not be null value in training dataset. col: "
						+ cols[i] + ", "
						+ "Maybe you can use {@code Imputer} to fill the missing values");
				}
			}
			return value;
		}
	}

	private DataSet<Row> seriesTrain(BatchOperator<?> in) {
		DataSet<Row> trainDataSet = in.getDataSet();

		MapPartitionOperator<Row, Tuple2<Integer, Row>> sampled = trainDataSet
			.mapPartition(new SampleData(
					get(HasSeed.SEED),
					get(HasSubsamplingRatio.SUBSAMPLING_RATIO),
					get(HasNumTreesDefaltAs10.NUM_TREES)
				)
			);

		if (getParams().get(HasSubsamplingRatio.SUBSAMPLING_RATIO) > 1.0) {
			DataSet<Long> cnt = DataSetUtils
				.countElementsPerPartition(trainDataSet)
				.sum(1)
				.map(new MapFunction<Tuple2<Integer, Long>, Long>() {
					@Override
					public Long map(Tuple2<Integer, Long> value) throws Exception {
						return value.f1;
					}
				});

			sampled = sampled.withBroadcastSet(cnt, "totalCnt");
		}

		DataSet<Integer> labelSize = labels.map(new MapFunction<Object[], Integer>() {
			@Override
			public Integer map(Object[] objects) throws Exception {
				return objects.length;
			}
		});

		DataSet<Tuple2<Integer, String>> pModel = sampled
			.groupBy(0)
			.withPartitioner(new AvgPartition())
			.reduceGroup(new SeriesTrainFunction(getParams()))
			.withBroadcastSet(stringIndexerModel.getDataSet(), "stringIndexerModel")
			.withBroadcastSet(labelSize, "labelSize");

		return pModel
			.reduceGroup(new SerializeModel(getParams()))
			.withBroadcastSet(stringIndexerModel.getDataSet(), "stringIndexerModel")
			.withBroadcastSet(labels, "labels");
	}

	private static class SeriesTrainFunction
		extends RichGroupReduceFunction<Tuple2<Integer, Row>, Tuple2<Integer, String>> {

		private static final Logger LOG = LoggerFactory.getLogger(SeriesTrainFunction.class);
		private Map<String, Integer> categoricalColsSize;
		private Params params;

		public SeriesTrainFunction(Params params) {
			this.params = params;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			categoricalColsSize = getRuntimeContext()
				.getBroadcastVariableWithInitializer(
					"stringIndexerModel",
					new BroadcastVariableInitializer<Row, Map<String, Integer>>() {
						@Override
						public Map<String, Integer> initializeBroadcastVariable(Iterable<Row> iterable) {
							List<Row> stringIndexerSerialized = new ArrayList<>();
							for (Row row : iterable) {
								stringIndexerSerialized.add(row);
							}
							return TreeUtil.extractCategoricalColsSize(
								stringIndexerSerialized, params.get(HasCategoricalCols.CATEGORICAL_COLS));
						}
					});

			if (!Criteria.isRegression(params.get(HasTreeType.TREE_TYPE))) {
				categoricalColsSize.put(
					params.get(HasLabelCol.LABEL_COL),
					getRuntimeContext()
						.getBroadcastVariableWithInitializer(
							"labelSize",
							new BroadcastVariableInitializer<Integer, Integer>() {
								@Override
								public Integer initializeBroadcastVariable(Iterable<Integer> iterable) {
									return iterable.iterator().next();
								}
							}
						)
				);
			}
		}

		@Override
		public void reduce(Iterable<Tuple2<Integer, Row>> values, Collector<Tuple2<Integer, String>> out)
			throws Exception {
			LOG.info("start the random forests training");
			List<Row> dataCache = new ArrayList<>();
			int treeId = 0;

			for (Tuple2<Integer, Row> value : values) {
				treeId = value.f0;
				dataCache.add(value.f1);
			}

			// create dense data.
			DenseData data = new DenseData(
				dataCache.size(),
				TreeUtil.getFeatureMeta(params.get(HasFeatureCols.FEATURE_COLS), categoricalColsSize),
				TreeUtil.getLabelMeta(
					params.get(HasLabelCol.LABEL_COL),
					params.get(HasFeatureCols.FEATURE_COLS).length,
					categoricalColsSize
				)
			);

			// read instance to data.
			data.readFromInstances(dataCache);

			// rewrite gain for this tree.
			params.set(Criteria.Gain.GAIN, getGainFromParams(params, treeId));

			// fit the decision tree.
			Node root = new DecisionTree(data, params).fit();

			// serialize the tree.
			for (String serialized : TreeModelDataConverter.serializeTree(root)) {
				out.collect(Tuple2.of(treeId, serialized));
			}

			LOG.info("end the random forests training");
		}
	}

	public static class AvgPartition implements Partitioner<Integer> {

		@Override
		public int partition(Integer key, int numPartitions) {
			return key % numPartitions;
		}
	}

	private static class SerializeModel extends RichGroupReduceFunction<Tuple2<Integer, String>, Row> {
		private Params params;
		private transient List<Row> stringIndexerModelSerialized;
		private transient Object[] labels;

		public SerializeModel(Params params) {
			this.params = params;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			stringIndexerModelSerialized = getRuntimeContext().getBroadcastVariable("stringIndexerModel");
			labels = getRuntimeContext().getBroadcastVariableWithInitializer("labels",
				new BroadcastVariableInitializer<Object[], Object[]>() {
					@Override
					public Object[] initializeBroadcastVariable(Iterable<Object[]> data) {
						Iterator<Object[]> iter = data.iterator();

						if (iter.hasNext()) {
							return iter.next();
						} else {
							return null;
						}
					}
				});
		}

		@Override
		public void reduce(Iterable<Tuple2<Integer, String>> values, Collector<Row> out) throws Exception {
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
		String treeType = params.get(HasTreeType.TREE_TYPE).trim().toUpperCase();

		if (treeType.equals("AVG")) {
			return getAvgGain(params.get(HasNumTreesDefaltAs10.NUM_TREES), treeId);
		} if (isInterval(treeType)) {
			return getIntervalGain(treeType, treeId);
		} else {
			return Criteria.Gain.valueOf(treeType);
		}
	}

	private static boolean isInterval(String treeType) {
		return treeType.contains(",");
	}

	private static Criteria.Gain getIntervalGain(String treeType, int id) {
		String[] intervalStrs = treeType.split(",");

		Preconditions.checkState(intervalStrs.length == 2, "Error format of treeType: " + treeType);

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

	public static class SampleData extends RichMapPartitionFunction<Row, Tuple2<Integer, Row>> {
		private long seed;
		private double factor;
		private int treeNum;

		public SampleData(long seed, double factor, int treeNum) {
			this.seed = seed;
			this.factor = factor;
			this.treeNum = treeNum;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			if (factor > 1.0) {
				factor = Math.min(factor / getRuntimeContext()
					.getBroadcastVariableWithInitializer("totalCnt", new BroadcastVariableInitializer<Long, Double>() {
						@Override
						public Double initializeBroadcastVariable(Iterable<Long> data) {
							for (Long cnt : data) {
								return cnt.doubleValue();
							}

							throw new RuntimeException(
								"Can not find total sample count of sample in training dataset if factor > 1.0"
							);
						}
					}), 1.0);
			}
		}

		@Override
		public void mapPartition(
			Iterable<Row> values,
			Collector<Tuple2<Integer, Row>> out)
			throws Exception {
			Random rand = new Random(this.seed + getRuntimeContext().getIndexOfThisSubtask());

			for (Row row : values) {
				for (int i = 0; i < treeNum; ++i) {
					double randNum = rand.nextDouble();

					if (randNum < factor) {
						out.collect(new Tuple2<>(i, row));
					}
				}
			}
		}
	}
}
