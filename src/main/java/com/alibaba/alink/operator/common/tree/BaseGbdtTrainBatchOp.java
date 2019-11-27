package com.alibaba.alink.operator.common.tree;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.tree.parallelcart.*;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.shared.tree.HasMaxDepth;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaultAs100;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Gradient Boosting(often abbreviated to GBDT or GBM) is a popular supervised learning model.
 * It is the best off-the-shelf supervised learning model for a wide range of problems,
 * especially problems with medium to large data size.
 * <p>
 * This implementation use histogram-based algorithm.
 * See:
 * "Mcrank: Learning to rank using multiple classification and gradient boosting", Ping Li et al., NIPS 2007,
 * for detail and experiments on histogram-based algorithm.
 * <p>
 * This implementation use layer-wise tree growing strategy,
 * rather than leaf-wise tree growing strategy
 * (like the one in "Lightgbm: A highly efficient gradient boosting decision tree", Guolin Ke et al., NIPS 2017),
 * because we found the former being faster in flink-based distributed computing environment.
 * <p>
 * This implementation use data-parallel algorithm.
 * See:
 * "A communication-efficient parallel algorithm for decision tree", Qi Meng et al., NIPS 2016
 * for an introduction on data-parallel, feature-parallel, etc., algorithms to construct decision forests.
 */
public class BaseGbdtTrainBatchOp<T extends BaseGbdtTrainBatchOp<T>> extends BatchOperator<T>
	implements GbdtTrainParams<T> {

	private static final Logger LOG = LoggerFactory.getLogger(BaseGbdtTrainBatchOp.class);
	// 0 = regression,
	// 1 = binary classification,
	// 2~4 = learning to rank, 2: lambdaMART+NDCG 3: lambdaMART+DCG 4: GBRank
	public int algoType = 0;
	private int featureColNum;
	private int binNum;
	private int treeNum;
	private int treeDepth;
	private int minSamplesPerLeaf;
	private double minSumHessianPerLeaf;
	private double learningRate;
	private double minInfoGain;
	private double subSamplingRatio;
	private double featureSubSamplingRatio;
	private int seed;

	public BaseGbdtTrainBatchOp() {
		this(new Params());
	}

	public BaseGbdtTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public T linkFrom(BatchOperator<?>... inputs) {
		BatchOperator<?> in = checkAndGetFirst(inputs);
		LOG.info(Thread.currentThread().getName(), "open");

		getParams().set(
			HasCategoricalCols.CATEGORICAL_COLS,
			TableUtil.getCategoricalCols(
				in.getSchema(),
				getFeatureCols(),
				getParams().contains(GbdtTrainParams.CATEGORICAL_COLS) ? getCategoricalCols() : null
			)
		);

		init(TableUtil.findColType(in.getSchema(), getParams().get(HasLabelCol.LABEL_COL)));

		int parallelism = Math.max(1, MLEnvironmentFactory
			.get(getMLEnvironmentId())
			.getExecutionEnvironment()
			.getParallelism());

		String[] trainColNames = trainColsWithGroup();

		in = in.select(trainColNames);

		DataSet<Object[]> labels = Preprocessing.generateLabels(in, getParams(), !(algoType == 1));

		labels = labels.map(new CheckNumLabels4BinaryClassifier(algoType));
		BatchOperator<?> stringIndexerModel = Preprocessing.generateStringIndexerModel(in, getParams());
		BatchOperator<?> quantileModel = Preprocessing.generateQuantileDiscretizerModel(in, getParams());

		DataSet<Row> trainDataSet =
			Preprocessing.castLabel(
				Preprocessing.castToQuantile(
					Preprocessing.castContinuousCols(
						Preprocessing.castCategoricalCols(
							in, stringIndexerModel, getParams()
						),
						getParams()
					),
					quantileModel,
					getParams()
				),
				getParams(),
				labels,
				!(algoType == 1)
			).getDataSet();

		DataSet<Tuple2<Long, int[][]>> categoryDetail =
			stringIndexerModel
				.getDataSet()
				.reduceGroup(new TransformCategoryDetail(trainColNames))
				.map(new CategoryDetailChecker(binNum))
				.name("Category Detail Checker");

		//construct tree data
		ArrayList<TreeParaContainer> treeArray = new ArrayList<>();
		TreeParaContainer curTree = new TreeParaContainer();
		treeArray.add(curTree);
		DataSet<TreeParaContainer> treeData =
			MLEnvironmentFactory
				.get(getMLEnvironmentId())
				.getExecutionEnvironment()
				.fromCollection(treeArray);

		//extract
		DataSet<Tuple3<byte[], Integer, Double>> dataExtractSet =
			trainDataSet
				.map(new ExtractData2ByteArray(algoType))
				.withBroadcastSet(categoryDetail, "categoryDetail")
				.name("Extract Data");

		//get data sum(and average)
		DataSet<Tuple2<Double, Integer>> sum = dataExtractSet
			.reduceGroup(
				new GroupReduceFunction<Tuple3<byte[], Integer, Double>, Tuple2<Double, Integer>>() {
					@Override
					public void reduce(Iterable<Tuple3<byte[], Integer, Double>> values,
									   Collector<Tuple2<Double, Integer>> out) throws Exception {
						double sum = 0.;
						int cnt = 0;
						for (Tuple3<byte[], Integer, Double> value : values) {
							cnt += 1;
							sum += value.f2;
						}

						out.collect(new Tuple2<>(sum, cnt));
					}
				})
			.name("Cal Data Average");

		// construct input data
		// here adjust label by data average when algoType == 0 (regression)
		DataSet<Tuple2<Long, List<PartitionedData>>> testArrayListBin = dataExtractSet
			.map(new InputTransformation(algoType, parallelism))
			.withBroadcastSet(sum, "gbdt.y.sum")
			.name("Transformat")
			.groupBy(0)
			.withPartitioner(new PartitionStatGbdt())
			.reduce(new ReduceFunction<Tuple2<Long, List<PartitionedData>>>() {
				@Override
				public Tuple2<Long, List<PartitionedData>> reduce(
					Tuple2<Long, List<PartitionedData>> value1,
					Tuple2<Long, List<PartitionedData>> value2) {
					value1.f1.addAll(value2.f1);
					return value1;
				}
			})
			.withBroadcastSet(sum, "gbdt.y.sum")
			//.partitionByRange(0)
			.partitionCustom(new Partitioner<Long>() {
				@Override
				public int partition(Long key, int numPartitions) {
					return key.intValue() % numPartitions;
				}
			}, 0);

		DataSet<Row> model = new IterativeComQueue()
			.setMaxIter(treeNum * treeDepth)
			.initWithPartitionedData("trainData", testArrayListBin)
			.initWithBroadcastData("gbdt.y.sum", sum)
			.initWithBroadcastData("categoryDetail", categoryDetail)
			.initWithBroadcastData("tree", treeData)
			.initWithBroadcastData("quantileModel", quantileModel.getDataSet())
			.initWithBroadcastData("stringIndexerModel", stringIndexerModel.getDataSet())
			.initWithBroadcastData("labels", labels)
			.add(new InitialTrainningBuffer(
				algoType, featureColNum, subSamplingRatio, featureSubSamplingRatio, seed))
			.add(new DataFormatToArray(featureColNum))
			.add(new ConstructLocalBin(algoType, binNum, featureColNum, treeDepth))
			.add(new AllReduce("gbdtBin"))
			.add(new CalBestSplit(
				binNum, featureColNum, minSamplesPerLeaf, minSumHessianPerLeaf, minInfoGain, learningRate))
			.add(new Split(algoType, featureColNum, treeDepth, subSamplingRatio, featureSubSamplingRatio,
				seed))
			.add(new UpdateTreeData(treeDepth, treeNum))
			.closeWith(new SaveModel(algoType, binNum, getParams()))
			.exec();

		setOutput(model, new TreeModelDataConverter(
				FlinkTypeConverter.getFlinkType(getParams().get(ModelParamName.LABEL_TYPE_NAME))
			).getModelSchema()
		);

		DataSet<Row> importance = model.reduceGroup(new TreeModelDataConverter.FeatureImportanceReducer(getFeatureCols()))
			.setParallelism(1);

		Table importanceTable = DataSetConversionUtil.toTable(getMLEnvironmentId(), importance,
			new String[]{"feature", "importance"}, new TypeInformation[]{Types.STRING, Types.LONG});
		this.setSideOutputTables(new Table[]{importanceTable});

		return (T) this;
	}

	public final static ParamInfo<Integer> ALGO_TYPE = ParamInfoFactory
		.createParamInfo("algoType", Integer.class)
		.build();

	private String[] trainColsWithGroup() {
		// algoType >= 2: learning to rank, needs to get group col name
		List<String> trainCols = new ArrayList<>(Arrays.asList(getFeatureCols()));
		if (algoType >= 2) {
			trainCols.add(getGroupCol());
		}
		trainCols.add(getLabelCol());

		return trainCols.toArray(new String[0]);
	}

	private void init(TypeInformation<?> labelType) {
		//set depth as minimum value of depth itself and depth implicated by leaf
		//notice: "maxLeaves" is implemented approximately here.
		int depth = getMaxDepth();

		if (getMaxLeaves() > 0) {
			int depthLeaf = ((Double) (Math.log(getMaxLeaves() + 0.01) / Math.log(2.0))).intValue() + 1;
			if (depthLeaf < depth) {
				depth = depthLeaf;
			}
		}

		Params gbdtMeta = new Params();

		getParams().set(ALGO_TYPE, algoType);

		if (algoType == 1) {
			getParams().set(ModelParamName.LABEL_TYPE_NAME, FlinkTypeConverter.getTypeString(labelType));
		} else {
			getParams().set(ModelParamName.LABEL_TYPE_NAME, FlinkTypeConverter.getTypeString(Types.DOUBLE));
		}

		gbdtMeta.set(HasFeatureCols.FEATURE_COLS, getFeatureCols());
		gbdtMeta.set(HasLabelCol.LABEL_COL, getLabelCol());
		gbdtMeta.set(HasCategoricalCols.CATEGORICAL_COLS, getCategoricalCols());

		gbdtMeta.set(HasNumTreesDefaultAs100.NUM_TREES, getNumTrees());
		gbdtMeta.set(HasMaxDepth.MAX_DEPTH, depth);

		getParams().merge(gbdtMeta);

		featureColNum = getFeatureCols().length;
		treeNum = getNumTrees();

		//notice: implementation uses (real_depth - 1)
		treeDepth = getParams().get(HasMaxDepth.MAX_DEPTH) - 1;

		binNum = getMaxBins();
		if (binNum > 255) {
			throw new RuntimeException("binNum must be less or equal than 255.");
		}

		minSamplesPerLeaf = getMinSamplesPerLeaf();

		minSumHessianPerLeaf = getMinSumHessianPerLeaf();

		learningRate = getLearningRate();

		minInfoGain = getMinInfoGain();

		subSamplingRatio = getSubsamplingRatio();
		featureSubSamplingRatio = getFeatureSubsamplingRatio();
		seed = (int) new Date().getTime();
	}

	public static class ExtractData2ByteArray extends RichMapFunction<Row, Tuple3<byte[], Integer, Double>> {
		private int algoType;
		private transient HashMap<Integer, Integer> categoryMax;

		public ExtractData2ByteArray(int algoType) {
			this.algoType = algoType;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			LOG.info(Thread.currentThread().getName(), "open");
			Tuple2<Long, int[][]> categoryDetail = getRuntimeContext()
				.getBroadcastVariableWithInitializer("categoryDetail",
					new BroadcastVariableInitializer<Tuple2<Long, int[][]>, Tuple2<Long, int[][]>>() {
						@Override
						public Tuple2<Long, int[][]> initializeBroadcastVariable(Iterable<Tuple2<Long, int[][]>> data) {
							// flink check that the returned value of this function is null or not
							// to determine that call this function or not. so we should check the
							// data is null or not.
							if (data == null || !data.iterator().hasNext()) {
								return null;
							}
							return data.iterator().next();
						}
					});

			if (categoryDetail != null) {

				categoryMax = new HashMap<>(categoryDetail.f1.length * 2);

				for (int category = 0; category < categoryDetail.f1.length; category++) {
					categoryMax.put(categoryDetail.f1[category][0], categoryDetail.f1[category][1]);
				}
			} else {
				categoryMax = new HashMap<>(0);
			}
		}

		@Override
		public Tuple3<byte[], Integer, Double> map(Row value) throws Exception {
			if (algoType >= 2) {
				byte[] feature = new byte[value.getArity() - 2];
				int group = ((Number) value.getField(value.getArity() - 2)).intValue();
				double label = ((Number) value.getField(value.getArity() - 1)).doubleValue();

				for (int i = 0; i < value.getArity() - 2; ++i) {
					if (value.getField(i) == null) {
						feature[i] = ((Integer) (categoryMax.get(i).byteValue() - 1)).byteValue();
					} else {
						feature[i] = ((Number) value.getField(i)).byteValue();
					}
				}

				return new Tuple3<>(feature, group, label);
			} else {
				byte[] feature = new byte[value.getArity() - 1];
				double label = ((Number) value.getField(value.getArity() - 1)).doubleValue();

				for (int i = 0; i < value.getArity() - 1; ++i) {
					if (value.getField(i) == null) {
						if (categoryMax.containsKey(i)) {
							feature[i] = ((Integer) (categoryMax.get(i).byteValue() - 1)).byteValue();
						} else {
							throw new IllegalArgumentException("There is null value in the numerical feature!");
						}
					} else {
						feature[i] = ((Number) value.getField(i)).byteValue();
					}
				}

				return new Tuple3<>(feature, 0, label);
			}
		}
	}

	public static class InputTransformation
		extends RichMapFunction<Tuple3<byte[], Integer, Double>, Tuple2<Long, List<PartitionedData>>> {

		double avg;
		int algoType;
		int partitionNum;
		Random random;

		public InputTransformation(int algoType, int partitionNum) {
			this.algoType = algoType;
			this.partitionNum = partitionNum;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			LOG.info(Thread.currentThread().getName(), "open");

			Tuple2<Double, Integer> sum =
				getRuntimeContext()
					.getBroadcastVariableWithInitializer(
						"gbdt.y.sum",
						new BroadcastVariableInitializer<Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
							@Override
							public Tuple2<Double, Integer> initializeBroadcastVariable(
								Iterable<Tuple2<Double, Integer>> data) {
								return data.iterator().next();
							}
						}
					);

			avg = sum.f0 / ((double) sum.f1);

			random = new Random();
		}

		@Override
		public Tuple2<Long, List<PartitionedData>> map(
			Tuple3<byte[], Integer, Double> input) throws Exception {

			PartitionedData partitionData = new PartitionedData();

			partitionData.setFeatures(input.f0);
			partitionData.setNodeId(0);

			//adjust label by avg
			//this is only needed in regression
			//binary classification/learning to rank doesn't use this
			if (algoType == 0) {
				partitionData.setLabel(input.f2 - avg);
			} else if (algoType == 1) {
				partitionData.setLabel(input.f2);
			} else if (algoType == 2 || algoType == 3 || algoType == 4) {
				partitionData.setGroup(input.f1);
				partitionData.setLabel(input.f2);
			} else {
				assert (false);
			}

			List<PartitionedData> dataList = new ArrayList<>(1);
			dataList.add(partitionData);

			if (algoType == 0 || algoType == 1) {
				//parNum * 10 to overcome difficulty in PartitionByRange
				return new Tuple2<>((long) (random.nextInt(partitionNum * 10)), dataList);
			} else {
				//partition by group
				return new Tuple2<>((long) partitionData.getGroup(), dataList);
			}
		}
	}

	public static class CategoryDetailChecker implements MapFunction<Tuple2<Long, int[][]>, Tuple2<Long, int[][]>> {
		int binNum;

		CategoryDetailChecker(int binNum) {
			this.binNum = binNum;
		}

		@Override
		public Tuple2<Long, int[][]> map(Tuple2<Long, int[][]> value) throws Exception {
			for (int index = 0; index < value.f1.length; index++) {
				//size must be less or equal than binNum
				int columnSize = value.f1[index][1];

				if (columnSize > binNum) {
					throw new RuntimeException(
						String.format(
							"For categorical variable, number " +
								"of category must be less or equal than binNum  " +
								"(current value %d, categorical size: %d).", binNum, columnSize)
					);
				}
			}

			return value;
		}
	}

	public static class PartitionStatGbdt implements Partitioner<Long> {
		@Override
		public int partition(Long key, int numPartitions) {
			return (int) (key % numPartitions);
		}
	}

	public static class TransformCategoryDetail
		extends RichGroupReduceFunction<Row, Tuple2<Long, int[][]>> {
		String[] colNames;

		public TransformCategoryDetail(String[] colNames) {
			this.colNames = colNames;
		}

		@Override
		public void reduce(Iterable<Row> values, Collector<Tuple2<Long, int[][]>> out) throws Exception {
			List<Row> modelSerialized = new ArrayList<>();

			for (Row val : values) {
				modelSerialized.add(val);
			}

			List<int[]> detailList = new ArrayList<>();

			if (modelSerialized.isEmpty()) {
				detailList.add(new int[]{-1, 0});
			} else {
				MultiStringIndexerModelData model =
					new MultiStringIndexerModelDataConverter()
						.load(modelSerialized);

				for (String col : model.meta.get(HasSelectedCols.SELECTED_COLS)) {
					detailList.add(new int[]{
							TableUtil.findColIndex(colNames, col),
							(int) model.getNumberOfTokensOfColumn(col)
						}
					);
				}
			}

			int[][] detail = new int[detailList.size()][detailList.get(0).length];
			for (int index = 0; index < detailList.size(); index++) {
				detail[index] = detailList.get(index);

				//insert a dummy value at back to represent "null"
				detail[index][1]++;
			}

			out.collect(new Tuple2<>((long) 0, detail));
		}
	}

	private static class CheckNumLabels4BinaryClassifier implements MapFunction<Object[], Object[]> {
		private int algoType;

		public CheckNumLabels4BinaryClassifier(int algoType) {
			this.algoType = algoType;
		}

		@Override
		public Object[] map(Object[] value) throws Exception {
			if (algoType == 1) {
				if (value == null || value.length != 2) {
					throw new IllegalArgumentException("The gbdt only support binary class right now.");
				}
			}
			return value;
		}
	}
}
