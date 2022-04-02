package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.operator.common.tree.TreeModelDataConverter;
import com.alibaba.alink.operator.common.tree.parallelcart.booster.BoosterType;
import com.alibaba.alink.operator.common.tree.parallelcart.communication.AllReduceT;
import com.alibaba.alink.operator.common.tree.parallelcart.communication.ReduceScatter;
import com.alibaba.alink.operator.common.tree.parallelcart.criteria.CriteriaType;
import com.alibaba.alink.operator.common.tree.parallelcart.data.DataUtil;
import com.alibaba.alink.operator.common.tree.parallelcart.leafscoreupdater.LeafScoreUpdaterType;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossType;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import com.alibaba.alink.params.regression.LambdaMartNdcgParams;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {
	@PortSpec(PortType.MODEL),
	@PortSpec(value = PortType.DATA, desc = PortDesc.FEATURE_IMPORTANCE)
})
@ParamSelectColumnSpec(
	name = "featureCols", allowedTypeCollections = TypeCollections.TREE_FEATURE_TYPES
)
@ParamSelectColumnSpec(
	name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES
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
public abstract class BaseGbdtTrainBatchOp<T extends BaseGbdtTrainBatchOp <T>> extends BatchOperator <T> {

	private static final Logger LOG = LoggerFactory.getLogger(BaseGbdtTrainBatchOp.class);
	private static final long serialVersionUID = 6942357843795354849L;

	public BaseGbdtTrainBatchOp() {
		this(new Params());
	}

	public BaseGbdtTrainBatchOp(Params params) {
		super(params);
	}

	public final static ParamInfo <Integer> ALGO_TYPE = ParamInfoFactory
		.createParamInfo("algoType", Integer.class)
		.build();

	public final static ParamInfo <Boolean> USE_MISSING = ParamInfoFactory
		.createParamInfo("useMissing", Boolean.class)
		.setHasDefaultValue(true)
		.build();

	public final static ParamInfo <Boolean> USE_ONEHOT = ParamInfoFactory
		.createParamInfo("useOneHot", Boolean.class)
		.setHasDefaultValue(false)
		.build();

	public final static ParamInfo <Boolean> USE_EPSILON_APPRO_QUANTILE = ParamInfoFactory
		.createParamInfo("useEpsilonApproQuantile", Boolean.class)
		.setHasDefaultValue(false)
		.build();

	public final static ParamInfo <Double> SKETCH_EPS = ParamInfoFactory
		.createParamInfo("sketchEps", Double.class)
		.setHasDefaultValue(0.03)
		.build();

	public final static ParamInfo <Double> SKETCH_RATIO = ParamInfoFactory
		.createParamInfo("sketchRatio", Double.class)
		.setHasDefaultValue(2.0)
		.build();

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		LOG.info("gbdt train start");

		if (!Preprocessing.isSparse(getParams())) {
			getParams().set(
				HasCategoricalCols.CATEGORICAL_COLS,
				TableUtil.getCategoricalCols(
					in.getSchema(),
					Preprocessing.checkAndGetOptionalFeatureCols(getParams(), this),
					getParams().contains(GbdtTrainParams.CATEGORICAL_COLS) ? getParams()
						.get(GbdtTrainParams.CATEGORICAL_COLS) : null
				)
			);
		}

		LossType loss = getParams().get(LossUtils.LOSS_TYPE);

		getParams().set(ALGO_TYPE, LossUtils.lossTypeToInt(loss));

		rewriteLabelType(in.getSchema(), getParams());

		if (!Preprocessing.isSparse(getParams())) {
			getParams().set(
				ModelParamName.FEATURE_TYPES,
				FlinkTypeConverter.getTypeString(
					TableUtil.findColTypes(
						in.getSchema(),
						Preprocessing.checkAndGetOptionalFeatureCols(getParams(), this)
					)
				)
			);
		}

		if (LossUtils.isRanking(getParams().get(LossUtils.LOSS_TYPE))) {
			if (!getParams().contains(LambdaMartNdcgParams.GROUP_COL)) {
				throw new IllegalArgumentException("Group column should be set in ranking loss function.");
			}
		}

		String[] trainColNames = trainColsWithGroup();

		//check label if has null value or not.
		final String labelColName = this.getParams().get(HasLabelCol.LABEL_COL);
		final int labelColIdx = TableUtil.findColIndex(in.getSchema(), labelColName);

		in = new TableSourceBatchOp(DataSetConversionUtil.toTable(in.getMLEnvironmentId(),
			in.getDataSet()
				.map(new MapFunction <Row, Row>() {
					@Override
					public Row map(Row row) throws Exception {
						if (null == row.getField(labelColIdx)) {
							throw new RuntimeException("label col has null values.");
						}
						return row;
					}
				}),
			in.getSchema()))
			.setMLEnvironmentId(in.getMLEnvironmentId());

		in = Preprocessing.select(in, trainColNames);

		DataSet <Object[]> labels = Preprocessing.generateLabels(
			in, getParams(),
			LossUtils.isRegression(loss) || LossUtils.isRanking(loss)
		);

		if (LossUtils.isClassification(loss)) {
			labels = labels.map(new CheckNumLabels4BinaryClassifier());
		}

		DataSet <Row> trainDataSet;
		BatchOperator <?> stringIndexerModel;
		BatchOperator <?> quantileModel;

		if (getParams().get(USE_ONEHOT)) {
			// create empty string indexer model.
			stringIndexerModel = Preprocessing.generateStringIndexerModel(in, new Params());

			// create empty quantile model.
			quantileModel = Preprocessing.generateQuantileDiscretizerModel(
				in, new Params().set(HasFeatureCols.FEATURE_COLS, new String[] {})
					.set(HasCategoricalCols.CATEGORICAL_COLS, new String[] {})
			);

			trainDataSet = Preprocessing.castLabel(
				in, getParams(), labels, LossUtils.isRegression(loss) || LossUtils.isRanking(loss)
			).getDataSet();
		} else if (getParams().get(USE_EPSILON_APPRO_QUANTILE)) {
			// create string indexer model
			stringIndexerModel = Preprocessing.generateStringIndexerModel(in, getParams());

			// create empty quantile model
			quantileModel = Preprocessing.generateQuantileDiscretizerModel(
				in, new Params().set(HasFeatureCols.FEATURE_COLS, new String[] {})
					.set(HasCategoricalCols.CATEGORICAL_COLS, new String[] {})
			);

			trainDataSet = Preprocessing.castLabel(
				Preprocessing.isSparse(getParams()) ?
					in :
					Preprocessing.castContinuousCols(
						Preprocessing.castCategoricalCols(
							in, stringIndexerModel, getParams()
						),
						getParams()
					),
				getParams(), labels, LossUtils.isRegression(loss) || LossUtils.isRanking(loss)
			).getDataSet();
		} else {
			stringIndexerModel = Preprocessing.generateStringIndexerModel(in, getParams());
			quantileModel = Preprocessing.generateQuantileDiscretizerModel(in, getParams());

			trainDataSet =
				Preprocessing.castLabel(
					Preprocessing.castToQuantile(
						Preprocessing.isSparse(getParams()) ?
							in :
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
					LossUtils.isRegression(loss) || LossUtils.isRanking(loss)
				).getDataSet();
		}

		if (LossUtils.isRanking(getParams().get(LossUtils.LOSS_TYPE))) {
			trainDataSet = trainDataSet
				.partitionCustom(new Partitioner <Number>() {
					private static final long serialVersionUID = -7790649477852624964L;

					@Override
					public int partition(Number key, int numPartitions) {
						return (int) (key.longValue() % numPartitions);
					}
				}, 0);
		}

		DataSet <Tuple2 <Double, Long>> sum = trainDataSet
			.mapPartition(new MapPartitionFunction <Row, Tuple2 <Double, Long>>() {
				private static final long serialVersionUID = -8333738060239409640L;

				@Override
				public void mapPartition(Iterable <Row> iterable, Collector <Tuple2 <Double, Long>> collector)
					throws Exception {
					double sum = 0.;
					long cnt = 0;
					for (Row row : iterable) {
						sum += ((Number) row.getField(row.getArity() - 1)).doubleValue();
						cnt++;
					}

					collector.collect(Tuple2.of(sum, cnt));
				}
			})
			.reduce(new ReduceFunction <Tuple2 <Double, Long>>() {
				private static final long serialVersionUID = -6464200385237876961L;

				@Override
				public Tuple2 <Double, Long> reduce(Tuple2 <Double, Long> t0, Tuple2 <Double, Long> t1)
					throws Exception {
					return Tuple2.of(t0.f0 + t1.f0, t0.f1 + t1.f1);
				}
			});

		DataSet <FeatureMeta> featureMetas;

		if (getParams().get(USE_ONEHOT)) {
			featureMetas = DataUtil.createOneHotFeatureMeta(trainDataSet, getParams(), trainColNames);
		} else if (getParams().get(USE_EPSILON_APPRO_QUANTILE)) {
			featureMetas = DataUtil.createEpsilonApproQuantileFeatureMeta(
				trainDataSet, stringIndexerModel.getDataSet(), getParams(), trainColNames, getMLEnvironmentId()
			);
		} else {
			featureMetas = DataUtil.createFeatureMetas(
				quantileModel.getDataSet(), stringIndexerModel.getDataSet(), getParams()
			);
		}

		{
			getParams().set(BoosterType.BOOSTER_TYPE, BoosterType.HESSION_BASE);
			getParams().set(
				CriteriaType.CRITERIA_TYPE,
				CriteriaType.valueOf(getParams().get(GbdtTrainParams.CRITERIA).toString())
			);

			if (getParams().get(GbdtTrainParams.NEWTON_STEP)) {
				getParams().set(
					LeafScoreUpdaterType.LEAF_SCORE_UPDATER_TYPE,
					LeafScoreUpdaterType.NEWTON_SINGLE_STEP_UPDATER
				);
			} else {
				getParams().set(
					LeafScoreUpdaterType.LEAF_SCORE_UPDATER_TYPE,
					LeafScoreUpdaterType.WEIGHT_AVG_UPDATER
				);
			}
		}

		IterativeComQueue comQueue = new IterativeComQueue()
			.initWithPartitionedData("trainData", trainDataSet)
			.initWithBroadcastData("gbdt.y.sum", sum)
			.initWithBroadcastData("quantileModel", quantileModel.getDataSet())
			.initWithBroadcastData("stringIndexerModel", stringIndexerModel.getDataSet())
			.initWithBroadcastData("labels", labels)
			.initWithBroadcastData("featureMetas", featureMetas)
			.add(new InitBoostingObjs(getParams()))
			.add(new Boosting())
			.add(new Bagging())
			// training tree starts here.
			.add(new InitTreeObjs());

		if (getParams().get(USE_EPSILON_APPRO_QUANTILE)) {
			comQueue
				.add(new BuildLocalSketch())
				.add(
					new AllReduceT <>(
						BuildLocalSketch.SKETCH,
						BuildLocalSketch.FEATURE_SKETCH_LENGTH,
						new BuildLocalSketch.SketchReducer(getParams()),
						EpsilonApproQuantile.WQSummary.class
					)
				)
				.add(new FinalizeBuildSketch());
		}

		comQueue
			.add(new ConstructLocalHistogram())
			.add(new ReduceScatter("histogram", "histogram", "recvcnts", AllReduce.SUM))
			.add(new CalcFeatureGain())
			.add(
				new AllReduceT <>(
					"best",
					"bestLength",
					new NodeReducer(),
					Node.class
				)
			)
			.add(new SplitInstances())
			.add(new UpdateLeafScore())
			// F_{m} = F_{m-1} + \rhoH(x)
			.add(new UpdatePredictionScore())
			// training tree ends here.
			.setCompareCriterionOfNode0(new TerminateCriterion())
			.closeWith(new SaveModel(getParams()));

		DataSet <Row> model = comQueue.exec();
		setOutput(model, new TreeModelDataConverter(
				FlinkTypeConverter.getFlinkType(getParams().get(ModelParamName.LABEL_TYPE_NAME))
			).getModelSchema()
		);

		this.setSideOutputTables(new Table[] {
			DataSetConversionUtil.toTable(
				getMLEnvironmentId(),
				model.reduceGroup(
					new TreeModelDataConverter.FeatureImportanceReducer()
				),
				new String[] {
					getParams().get(TreeModelDataConverter.IMPORTANCE_FIRST_COL),
					getParams().get(TreeModelDataConverter.IMPORTANCE_SECOND_COL)
				},
				new TypeInformation[] {Types.STRING, Types.DOUBLE}
			)
		});

		return (T) this;
	}

	public static void rewriteLabelType(TableSchema schema, Params params) {
		LossType loss = params.get(LossUtils.LOSS_TYPE);

		if (LossUtils.isClassification(loss)) {
			params.set(
				ModelParamName.LABEL_TYPE_NAME,
				FlinkTypeConverter.getTypeString(
					TableUtil.findColType(schema, params.get(GbdtTrainParams.LABEL_COL))
				)
			);
		} else {
			params.set(
				ModelParamName.LABEL_TYPE_NAME,
				FlinkTypeConverter.getTypeString(Types.DOUBLE)
			);
		}
	}

	private String[] trainColsWithGroup() {
		// group column
		List <String> trainCols = new ArrayList <>();
		if (LossUtils.isRanking(getParams().get(LossUtils.LOSS_TYPE))) {
			trainCols.add(getParams().get(LambdaMartNdcgParams.GROUP_COL));
		}

		//features
		if (Preprocessing.isSparse(getParams())) {
			trainCols.add(
				Preprocessing.checkAndGetOptionalVectorCols(getParams(), this)
			);
		} else {
			trainCols.addAll(Arrays.asList(
				Preprocessing.checkAndGetOptionalFeatureCols(getParams(), this)
			));
		}

		//label
		trainCols.add(getParams().get(GbdtTrainParams.LABEL_COL));

		return trainCols.toArray(new String[0]);
	}

	private final static class CheckNumLabels4BinaryClassifier implements MapFunction <Object[], Object[]> {
		private static final long serialVersionUID = -8337756848972278905L;

		public CheckNumLabels4BinaryClassifier() {
		}

		@Override
		public Object[] map(Object[] value) throws Exception {
			if (value == null || value.length != 2) {
				throw new IllegalArgumentException("The gbdt only support binary class right now.");
			}
			return value;
		}
	}

	private final static class NodeReducer implements AllReduceT.SerializableBiConsumer <Node[], Node[]> {
		private static final long serialVersionUID = 6875638618412288149L;

		@Override
		public void accept(Node[] left, Node[] right) {
			for (int i = 0; i < left.length; ++i) {
				if (left[i] == null && right[i] != null) {
					left[i] = right[i];
				} else if (left[i] != null && right[i] != null) {
					if (left[i].getGain() < right[i].getGain()) {
						left[i].copy(right[i]);
					} else if (left[i].getGain() == right[i].getGain()) {
						if (left[i].getFeatureIndex() < right[i].getFeatureIndex()) {
							left[i].copy(right[i]);
						}
					}
				}
			}
		}
	}

}
