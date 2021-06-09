package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp.AvgPartition;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.operator.common.tree.TreeModelDataConverter;
import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.operator.common.tree.itree.IForest;
import com.alibaba.alink.params.classification.IndividualTreeParams;
import com.alibaba.alink.params.outlier.IsolationForestsTrainParams;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.tree.HasMaxDepth;
import com.alibaba.alink.params.shared.tree.HasMaxLeaves;
import com.alibaba.alink.params.shared.tree.HasMinSampleRatioPerChild;
import com.alibaba.alink.params.shared.tree.HasMinSamplesPerLeaf;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaltAs10;
import com.alibaba.alink.params.shared.tree.HasSeed;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class IsolationForestsTrainBatchOp extends BatchOperator <IsolationForestsTrainBatchOp>
	implements IsolationForestsTrainParams <IsolationForestsTrainBatchOp> {

	private static final long serialVersionUID = -4130896472912219071L;

	public IsolationForestsTrainBatchOp() {
		this(new Params());
	}

	public IsolationForestsTrainBatchOp(Params params) {
		super(params);
	}

	@Deprecated
	public IsolationForestsTrainBatchOp(
		String[] featureNames, int depth, int bestLeafs,
		int minLeafSample, int minPercent, double factor,
		long seed, int treeNum) {

		this(new Params());

		getParams().set(HasFeatureCols.FEATURE_COLS, featureNames);
		getParams().set(HasMaxDepth.MAX_DEPTH, depth);
		getParams().set(HasMaxLeaves.MAX_LEAVES, bestLeafs);
		getParams().set(HasMinSamplesPerLeaf.MIN_SAMPLES_PER_LEAF, minLeafSample);
		getParams().set(HasMinSampleRatioPerChild.MIN_SAMPLE_RATIO_PERCHILD, (double) minPercent);
		getParams().set(HasSubsamplingRatio.SUBSAMPLING_RATIO, factor);
		getParams().set(HasSeed.SEED, seed);
		getParams().set(HasNumTreesDefaltAs10.NUM_TREES, treeNum);
	}

	@Override
	public IsolationForestsTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		in = in.select(TreeUtil.trainColNames(getParams()));

		getParams().set(
			IndividualTreeParams.CATEGORICAL_COLS,
			TableUtil.getCategoricalCols(
				in.getSchema(),
				getParams().get(IndividualTreeParams.FEATURE_COLS),
				getParams().contains(IndividualTreeParams.CATEGORICAL_COLS) ?
					getParams().get(IndividualTreeParams.CATEGORICAL_COLS) : null
			)
		);

		getParams().set(ModelParamName.LABEL_TYPE_NAME, FlinkTypeConverter.getTypeString(Types.DOUBLE));

		DataSet <Row> trainDataSet = Preprocessing
			.castContinuousCols(in, getParams())
			.getDataSet();

		MapPartitionOperator <Row, Tuple2 <Integer, Row>> sampled = trainDataSet
			.mapPartition(
				new SampleData(
					getSeed(), getSubsamplingRatio(), getNumTrees()
				)
			);

		if (getSubsamplingRatio() > 1.0) {
			DataSet <Long> cnt = DataSetUtils
				.countElementsPerPartition(trainDataSet)
				.sum(1)
				.map(new MapFunction <Tuple2 <Integer, Long>, Long>() {
					private static final long serialVersionUID = -7700977791481640443L;

					@Override
					public Long map(Tuple2 <Integer, Long> value) {
						return value.f1;
					}
				});

			sampled = sampled.withBroadcastSet(cnt, "totalCnt");
		}

		DataSet <Double> groupedCount = sampled
			.groupBy(0)
			.reduceGroup(new GroupReduceFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, Double>>() {
				private static final long serialVersionUID = 85932222033848509L;

				@Override
				public void reduce(Iterable <Tuple2 <Integer, Row>> values, Collector <Tuple2 <Integer, Double>> out) {
					Integer groupId = null;
					double count = 0.0;

					for (Tuple2 <Integer, Row> value : values) {
						groupId = value.f0;
						count += 1.0;
					}

					if (groupId != null) {
						out.collect(Tuple2.of(groupId, count));
					}
				}
			})
			.reduceGroup(new GroupReduceFunction <Tuple2 <Integer, Double>, Double>() {
				private static final long serialVersionUID = -8217505090252009352L;

				@Override
				public void reduce(Iterable <Tuple2 <Integer, Double>> values, Collector <Double> out) {
					double max = 0.0;

					for (Tuple2 <Integer, Double> value : values) {
						max = max < value.f1 ? value.f1 : max;
					}

					out.collect(max);
				}
			});

		DataSet <Row> model = sampled
			.groupBy(0)
			.withPartitioner(new AvgPartition())
			.reduceGroup(new IForestTrain(getParams()))
			.reduceGroup(new SerializeModel(getParams()))
			.withBroadcastSet(groupedCount, "isolation.group.count");

		setOutput(model, new TreeModelDataConverter(Types.DOUBLE).getModelSchema());
		return this;
	}

	public static class IForestTrain extends RichGroupReduceFunction <Tuple2 <Integer, Row>, Tuple2 <Integer,
		String>> {
		private static final long serialVersionUID = -4292597163635395117L;
		private final Params params;

		public IForestTrain(Params params) {
			this.params = params;
		}

		private Tuple2 <Integer, double[][]> readData(Iterable <Tuple2 <Integer, Row>> values) {
			int treeId = -1;
			int featureSize = 0;
			ArrayList <Row> cache = new ArrayList <>();
			for (Tuple2 <Integer, Row> row : values) {
				treeId = row.f0;
				featureSize = row.f1.getArity();
				cache.add(row.f1);
			}

			double[][] data = new double[featureSize][cache.size()];

			for (int i = 0; i < cache.size(); ++i) {
				for (int j = 0; j < featureSize; ++j) {
					data[j][i] = (double) cache.get(i).getField(j);
				}
			}

			return Tuple2.of(treeId, data);
		}

		@Override
		public void reduce(Iterable <Tuple2 <Integer, Row>> values, Collector <Tuple2 <Integer, String>> out)
			throws Exception {
			Tuple2 <Integer, double[][]> treeIdWithData = readData(values);

			Node root = new IForest(treeIdWithData.f1, params).fit();

			List <String> serialized = TreeModelDataConverter.serializeTree(root);

			for (String str : serialized) {
				out.collect(Tuple2.of(treeIdWithData.f0, str));
			}
		}
	}

	public static class SampleData extends RichMapPartitionFunction <Row, Tuple2 <Integer, Row>> {
		private static final long serialVersionUID = -8114271430777216933L;
		private final long seed;
		private double factor;
		private final int treeNum;

		public SampleData(long seed, double factor, int treeNum) {
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

							throw new RuntimeException(
								"Can not find total sample count of sample in training dataset if factor > 1.0"
							);
						}
					}), 1.0);
			}
		}

		@Override
		public void mapPartition(
			Iterable <Row> values,
			Collector <Tuple2 <Integer, Row>> out)
			throws Exception {
			Random rand = new Random(this.seed + getRuntimeContext().getIndexOfThisSubtask());

			for (Row row : values) {
				for (int i = 0; i < treeNum; ++i) {
					double randNum = rand.nextDouble();

					if (randNum < factor) {
						out.collect(new Tuple2 <>(i, row));
					}
				}
			}
		}
	}

	private static class SerializeModel extends RichGroupReduceFunction <Tuple2 <Integer, String>, Row> {
		private static final long serialVersionUID = 7978754191537540385L;
		private final Params params;

		public SerializeModel(Params params) {
			this.params = params;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			Double maxCount = (Double) getRuntimeContext()
				.getBroadcastVariable("isolation.group.count")
				.get(0);
			params.set(IForest.ISOLATION_GROUP_MAXCOUNT, maxCount);
		}

		@Override
		public void reduce(Iterable <Tuple2 <Integer, String>> values, Collector <Row> out) throws Exception {
			TreeModelDataConverter.saveModelWithData(
				StreamSupport.stream(values.spliterator(), false)
					.collect(Collectors.groupingBy(x -> x.f0, Collectors.mapping(x -> x.f1, Collectors.toList())))
					.entrySet()
					.stream()
					.sorted(Map.Entry.comparingByKey())
					.map(x -> TreeModelDataConverter.deserializeTree(x.getValue()))
					.collect(Collectors.toList()),
				params,
				null,
				null
			).forEach(out::collect);
		}
	}
}
