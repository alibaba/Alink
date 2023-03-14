package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.NumericalTypeCastMapper;
import com.alibaba.alink.operator.common.outlier.IForestDetector.IForestTrain;
import com.alibaba.alink.operator.common.outlier.IForestDetector.Node;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector.IForestModel;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector.IForestModelDataConverter;
import com.alibaba.alink.params.outlier.IForestTrainParams;
import com.alibaba.alink.operator.common.outlier.OutlierUtil;
import com.alibaba.alink.params.outlier.WithMultiVarParams;
import com.alibaba.alink.params.dataproc.HasTargetType.TargetType;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@InputPorts(values = @PortSpec(value = PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@NameCn("IForest模型异常检测训练")
@NameEn("IForest model outlier")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.outlier.IForestModelOutlier")
public class IForestModelOutlierTrainBatchOp
	extends BatchOperator <IForestModelOutlierTrainBatchOp>
	implements IForestTrainParams <IForestModelOutlierTrainBatchOp> {

	private static final Logger LOG = LoggerFactory.getLogger(IForestModelOutlierTrainBatchOp.class);

	public IForestModelOutlierTrainBatchOp() {
		this(null);
	}

	public IForestModelOutlierTrainBatchOp(Params params) {
		super(params);
	}

	private static final double LOG2 = Math.log(2);

	@Override
	public IForestModelOutlierTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		final Params params = getParams().clone();
		final int numTrees = getNumTrees();
		final int subsamplingSize = getSubsamplingSize();

		final String[] colNames = in.getColNames();
		final TypeInformation<?>[] colTypes = in.getColTypes();

		DataSet <Row> input = in.getDataSet();

		DataSet <Integer> maxVectorSize;

		if (params.contains(WithMultiVarParams.VECTOR_COL)) {

			final int vectorIndex = TableUtil.findColIndexWithAssertAndHint(
				in.getSchema(), params.get(WithMultiVarParams.VECTOR_COL)
			);

			maxVectorSize = input
				.map(new MapFunction <Row, Vector>() {
					@Override
					public Vector map(Row value) throws Exception {
						return (Vector) value.getField(vectorIndex);
					}
				})
				.map(new MapFunction <Vector, Integer>() {
					@Override
					public Integer map(Vector value) throws Exception {
						return OutlierUtil.vectorSize(value);
					}
				})
				.reduce(new ReduceFunction <Integer>() {
					@Override
					public Integer reduce(Integer value1, Integer value2) throws Exception {
						return Math.max(value1, value2);
					}
				});
		} else {
			maxVectorSize = MLEnvironmentFactory
				.get(getMLEnvironmentId())
				.getExecutionEnvironment()
				.fromElements(0);

			params.set(WithMultiVarParams.FEATURE_COLS, OutlierUtil.fillFeatures(in.getColNames(), params));
		}

		IterativeDataSet <Tuple2 <Integer, byte[]>> loop = MLEnvironmentFactory
			.get(getMLEnvironmentId())
			.getExecutionEnvironment()
			.fromElements(Tuple2.of(-1, new byte[0]))
			.iterate(numTrees);

		DataSet <Tuple2 <Integer, byte[]>> loopResult = input
			.mapPartition(new RichMapPartitionFunction <Row, Tuple2 <Integer, Row>>() {

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, Row>> out) {

					int parallel = getIterationRuntimeContext().getNumberOfParallelSubtasks();
					int nextSuperStep = getIterationRuntimeContext().getSuperstepNumber();

					int start = (nextSuperStep - 1) * parallel, end = nextSuperStep * parallel;

					int numTreesCurLoop = Math.min(numTrees - start, end - start);

					Random rnd = ThreadLocalRandom.current();

					List <PriorityQueue <Tuple3 <Double, Integer, Row>>> priorityQueues
						= new ArrayList <>(numTreesCurLoop);

					for (int i = 0; i < numTreesCurLoop; ++i) {
						priorityQueues.add(new PriorityQueue <>(subsamplingSize, Comparator.comparing(o -> o.f0)));
					}

					for (Row row : values) {
						for (int i = 0; i < numTreesCurLoop; ++i) {
							PriorityQueue <Tuple3 <Double, Integer, Row>> q = priorityQueues.get(i);

							if (q.size() < subsamplingSize) {
								q.offer(Tuple3.of(rnd.nextDouble(), i, row));
							} else {
								Double rand = rnd.nextDouble();

								if (rand > q.element().f0) {
									q.poll();
									q.offer(Tuple3.of(rand, i, row));
								}
							}
						}
					}

					for (PriorityQueue <Tuple3 <Double, Integer, Row>> q : priorityQueues) {
						for (Tuple3 <Double, Integer, Row> item : q) {
							out.collect(Tuple2.of(item.f1, item.f2));
						}
					}
				}
			})
			.withBroadcastSet(loop, "loop")
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key;
				}
			}, 0)
			.mapPartition(new RichMapPartitionFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, byte[]>>() {

				private final List <List <Node>> model = new ArrayList <>();

				@Override
				public void mapPartition(
					Iterable <Tuple2 <Integer, Row>> values, Collector <Tuple2 <Integer, byte[]>> out) throws Exception {

					Random rnd = ThreadLocalRandom.current();

					PriorityQueue <Tuple2 <Double, Row>> q = new PriorityQueue <>(
						subsamplingSize, Comparator.comparing(o -> o.f0)
					);

					for (Tuple2 <Integer, Row> val : values) {
						if (q.size() < subsamplingSize) {
							q.offer(Tuple2.of(rnd.nextDouble(), val.f1));
						} else {
							Double rand = rnd.nextDouble();

							if (rand > q.element().f0) {
								q.poll();
								q.offer(Tuple2.of(rand, val.f1));
							}
						}
					}

					MTable series = new MTable(q.stream().map(v -> v.f1).collect(Collectors.toList()), colNames, colTypes);

					series = OutlierUtil.getMTable(series, params);

					NumericalTypeCastMapper numericalTypeCastMapper = new NumericalTypeCastMapper(
						series.getSchema(),
						new Params()
							.set(NumericalTypeCastParams.SELECTED_COLS, series.getColNames())
							.set(NumericalTypeCastParams.TARGET_TYPE, TargetType.DOUBLE)
					);

					int numRows = series.getNumRow();

					List<Row> rows = new ArrayList <>(numRows);

					for (int i = 0; i < numRows; ++i) {
						rows.add(numericalTypeCastMapper.map(series.getRow(i)));
					}

					series = new MTable(rows, series.getSchemaStr());

					if (numRows > 0) {
						int heightLimit = (int) Math.ceil(Math.log(Math.min(rows.size(), subsamplingSize)) / LOG2);

						IForestTrain iForestTrain = new IForestTrain(params);

						model.add(iForestTrain.iTree(series, heightLimit, rnd));
					}

					int parallel = getIterationRuntimeContext().getNumberOfParallelSubtasks();
					int nextSuperStep = getIterationRuntimeContext().getSuperstepNumber();

					if (!((nextSuperStep - 1) * parallel < numTrees && nextSuperStep * parallel >= numTrees)) {
						out.collect(Tuple2.of(-1, new byte[0]));
						return;
					}

					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						int subSamplingSize = Math.min(params.get(IForestTrainParams.SUBSAMPLING_SIZE), numRows);

						params.set(IForestTrainParams.SUBSAMPLING_SIZE, subSamplingSize);

						if (params.contains(WithMultiVarParams.VECTOR_COL)) {
							params.set(
								OutlierUtil.MAX_VECTOR_SIZE,
								getRuntimeContext().getBroadcastVariableWithInitializer(
									"maxVectorSize",
									new BroadcastVariableInitializer <Integer, Integer>() {
										@Override
										public Integer initializeBroadcastVariable(Iterable <Integer> data) {
											return data.iterator().next();
										}
									}
								)
							);
						}

						out.collect(Tuple2.of(0, SerializationUtils.serialize(params)));
					}

					for (List <Node> tree : model) {
						out.collect(
							Tuple2.of(1, SerializationUtils.serialize(tree.toArray(new Node[0]))));
					}
				}
			})
			.withBroadcastSet(maxVectorSize, "maxVectorSize");

		loopResult = loop.closeWith(
			loopResult.filter(new FilterFunction <Tuple2 <Integer, byte[]>>() {
				@Override
				public boolean filter(Tuple2 <Integer, byte[]> value) {
					return value.f0 >= 0;
				}
			}),
			loopResult.filter(new FilterFunction <Tuple2 <Integer, byte[]>>() {
				@Override
				public boolean filter(Tuple2 <Integer, byte[]> value) {
					return value.f0 < 0;
				}
			})
		);

		setOutput(
			loopResult
				.reduceGroup(new GroupReduceFunction <Tuple2 <Integer, byte[]>, Row>() {
					@Override
					public void reduce(Iterable <Tuple2 <Integer, byte[]>> values, Collector <Row> out) {

						IForestModel model = new IForestModel();

						for (Tuple2 <Integer, byte[]> val : values) {
							if (val.f0 == 0) {
								model.meta = SerializationUtils.deserialize(val.f1);
								continue;
							}

							model.trees.add(new ArrayList <>(
								Arrays.asList(SerializationUtils.deserialize(val.f1))
							));
						}

						new IForestModelDataConverter().save(model, out);
					}
				}),
			new IForestModelDataConverter().getModelSchema()
		);

		return this;
	}
}
