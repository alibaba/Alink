package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.huge.line.ApsIteratorLine;
import com.alibaba.alink.operator.batch.huge.line.ApsSerializeDataLine;
import com.alibaba.alink.operator.batch.huge.line.ApsSerializeModelLine;
import com.alibaba.alink.operator.common.aps.ApsContext;
import com.alibaba.alink.operator.common.aps.ApsEnv;
import com.alibaba.alink.operator.common.graph.GraphUtilsWithString;
import com.alibaba.alink.operator.common.nlp.WordCountUtil;
import com.alibaba.alink.params.graph.LineParams;
import com.alibaba.alink.params.nlp.HasBatchSize;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LineBatchOp extends BatchOperator <LineBatchOp>
	implements LineParams <LineBatchOp> {

	private static final long serialVersionUID = -5857950388102221227L;

	public LineBatchOp(Params params) {
		super(params);
	}

	public LineBatchOp() {
		super(new Params());
	}

	@Override
	public LineBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator<?> in = checkAndGetFirst(inputs);
		String sourceCol = getSourceCol();
		String targetCol = getTargetCol();
		int vectorDim = getVectorSize();
		int maxIter = getMaxIter();
		int order = getOrder().getValue();
		boolean addReverseEdge = getIsToUndigraph();
		int threadNum = getNumThreads();
		getParams().set("threadNum", threadNum);
		Params apsContext = new Params();
		apsContext.set("alinkApsNumMiniBatch", 1);
		ApsContext context = new ApsContext(getMLEnvironmentId()).put(apsContext);
		ApsEnv <Number[], float[][]> apsEnv = new ApsEnv <>(
			null,
			new ApsSerializeDataLine(),
			new ApsSerializeModelLine(),
			getMLEnvironmentId()
		);
		String weightCol = getWeightCol();
		boolean hasWeightCol = !(weightCol==null);
		String[] inputEdgeCols = hasWeightCol ? new String[] {sourceCol, targetCol, weightCol}
		: new String[] {sourceCol, targetCol};
		int vertexColTypeIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), sourceCol);
		TypeInformation <?> vertexType = in.getColTypes()[vertexColTypeIndex];
		DataSet<Row> inputData = GraphUtilsWithString.input2json(in, inputEdgeCols, 2, true);
		GraphUtilsWithString map = new GraphUtilsWithString(inputData, vertexType);

		DataSet <Tuple3 <Long, Long, Double>> edges = map
			.inputType2longTuple3(inputData, hasWeightCol);
		if (addReverseEdge) {
			edges = edges.flatMap(new FlatMapFunction <Tuple3 <Long, Long, Double>, Tuple3 <Long, Long, Double>>() {
				private static final long serialVersionUID = 2164543041121716573L;

				@Override
				public void flatMap(Tuple3 <Long, Long, Double> value, Collector <Tuple3 <Long, Long, Double>> out) {
					out.collect(value);
					long temp = value.f0;
					value.f0 = value.f1;
					value.f1 = temp;
					out.collect(value);
				}
			});
		}
		//id degree weight
		DataSet <Row> vertices = edges
			.flatMap(new FlatMapFunction <Tuple3 <Long, Long, Double>, Tuple1 <Long>>() {
				private static final long serialVersionUID = -3157356818548756725L;

				@Override
				public void flatMap(Tuple3 <Long, Long, Double> value, Collector <Tuple1 <Long>> out) {
					out.collect(Tuple1.of(value.f0));
					out.collect(Tuple1.of(value.f1));
				}
			}).groupBy(0).reduceGroup(new GroupReduceFunction <Tuple1 <Long>, Row>() {
				private static final long serialVersionUID = -5732373345671140682L;

				@Override
				public void reduce(Iterable <Tuple1 <Long>> values, Collector <Row> out) throws Exception {
					AtomicInteger count = new AtomicInteger();
					AtomicLong vertex = new AtomicLong();
					values.forEach(x -> {
						vertex.set(x.f0);
						count.addAndGet(1);
					});
					Row res = new Row(3);
					res.setField(0, vertex.longValue());
					res.setField(1, count.intValue());
					res.setField(2, Math.pow(count.intValue(), 0.75));
					out.collect(res);
				}
			}).returns(new RowTypeInfo(Types.LONG, Types.INT, Types.DOUBLE));

		Tuple3 <DataSet <Row>, DataSet <Long[]>, DataSet <long[]>> sortedIndexVocab
			= WordCountUtil.sortedIndexVocab(vertices, 1, false);
		DataSet <Row> vocab = sortedIndexVocab.f0;
		context.put("negBound", sortedIndexVocab.f1);

		if (!getParams().contains(HasBatchSize.BATCH_SIZE)) {
			context.map(new MapFunction <Params, Params>() {
				private static final long serialVersionUID = -746561127812958030L;

				@Override
				public Params map(Params value) throws Exception {
					value.set(ApsContext.ALINK_APS_NUM_MINI_BATCH, 1);
					return value;
				}
			});
		} else {
			DataSet <Long> edgeCount = edges
				.mapPartition(new MapPartitionFunction <Tuple3 <Long, Long, Double>, Long>() {
					private static final long serialVersionUID = -6208289751895867389L;

					@Override
					public void mapPartition(Iterable <Tuple3 <Long, Long, Double>> values,
											 Collector <Long> out) throws Exception {
						long res = 0;
						for (Tuple3 <Long, Long, Double> value : values) {
							res += 1;
						}
						out.collect(res);
					}
				}).reduce(new ReduceFunction <Long>() {
					private static final long serialVersionUID = 2077893051956735732L;

					@Override
					public Long reduce(Long value1, Long value2) throws Exception {
						return value1 + value2;
					}
				});
			DataSet <Integer> miniBatchSize = edgeCount.map(new MapMiniBatchNum(get(HasBatchSize.BATCH_SIZE)));
			context.put(ApsContext.alinkApsNumMiniBatch, miniBatchSize);
		}

		DataSet <Tuple2 <Long, float[][]>> model = vocab.mapPartition(new IniModel(vectorDim, order));
		DataSet <Number[]> trainData = encode(edges, vocab);

		Tuple2 <DataSet <Tuple2 <Long, float[][]>>, BatchOperator[]> apsRes = apsEnv.iterate(
			model, trainData, context, null,
			true, maxIter, 1, getParams(),
			new ApsIteratorLine(), new ApsEnv.PersistentHook <Tuple2 <Long, float[][]>>() {
				@Override
				public DataSet <Tuple2 <Long, float[][]>> hook(DataSet <Tuple2 <Long, float[][]>> input) {
					//this is used to balance the model.
					return input.groupBy(0).withPartitioner(new RandomPartitioner())
						.reduceGroup(new GroupReduceFunction <Tuple2 <Long, float[][]>, Tuple2 <Long, float[][]>>() {
							private static final long serialVersionUID = 4586697690697331816L;

							@Override
							public void reduce(Iterable <Tuple2 <Long, float[][]>> values,
											   Collector <Tuple2 <Long, float[][]>> out) throws Exception {
								out.collect(values.iterator().next());
							}
						});
				}
			}
		);
		model = apsRes.f0;
		DataSet <Tuple2 <Long, double[]>> modelRes = model
			.join(vocab).where(0).equalTo(new RowKeySelector(2))
			.with(new JoinFunction <Tuple2 <Long, float[][]>, Row, Tuple2 <Long, double[]>>() {
				private static final long serialVersionUID = -2935973948347718658L;

				@Override
				public Tuple2 <Long, double[]> join(Tuple2 <Long, float[][]> first, Row second) throws Exception {
					int length = first.f1[0].length;
					double[] doubleData = new double[length];
					for (int i = 0; i < length; i++) {
						//just add value, not context.
						doubleData[i] = first.f1[0][i];
					}
					return Tuple2.of((long) second.getField(0), doubleData);
				}
			});
		DataSet <Row> res = map.mapLine(modelRes);

		this.setOutput(res, new String[] {"vertexId", "vertexVector"},
			new TypeInformation <?>[] {vertexType, TypeInformation.of(DenseVector.class)});
		return this;
	}

	private static DataSet <Number[]> encode(DataSet <Tuple3 <Long, Long, Double>> edges, DataSet <Row> vocab) {
		return edges.coGroup(vocab)
			.where(0).equalTo(new RowKeySelector(0))
			.with(new CoGroupFunction <Tuple3 <Long, Long, Double>, Row, Tuple3 <Long, Long, Double>>() {
				private static final long serialVersionUID = -6776475425155785444L;

				@Override
				public void coGroup(Iterable <Tuple3 <Long, Long, Double>> first,
									Iterable <Row> second,
									Collector <Tuple3 <Long, Long, Double>> out) throws Exception {
					Row row = second.iterator().next();
					for (Tuple3 <Long, Long, Double> value : first) {
						out.collect(Tuple3.of((long) row.getField(2), value.f1, value.f2));
					}
				}
			}).coGroup(vocab).where(1).equalTo(new RowKeySelector(0))
			.with(new CoGroupFunction <Tuple3 <Long, Long, Double>, Row, Tuple3 <Long, Long, Double>>() {
				private static final long serialVersionUID = 1331079849234438512L;

				@Override
				public void coGroup(Iterable <Tuple3 <Long, Long, Double>> first,
									Iterable <Row> second,
									Collector <Tuple3 <Long, Long, Double>> out) throws Exception {
					Row row = second.iterator().next();
					for (Tuple3 <Long, Long, Double> value : first) {
						out.collect(Tuple3.of(value.f0, (long) row.getField(2), value.f2));
					}
				}
			}).map(new MapFunction <Tuple3 <Long, Long, Double>, Number[]>() {
				private static final long serialVersionUID = -223306152758743411L;

				@Override
				public Number[] map(Tuple3 <Long, Long, Double> value) throws Exception {
					Number[] res = new Number[3];
					res[0] = value.f0;
					res[1] = value.f1;
					res[2] = value.f2.floatValue();
					return res;
				}
			});
	}

	private static class MapMiniBatchNum implements MapFunction <Long, Integer> {
		private static final long serialVersionUID = -5385263705043389249L;
		int batchSize;

		MapMiniBatchNum(int batchSize) {
			this.batchSize = batchSize;
		}

		@Override
		public Integer map(Long value) throws Exception {
			return Double.valueOf(Math.ceil(1.0 * value / batchSize)).intValue();
		}
	}

	private static class IniModel extends RichMapPartitionFunction <Row, Tuple2 <Long, float[][]>> {
		private static final long serialVersionUID = 4601462262211542871L;
		private final int vectorDim;
		private final int order;

		IniModel(int vectorDim, int order) {
			this.vectorDim = vectorDim;
			this.order = order;
		}

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Long, float[][]>> out) throws Exception {
			Random rand = new Random();
			for (Row row : values) {
				Long idx = (Long) row.getField(2);
				float[][] ds = new float[order][vectorDim];
				for (int i = 0; i < this.vectorDim; i++) {
					ds[0][i] = (rand.nextFloat() - 0.5f) / vectorDim;
				}
				out.collect(new Tuple2 <>(idx, ds));
			}
		}
	}

	public static class RowKeySelector implements KeySelector <Row, Long> {
		private static final long serialVersionUID = 7514280642434354647L;
		int index;

		public RowKeySelector(int index) {
			this.index = index;
		}

		@Override
		public Long getKey(Row value) {
			return (Long) value.getField(index);
		}
	}

	private static class RandomPartitioner implements Partitioner <Long> {
		private static final long serialVersionUID = -2350703157277923339L;

		@Override
		public int partition(Long key, int numPartitions) {
			return (int) (key % numPartitions);
		}
	}

}
